"""런타임 검토에서 발견한 중복 실행·경로 이탈·분할·큐 종료 회귀를 검사한다."""
import asyncio
import queue
import threading
from contextlib import contextmanager

import pytest

from coordinator.parser import QueryValidationError, validate_and_parse
from coordinator.splitter import split
from executor.app import create_app
from executor.backend import ImpalaToGreenplumBackend
from executor.models import CreateTaskRequest
from core.config import settings
from fastapi import HTTPException


def endpoint(app, path):
    return next(route.endpoint for route in app.routes if route.path == path)


@pytest.mark.parametrize('strict', [True, False])
@pytest.mark.parametrize('condition', ["dt IN ('a','b') OR enabled=1", "NOT (dt IN ('a','b'))", "(dt IN ('a','b')) = false"])
def test_partition_predicate_must_be_positive_and_mandatory(strict, condition):
    with pytest.raises(QueryValidationError) as exc:
        validate_and_parse(f'SELECT * FROM t WHERE {condition}', 'dt', strict=strict)
    assert exc.value.code in ('UNSAFE_PARTITION_PREDICATE', 'NEGATED_IN')


def test_strict_partition_filter_must_be_in_root_where():
    with pytest.raises(QueryValidationError) as exc:
        validate_and_parse("SELECT * FROM (SELECT * FROM t WHERE dt IN ('a','b')) s WHERE enabled=1", 'dt')
    assert exc.value.code == 'UNSAFE_PARTITION_PREDICATE'


def test_duplicate_values_do_not_create_overlapping_tasks():
    parsed = validate_and_parse("SELECT * FROM t WHERE dt IN ('a','a','b')", 'dt')
    assert parsed.partition_values == ["'a'", "'b'"]
    assert [s.partition_values for s in split(parsed, 3)] == [["'a'"], ["'b'"]]


@pytest.mark.parametrize('prefix', ["SELECT 'dt IN (bogus)' AS note, * FROM t", "SELECT * FROM t /* dt IN ('fake') */", "SELECT * FROM t -- dt IN ('fake')\n"])
def test_split_ignores_in_inside_literals_and_comments(prefix):
    sql = prefix + " WHERE dt IN ('a)','b(') AND enabled=1"
    subs = split(validate_and_parse(sql, 'dt'), 2)
    assert subs[0].sql == prefix + " WHERE dt IN ('a)') AND enabled=1"
    assert subs[1].sql == prefix + " WHERE dt IN ('b(') AND enabled=1"


def test_split_targets_selected_ast_node_when_text_order_differs():
    sql = "SELECT * FROM (SELECT * FROM t WHERE t.dt IN ('inner1','inner2')) s WHERE s.dt IN ('outer1','outer2')"
    subs = split(validate_and_parse(sql, 'dt', strict=False), 2)
    assert subs[0].sql == "SELECT * FROM (SELECT * FROM t WHERE t.dt IN ('inner1','inner2')) s WHERE s.dt IN ('outer1')"


@pytest.mark.parametrize('job_id', ['..', '.', '../victim', 'a/b', 'a\\b', ''])
@pytest.mark.parametrize('path', ['/stage/{job_id}/cleanup', '/s3/{job_id}/cleanup'])
def test_cleanup_rejects_unsafe_ids(tmp_path, monkeypatch, job_id, path):
    stage = tmp_path / 'stage'
    stage.mkdir()
    marker = tmp_path / 'keep'
    marker.write_text('keep')
    monkeypatch.setattr(settings, 'stage_local_dir', str(stage))
    with pytest.raises(HTTPException) as exc:
        endpoint(create_app(), path)(job_id)
    assert exc.value.status_code == 422
    assert marker.read_text() == 'keep'


def test_cleanup_valid_job_and_symlink_containment(tmp_path, monkeypatch):
    stage = tmp_path / 'stage'
    stage.mkdir()
    job = stage / 'job-1'
    job.mkdir()
    (job / 'part.csv').write_text('data')
    other = tmp_path / 'other'
    other.mkdir()
    (other / 'keep').write_text('keep')
    (stage / 'escape').symlink_to(other, target_is_directory=True)
    monkeypatch.setattr(settings, 'stage_local_dir', str(stage))
    cleanup = endpoint(create_app(), '/stage/{job_id}/cleanup')
    assert cleanup('job-1')['removed'] is True
    assert cleanup('job-1')['removed'] is False
    with pytest.raises(HTTPException) as exc:
        cleanup('escape')
    assert exc.value.status_code == 422
    assert (other / 'keep').read_text() == 'keep'


class RecordingHistory:
    def __init__(self):
        self.records = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def record(self, task):
        self.records.append(task.status.value)
        if task.status.value == 'QUEUED':
            self.entered.set()
            await self.release.wait()


async def test_task_claim_precedes_history_await_and_replays_same_body():
    history = RecordingHistory()
    class Backend:
        calls = 0
        def move(self, *args, **kwargs):
            self.calls += 1
            return 1
    backend = Backend()
    app = create_app(backend=backend, task_history=history)
    submit = endpoint(app, '/tasks')
    payload = dict(task_id='t1', job_id='j1', sub_query='SELECT 1', target_table='t', write_mode='append', partition_column='dt', partition_values=["'a'"])
    first = asyncio.create_task(submit(CreateTaskRequest(**payload)))
    await history.entered.wait()
    try:
        replay = await asyncio.wait_for(submit(CreateTaskRequest(**payload)), .2)
        assert replay['task_id'] == 't1'
        with pytest.raises(HTTPException) as exc:
            await submit(CreateTaskRequest(**dict(payload, sub_query='SELECT 2')))
        assert exc.value.status_code == 409
    finally:
        history.release.set()
        await first
    for _ in range(100):
        if endpoint(app, '/tasks/{task_id}')('t1')['status'] == 'DONE':
            break
        await asyncio.sleep(.01)
    assert backend.calls == 1
    await submit(CreateTaskRequest(**payload))
    assert backend.calls == 1
    assert history.records.count('QUEUED') == 1


@pytest.mark.parametrize('reader_error', [False, True])
def test_pipeline_delivers_eof_when_queue_is_full(monkeypatch, reader_error):
    eof_attempted = threading.Event()
    original = queue.Queue
    class BoundedQueue(original):
        def put(self, value, *args, **kwargs):
            if value is None:
                eof_attempted.set()
            return super().put(value, *args, **kwargs)
        def get(self, block=True, timeout=None):
            return super().get(block=block, timeout=1 if block else None)
    monkeypatch.setattr(queue, 'Queue', BoundedQueue)
    class Source:
        first = True
        def fetchmany(self, size):
            if self.first:
                self.first = False
                return [(1,)]
            if reader_error:
                raise RuntimeError('source failed')
            return []
    rows = []
    copy_errors = []
    class GP:
        @contextmanager
        def copy(self, sql):
            assert eof_attempted.wait(1)
            try:
                yield self
            except BaseException as exc:
                copy_errors.append(str(exc))
                raise
        def write_row(self, row):
            rows.append(row)
    backend = ImpalaToGreenplumBackend(impala_dsn={}, greenplum_dsn='x', queue_size=1)
    if reader_error:
        with pytest.raises(RuntimeError, match='source failed'):
            backend._stream_pipelined(Source(), GP(), 'COPY t FROM STDIN', None)
        assert copy_errors == ['source failed']
    else:
        assert backend._stream_pipelined(Source(), GP(), 'COPY t FROM STDIN', None)[0] == 1
    assert rows == [(1,)]


def test_numeric_equivalent_literals_are_split_only_once():
    parsed = validate_and_parse('SELECT * FROM t WHERE dt IN (1, 1.0, 01, -2, -2.0, 3)', 'dt')
    assert parsed.partition_values == ['1', '-2', '3']
    assert [s.partition_values for s in split(parsed, 6)] == [['1'], ['-2'], ['3']]


@pytest.mark.parametrize('predicate', ["EXISTS (SELECT 1 FROM x WHERE dt IN (1,2)) OR enabled=1", "NOT EXISTS (SELECT 1 FROM x WHERE dt IN (1,2))"])
def test_lenient_partition_cannot_be_bypassed_by_enclosing_predicate(predicate):
    with pytest.raises(QueryValidationError) as exc:
        validate_and_parse('SELECT * FROM t WHERE ' + predicate, 'dt', strict=False)
    assert exc.value.code == 'UNSAFE_PARTITION_PREDICATE'


def test_s3_cleanup_uses_exact_valid_job_prefix(monkeypatch):
    prefixes = []
    class Backend:
        def cleanup_s3_prefix(self, prefix):
            prefixes.append(prefix)
            return 2
    monkeypatch.setattr(settings, 's3_prefix', 'staging/root')
    cleanup = endpoint(create_app(backend=Backend()), '/s3/{job_id}/cleanup')
    assert cleanup('job-1') == {'job_id': 'job-1', 'deleted': 2}
    assert prefixes == ['staging/root/job-1/']


def test_split_ast_fallback_preserves_selected_partition_semantics():
    parsed = validate_and_parse("SELECT * FROM t WHERE dt IN ('a','b') AND enabled=1", 'dt')
    for node in parsed.expression.walk():
        node.meta.clear()
    subs = split(parsed, 2)
    assert [validate_and_parse(s.sql, 'dt').partition_values for s in subs] == [["'a'"], ["'b'"]]
    assert all('enabled = 1' in s.sql for s in subs)
