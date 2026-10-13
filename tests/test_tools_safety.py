"""운영자 CLI의 데이터 보존과 오류 집계를 검증한다."""

import argparse
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import gp_query, s3_ops
from tools.load.analysis import per_vu_levels
from tools.load.stats import Collector, SUBMIT_ERRORS


class ObjectStore:
    def __init__(self, objects):
        self.objects = dict(objects)
        self.effects = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, Bucket, Prefix):
        return [{"Contents": [{"Key": key, "Size": len(value)}
                              for (bucket, key), value in self.objects.items()
                              if bucket == Bucket and key.startswith(Prefix)]}]

    def copy_object(self, Bucket, Key, CopySource):
        self.effects.append(("copy", Bucket, Key))
        self.objects[Bucket, Key] = self.objects[CopySource["Bucket"], CopySource["Key"]]

    def delete_objects(self, Bucket, Delete):
        for item in Delete["Objects"]:
            self.effects.append(("delete", Bucket, item["Key"]))
            del self.objects[Bucket, item["Key"]]
        return {}

    def download_file(self, bucket, key, local):
        self.effects.append(("download", bucket, key))
        Path(local).write_bytes(self.objects[bucket, key])


def transfer_args(source="s3://b/p/", destination="s3://b/q/", **kwargs):
    return argparse.Namespace(source=source, uri=destination, recursive=True,
                              dry_run=False, no_progress=True, **kwargs)


@pytest.mark.parametrize("command", [s3_ops.cmd_cp, s3_ops.cmd_mv])
@pytest.mark.parametrize("source,destination", [
    ("p/", "p/nested/"), ("p/nested/", "p/"), ("p/", "p/"),
    ("", "p/"), ("p/", ""), ("", ""), ("p", "p/nested"),
])
def test_recursive_overlap_rejected_before_mutation(command, source, destination):
    objects = {("b", "p/a"): b"A", ("b", "p/nested/a"): b"B"}
    store = ObjectStore(objects)
    with pytest.raises(ValueError, match="겹"):
        command(store, transfer_args("s3://b/" + source, "s3://b/" + destination))
    assert store.objects == objects
    assert store.effects == []


@pytest.mark.parametrize("bucket,prefix", [("b", "p2/"), ("other", "p/")])
def test_recursive_move_preserves_contents(bucket, prefix):
    store = ObjectStore({("b", "p/a"): b"A", ("b", "p/nested/a"): b"B"})
    assert s3_ops.cmd_mv(store, transfer_args(destination=f"s3://{bucket}/{prefix}")) == 0
    assert store.objects == {(bucket, prefix + "a"): b"A", (bucket, prefix + "nested/a"): b"B"}


def test_recursive_copy_keeps_source_and_contents():
    store = ObjectStore({("b", "p/a"): b"A", ("b", "p/nested/a"): b"B"})
    assert s3_ops.cmd_cp(store, transfer_args()) == 0
    assert store.objects == {("b", "p/a"): b"A", ("b", "p/nested/a"): b"B",
                             ("b", "q/a"): b"A", ("b", "q/nested/a"): b"B"}


@pytest.mark.parametrize("command", [s3_ops.cmd_cp, s3_ops.cmd_mv])
def test_trailing_slash_overlap_is_rejected_without_recursive_flag(command):
    store = ObjectStore({("b", "p/a"): b"A"})
    args = transfer_args(destination="s3://b/p/nested/")
    args.recursive = False
    with pytest.raises(ValueError, match="겹"):
        command(store, args)
    assert store.effects == []


@pytest.mark.parametrize("command", [s3_ops.cmd_cp, s3_ops.cmd_mv])
def test_transfer_dry_run_does_not_mutate(command):
    store = ObjectStore({("b", "p/a"): b"A"})
    args = transfer_args()
    args.dry_run = True
    assert command(store, args) == 0
    assert store.objects == {("b", "p/a"): b"A"}
    assert store.effects == []


def test_move_deletes_only_copied_keys(monkeypatch):
    store = ObjectStore({("b", "keep"): b"K", ("b", "move"): b"M"})
    monkeypatch.setattr(s3_ops, "iter_copy_pairs", lambda *a: [("keep", "keep"), ("move", "q/move")])
    assert s3_ops.cmd_mv(store, transfer_args()) == 0
    assert store.objects == {("b", "keep"): b"K", ("b", "q/move"): b"M"}


@pytest.mark.parametrize("unsafe", ["../escape", "nested/../../escape", "/absolute"])
def test_download_validates_entire_plan_before_writes(tmp_path, unsafe):
    if unsafe == "/absolute":
        unsafe = str(tmp_path / "absolute")
    store = ObjectStore({("b", "p/a"): b"A", ("b", "p/" + unsafe): b"bad"})
    destination = tmp_path / "download"
    args = transfer_args(destination="s3://b/p/")
    args.destination, args.force = str(destination), True
    with pytest.raises(ValueError, match="경로"):
        s3_ops.cmd_download(store, args)
    assert store.effects == []
    assert not destination.exists()


@pytest.mark.parametrize("link_file", [False, True])
def test_recursive_download_rejects_symlink_escape(tmp_path, link_file):
    destination = tmp_path / "download"
    destination.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    if link_file:
        (outside / "secret").write_bytes(b"original")
        (destination / "z").symlink_to(outside / "secret")
        key = "p/z"
    else:
        (destination / "z").symlink_to(outside, target_is_directory=True)
        key = "p/z/escape"
    store = ObjectStore({("b", "p/a"): b"A", ("b", key): b"bad"})
    args = transfer_args(destination="s3://b/p/")
    args.destination, args.force = str(destination), True
    with pytest.raises(ValueError, match="경로"):
        s3_ops.cmd_download(store, args)
    assert store.effects == []
    assert not (destination / "a").exists()
    if link_file:
        assert (outside / "secret").read_bytes() == b"original"


def test_recursive_download_keeps_nested_files(tmp_path):
    store = ObjectStore({("b", "p/a"): b"A", ("b", "p/nested/b"): b"B"})
    args = transfer_args(destination="s3://b/p/")
    args.destination, args.force = str(tmp_path / "download"), False
    assert s3_ops.cmd_download(store, args) == 0
    assert (tmp_path / "download/a").read_bytes() == b"A"
    assert (tmp_path / "download/nested/b").read_bytes() == b"B"


class Connection:
    description = None
    rowcount = 1
    autocommit = False

    def __init__(self):
        self.statements = []
        self.rollbacks = self.commits = 0

    def cursor(self):
        return self

    def execute(self, sql):
        self.statements.append(sql)

    def fetchone(self):
        return None

    def close(self):
        pass

    def rollback(self):
        self.rollbacks += 1

    def commit(self):
        self.commits += 1


GP_ARGS = ["--no-config", "--host", "host", "-d", "db", "-u", "user", "--no-password-prompt"]


def test_gp_interactive_dry_run_rejected_before_connect(monkeypatch, capsys):
    connections = []
    def connect(**kwargs):
        connections.append(Connection())
        return connections[-1]
    monkeypatch.setattr(gp_query, "import_psycopg", lambda: SimpleNamespace(connect=connect))
    monkeypatch.setattr("builtins.input", lambda *a: "\\q")
    with pytest.raises(SystemExit) as exc:
        gp_query.main(GP_ARGS + ["--interactive", "--dry-run"])
    assert exc.value.code == 2
    assert connections == []
    assert "--dry-run" in capsys.readouterr().err


@pytest.mark.parametrize("schema", [None, "reporting"])
def test_gp_session_sql_applied_on_every_connection(schema):
    args = gp_query.build_parser().parse_args(GP_ARGS + ["--interactive", "--session-sql", "SET statement_timeout=1000"])
    args.schema = schema
    engine = gp_query.make_engine(args, SimpleNamespace(connect=lambda **kw: Connection()), None)
    for _ in range(2):
        connection = engine.connect()
        assert connection.autocommit
        assert connection.statements[-1] == "SET statement_timeout=1000"
        if schema:
            assert connection.statements[-2] == "SET search_path TO reporting"


def test_gp_batch_dry_run_rolls_back(monkeypatch):
    connection = Connection()
    monkeypatch.setattr(gp_query, "import_psycopg", lambda: SimpleNamespace(connect=lambda **kw: connection))
    assert gp_query.main(GP_ARGS + ["--dry-run", "-q", "DELETE FROM sample", "--session-sql", "SET statement_timeout=1000"]) == 0
    assert connection.statements == ["SET statement_timeout=1000", "DELETE FROM sample"]
    assert connection.rollbacks == 1
    assert connection.commits == 0


@pytest.mark.parametrize("outcome", SUBMIT_ERRORS)
def test_vu_error_rate_counts_request_errors(outcome):
    collector = Collector()
    collector.record_gauges(0, 2, 0)
    collector.record_submit(0, outcome, 0.1)
    assert per_vu_levels(collector.timeline(1), warmup_frac=0)[0]["error_rate"] == 1.0


def test_vu_error_rate_matches_summary_for_mixed_outcomes():
    collector = Collector()
    collector.record_gauges(0, 2, 0)
    for outcome in ["ok", "server_error", "rejected_429", "accepted", "accepted"]:
        collector.record_submit(0, outcome, 0.1)
    collector.record_job(0, "FAILED", False, 0.1)
    collector.record_job(0, "ABANDONED", False, None)
    level = per_vu_levels(collector.timeline(1), warmup_frac=0)[0]
    assert level["error_rate"] == collector.summary(1, 1)["error_rate"] == 0.5
    assert level["reject"] == 1
