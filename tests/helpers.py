"""테스트 공용 헬퍼(pytest 수집 대상 아님 — test_ 접두사 없음).

``MockLocalStageBackend``: GP·Impala 없이 local_stage 파이프라인을 "파일 루프 닫힘"까지
통합 검증하기 위한 백엔드 목. export 는 실제 CSV 파일을 쓰고, load 는 외부테이블 DDL 의
``file://`` 경로를 파싱해 그 파일들을 읽어 인메모리 target 에 집계한다(docs/GUIDE.md(local_stage 절) 참고).

``MockS3StageBackend``: GP·S3 없이 s3_stage 2-phase 를 "S3 루프 닫힘"까지 통합 검증한다.
Phase 1(export_to_s3)은 **인메모리 S3**(dict)에 CSV 를 업로드하고, Phase 2(load_external_s3)는
coordinator 가 조립한 PXF external_ddl 의 프리픽스를 파싱해 그 아래 객체들을 읽어 인메모리
target 에 집계한다. Phase 3(cleanup_s3_prefix)은 그 프리픽스 객체를 지운다.
"""

from __future__ import annotations

import csv
import io
import os
import re

from executor.backend import MockBackend


class MockLocalStageBackend(MockBackend):
    """export=실 CSV 파일 write, load=file:// 경로 파싱→read→target 집계, 토폴로지 제공."""

    def __init__(self, topology=None):
        super().__init__()
        self.topology = dict(topology or {})   # {host: S_h}
        self.target: list = []                 # 인메모리 GP target
        self.exported: list = []               # (out_path, rows)
        self.loads: list = []                  # external_ddl 기록
        self.cleaned: list = []                # (미사용) 확장 여지

    def export_to_local_csv(self, sub_query, out_path, csv_options=None,
                            on_progress=None, query_options=None, on_stage=None):
        opts = csv_options or {}
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        vals = re.findall(r"'([^']*)'", sub_query)  # sub_query 의 IN 값 → 값당 1행
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f, delimiter=opts.get("delimiter", "`"),
                           quotechar=opts.get("quote", '"'), lineterminator="\n")
            for i, v in enumerate(vals):
                w.writerow([i, 1.0, v])
        self.exported.append((out_path, len(vals)))
        if on_progress:
            on_progress(len(vals))
        return len(vals)

    def load_external_csv(self, external_ddl, staging_ddl, staging_load_sql,
                          pre_delete_sql, insert_sql, cleanup_sqls=None, on_stage=None):
        self.loads.append(external_ddl)
        paths = re.findall(r"file://[^/]*(/[^']+)", external_ddl)  # host 뒤 경로만
        loaded = 0
        for p in paths:
            with open(p, newline="", encoding="utf-8") as f:
                rows = list(csv.reader(f, delimiter="`"))
            self.target.extend(rows)
            loaded += len(rows)
        return loaded

    def segment_host_counts(self):
        return self.topology


class MockS3StageBackend(MockBackend):
    """s3_stage 2-phase 를 GP·S3 없이 통합 검증. Phase 1=인메모리 S3 업로드, Phase 2=프리픽스
    파싱→read→target 집계, Phase 3=프리픽스 삭제. LocalDispatcher 는 export/load/cleanup 이 같은
    이 인스턴스를 쓰므로(주입 백엔드 공유) 인메모리 S3 로 루프가 닫힌다.

    ``fail_export=True`` 면 Phase 1 이 항상 실패해, 배리어 후 Phase 2 가 건너뛰어지는 경로를
    검증할 수 있다."""

    def __init__(self, fail_export: bool = False):
        super().__init__()
        self.s3: dict = {}            # 인메모리 S3: object key -> CSV 텍스트
        self.target: list = []        # 인메모리 GP target(적재 결과)
        self.uploaded_keys: list = []
        self.loads: list = []         # (external_ddl, pre_delete_sql, insert_sql) 기록
        self.cleaned: list = []       # 정리한 프리픽스
        self.fail_export = fail_export

    def export_to_s3(self, sub_query, key, job_id, task_id, csv_options=None,
                     on_progress=None, query_options=None, on_stage=None):
        # Phase 1: sub_query 의 IN 값(값당 1행)을 CSV 로 만들어 인메모리 S3(key)에 업로드.
        if self.fail_export:
            raise RuntimeError("s3 upload boom")
        opts = csv_options or {}
        vals = re.findall(r"'([^']*)'", sub_query)
        buf = io.StringIO()
        w = csv.writer(buf, delimiter=opts.get("delimiter", "`"),
                       quotechar=opts.get("quote", '"'), lineterminator="\n")
        for i, v in enumerate(vals):
            w.writerow([i, 1.0, v])
        self.s3[key] = buf.getvalue()
        self.uploaded_keys.append(key)
        if on_progress:
            on_progress(len(vals))
        return len(vals)

    def load_external_s3(self, external_ddl, pre_delete_sql, insert_sql, cleanup_sqls=None,
                         on_stage=None):
        # Phase 2: external_ddl 의 pxf 프리픽스(pxf://bucket/<prefix>/) 아래 S3 객체를 읽어 집계.
        self.loads.append((external_ddl, pre_delete_sql, insert_sql))
        m = re.search(r"pxf://[^/]+/([^?']+)", external_ddl)
        prefix = m.group(1) if m else ""
        dm = re.search(r"DELIMITER '([^']*)'", external_ddl)
        delim = dm.group(1) if dm else "`"
        loaded = 0
        for k, text in self.s3.items():
            if k.startswith(prefix):
                rows = list(csv.reader(io.StringIO(text), delimiter=delim))
                self.target.extend(rows)
                loaded += len(rows)
        return loaded

    def cleanup_s3_prefix(self, prefix):
        # Phase 3: 인메모리 S3 에서 프리픽스 아래 객체를 삭제한다.
        gone = [k for k in self.s3 if k.startswith(prefix)]
        for k in gone:
            del self.s3[k]
        self.cleaned.append(prefix)
        return len(gone)
