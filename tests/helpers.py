"""테스트 공용 헬퍼(pytest 수집 대상 아님 — test_ 접두사 없음).

``MockLocalStageBackend``: GP·Impala 없이 local_stage 파이프라인을 "파일 루프 닫힘"까지
통합 검증하기 위한 백엔드 목. export 는 실제 CSV 파일을 쓰고, load 는 외부테이블 DDL 의
``file://`` 경로를 파싱해 그 파일들을 읽어 인메모리 target 에 집계한다(docs/SCENARIO.md B 참고).
"""

from __future__ import annotations

import csv
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
