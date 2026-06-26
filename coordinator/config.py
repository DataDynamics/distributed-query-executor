"""Coordinator 설정(환경 변수로 재정의 가능)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _csv_env(name: str) -> list[str]:
    raw = os.getenv(name, "").strip()
    return [u.strip() for u in raw.split(",") if u.strip()]


@dataclass
class Settings:
    # executor 서비스 URL 목록. 예: ["http://exec-1:8001", "http://exec-2:8001"]
    executors: list[str] = field(default_factory=lambda: _csv_env("EXECUTORS"))
    # Level-1 동시성: 동시에 처리하는 Job 수 상한
    max_concurrent_jobs: int = int(os.getenv("MAX_CONCURRENT_JOBS", "16"))
    # Level-2 동시성: 동시에 디스패치하는 sub-query 수 상한(전역)
    max_dispatch_concurrency: int = int(os.getenv("MAX_DISPATCH_CONCURRENCY", "32"))
    # executor task 상태 polling 간격(초)
    poll_interval_s: float = float(os.getenv("POLL_INTERVAL_S", "1.0"))
    # task 단위 타임아웃(초)
    task_timeout_s: float = float(os.getenv("TASK_TIMEOUT_S", "3600"))


settings = Settings()
