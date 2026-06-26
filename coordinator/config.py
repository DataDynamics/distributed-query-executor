"""Coordinator configuration (env-overridable)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _csv_env(name: str) -> list[str]:
    raw = os.getenv(name, "").strip()
    return [u.strip() for u in raw.split(",") if u.strip()]


@dataclass
class Settings:
    # URLs of executor services, e.g. ["http://exec-1:8001", "http://exec-2:8001"]
    executors: list[str] = field(default_factory=lambda: _csv_env("EXECUTORS"))
    # Level-1 concurrency: max jobs processed at once.
    max_concurrent_jobs: int = int(os.getenv("MAX_CONCURRENT_JOBS", "16"))
    # Level-2 concurrency: max sub-queries dispatched at once (global).
    max_dispatch_concurrency: int = int(os.getenv("MAX_DISPATCH_CONCURRENCY", "32"))
    # Polling interval (seconds) for executor task status.
    poll_interval_s: float = float(os.getenv("POLL_INTERVAL_S", "1.0"))
    # Per-task timeout (seconds).
    task_timeout_s: float = float(os.getenv("TASK_TIMEOUT_S", "3600"))


settings = Settings()
