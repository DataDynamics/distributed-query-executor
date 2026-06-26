"""인메모리 Job 저장소. 운영 환경에서는 Redis/Postgres로 교체."""

from __future__ import annotations

from typing import Optional

from .models import Job


class JobStore:
    def __init__(self) -> None:
        self._jobs: dict[str, Job] = {}

    def add(self, job: Job) -> None:
        self._jobs[job.job_id] = job

    def get(self, job_id: str) -> Optional[Job]:
        return self._jobs.get(job_id)

    def list(self) -> list[Job]:
        return list(self._jobs.values())
