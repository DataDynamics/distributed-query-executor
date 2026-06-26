"""Executor FastAPI application: accepts tasks, runs them, exposes status."""

from __future__ import annotations

import asyncio
from typing import Optional

from fastapi import FastAPI, HTTPException

from .backend import Backend, MockBackend
from .models import CreateTaskRequest, Task, TaskStatus


def create_app(backend: Optional[Backend] = None) -> FastAPI:
    backend = backend or MockBackend()
    tasks: dict[str, Task] = {}

    app = FastAPI(title="Query Executor", version="0.1.0")
    app.state.backend = backend
    app.state.tasks = tasks

    async def _run(task: Task) -> None:
        def progress(n: int) -> None:
            task.rows_written = n

        try:
            task.status = TaskStatus.READING
            loop = asyncio.get_running_loop()
            # impyla/psycopg are blocking -> run in a thread to keep the loop free.
            task.status = TaskStatus.WRITING
            rows = await loop.run_in_executor(
                None,
                lambda: app.state.backend.move(
                    task.sub_query,
                    task.target_table,
                    task.write_mode,
                    task.partition_column,
                    task.partition_values,
                    progress,
                ),
            )
            task.rows_written = rows
            task.status = TaskStatus.DONE
        except Exception as exc:
            task.status = TaskStatus.FAILED
            task.error = str(exc)

    @app.post("/tasks", status_code=202)
    async def create_task(req: CreateTaskRequest):
        task = Task(
            task_id=req.task_id,
            job_id=req.job_id,
            sub_query=req.sub_query,
            target_table=req.target_table,
            write_mode=req.write_mode,
            partition_column=req.partition_column,
            partition_values=req.partition_values,
        )
        tasks[task.task_id] = task
        asyncio.create_task(_run(task))
        return {"task_id": task.task_id, "status": task.status.value}

    @app.get("/tasks/{task_id}")
    def get_task(task_id: str):
        task = tasks.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="task not found")
        return task.view()

    @app.get("/tasks/{task_id}/result")
    def get_task_result(task_id: str):
        task = tasks.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="task not found")
        return {"rows_written": task.rows_written}

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    return app


app = create_app()
