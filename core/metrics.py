"""시스템 리소스(CPU/메모리/디스크) 메트릭 수집 (psutil 사용)."""

from __future__ import annotations

import psutil

_MB = 1024 * 1024
_GB = 1024 * 1024 * 1024


def collect_system_metrics(disk_path: str = "/") -> dict:
    """현재 호스트의 CPU/메모리/디스크 사용량을 dict 로 반환한다."""
    cpu_percent = psutil.cpu_percent(interval=0.1)
    vm = psutil.virtual_memory()
    disk = psutil.disk_usage(disk_path)
    return {
        "cpu_percent": cpu_percent,
        "memory": {
            "total_mb": round(vm.total / _MB, 1),
            "used_mb": round(vm.used / _MB, 1),
            "percent": vm.percent,
        },
        "disk": {
            "path": disk_path,
            "total_gb": round(disk.total / _GB, 2),
            "used_gb": round(disk.used / _GB, 2),
            "percent": disk.percent,
        },
    }
