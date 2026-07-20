"""시스템 리소스(CPU/메모리/디스크) 메트릭 수집 모듈 (psutil 사용).

executor 의 /metrics 응답이나 coordinator 의 헬스 모니터링에서, 현재 호스트의
자원 사용 현황을 한 번에 스냅샷으로 떠서 JSON 직렬화 가능한 dict 로 돌려주기
위한 작은 유틸이다. 값 단위는 사람이 읽기 쉽게 MB/GB 로 환산하고 소수점 자리수를
줄여 둔다.
"""

from __future__ import annotations

import psutil

# 바이트 → MB/GB 환산 상수(이진 단위 1024 기준). psutil 은 바이트로 값을 주므로
# 사용량을 사람이 읽기 쉬운 단위로 바꾸는 데 쓴다.
_MB = 1024 * 1024
_GB = 1024 * 1024 * 1024


def collect_system_metrics(disk_path: str = "/") -> dict:
    """현재 호스트의 CPU/메모리/디스크 사용량 스냅샷을 dict 로 반환한다.

    인자:
        disk_path: 사용량을 측정할 디스크 경로(기본 루트 "/"). 해당 경로가 속한
            파일시스템의 용량/사용량을 잰다.

    반환:
        다음 구조의 dict:
            - cpu_percent: 직전 짧은 구간의 CPU 사용률(%).
            - memory: total_mb / used_mb / percent.
            - disk: path / total_gb / used_gb / percent.

    참고:
        CPU 사용률은 ``interval=0.1`` 로 측정한다. 0(논블로킹)으로 두면 직전 호출과의
        차이로 계산돼 첫 호출이 0% 로 나오므로, 0.1초 동안 짧게 블로킹하며 실제
        구간 사용률을 재기 위함이다(그만큼 이 호출은 약 0.1초가 걸린다).
    """
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
