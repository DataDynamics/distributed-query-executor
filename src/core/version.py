"""애플리케이션 버전의 **유일한 출처(single source of truth)**다.

이 프로젝트는 pip 로 설치(distribution)되지 않고 소스 트리를 그대로(`PYTHONPATH=src`)
구동하므로, ``importlib.metadata.version()`` 대신 이 모듈의 :data:`__version__` 을
런타임 버전으로 쓴다. ``pyproject.toml`` 은 ``dynamic = ["version"]`` + ``attr`` 로 이
값을 그대로 읽으므로(설정: ``[tool.setuptools.dynamic]``), 버전을 올릴 때는 **이 한 줄만**
고치면 패키지 메타데이터·기동 배너·`--version` 이 모두 함께 따라온다.

배포 트리에는 ``.git`` 이 없으므로(설치 시 rsync 로 제외), git 리비전은 있으면 덧붙이는
best-effort 정보다(:func:`git_revision`).
"""

from __future__ import annotations

import os
import subprocess
from functools import lru_cache
from pathlib import Path

# 버전을 올릴 때 고치는 유일한 줄이다. SemVer 의 MAJOR.MINOR.PATCH 형식을 권장한다.
__version__ = "0.4.0"


@lru_cache(maxsize=1)
def git_revision() -> str | None:
    """배포/개발 트리의 짧은 git 리비전을 best-effort 로 반환한다(없으면 None).

    우선순위:
        1) 환경변수 ``QUERY_EXECUTOR_GIT_SHA`` — 설치 스크립트가 빌드 시 각인할 수 있는 값.
        2) 소스 트리에 ``.git`` 이 있으면 ``git rev-parse --short HEAD``.

    git 미설치·`.git` 부재·타임아웃 등 어떤 실패도 예외로 올리지 않고 None 을 돌려준다
    (버전 표시는 부가 정보이지 기동을 막을 이유가 아니다). 결과는 캐시한다.
    """
    stamped = os.getenv("QUERY_EXECUTOR_GIT_SHA")
    if stamped:
        return stamped.strip() or None

    # 이 파일을 기준으로 한 저장소 루트 후보다. src/core/version.py 에서 세 단계 위다.
    repo_root = Path(__file__).resolve().parents[2]
    if not (repo_root / ".git").exists():
        return None
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=1.0,
            check=True,
        )
        return out.stdout.strip() or None
    except Exception:
        # git 이 없거나 실패하거나 타임아웃이면 조용히 무시한다. 부가 정보일 뿐이기 때문이다.
        return None


def version_string() -> str:
    """표시용 버전 문자열을 만든다. ``0.1.0`` 이거나, 리비전이 있으면 ``0.1.0+g860f3cd`` 가 된다."""
    rev = git_revision()
    return f"{__version__}+g{rev}" if rev else __version__
