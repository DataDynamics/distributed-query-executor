"""부하 테스트 기록 파일(``<log.dir>/test_results/*.txt``)을 대시보드에 보여 주는 조회 함수다.

기록 파일은 ``bin/load-test`` 가 테스트마다 남긴다(``tools/load/resultfile.py``). 대시보드는 그 디렉터리를 읽기만
하며 파일을 만들거나 지우지 않는다. 목록에는 파일 머리의 개요 줄(시작 시각·종료 코드·대상·한 줄 요약 등)만
읽어 담는다. 긴 테스트의 기록은 결과 JSON 때문에 수 MB 가 될 수 있어, 목록을 만들 때마다 파일 전체를 읽으면
폴링할 때마다 디스크를 훑게 되기 때문이다.

파일 이름은 기록 도구가 만드는 모양(``YYYYMMDD-HHMMSS[-N].txt``)만 받는다. 요청 경로로 들어온 이름을 그대로
경로에 붙이면 ``../`` 로 디렉터리 밖을 읽을 수 있으므로, 모양을 먼저 확인하고 디렉터리 안에 있는지도 다시 본다.
기록 도구와 같은 호스트에서 돌 때만 파일이 보인다는 점에 주의한다(다른 호스트에서 돌린 테스트는 그 호스트의
로그 디렉터리에 남는다).
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

RESULT_SUBDIR = "test_results"   # tools/load/resultfile.RESULT_SUBDIR 와 같은 값이다
_NAME_RE = re.compile(r"^\d{8}-\d{6}(?:-\d+)?\.txt$")
_HEAD_BYTES = 4096               # 개요 줄을 찾으려고 읽는 앞부분 크기
JSON_MARKER = "[결과 JSON]"

# 머리 줄 이름 → 응답 필드 이름. 값은 "이름 : 값" 모양이다(resultfile.render 참고).
_HEAD_FIELDS = {
    "시작": "started_at", "종료": "finished_at", "부하 시간": "load_time", "종료 코드": "exit",
    "종료 사유": "stop_reason", "대상": "target", "시나리오": "scenario", "한 줄 요약": "headline",
}


def results_dir(settings) -> Path:
    """기록 파일 디렉터리다. 기록 도구와 같은 규칙(설정의 ``log.dir`` 아래 ``test_results``)을 따른다."""
    return Path(settings.log_dir) / RESULT_SUBDIR


def valid_name(name: str) -> bool:
    return bool(_NAME_RE.match(name or ""))


def parse_head(text: str) -> Dict[str, Optional[str]]:
    """기록 파일 앞부분에서 run_id·개요 줄·실행 명령을 꺼낸다. 모르는 줄은 무시한다."""
    out: Dict[str, Optional[str]] = {v: None for v in _HEAD_FIELDS.values()}
    out.update(run_id=None, command=None, exit_code=None)
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if "run_id=" in line and out["run_id"] is None:
            out["run_id"] = line.split("run_id=", 1)[1].strip() or None
        elif line.startswith("[테스트 커맨드]") and i + 1 < len(lines):
            out["command"] = lines[i + 1].strip() or None
            break  # 명령 뒤로는 개요 줄이 없다
        else:
            key, sep, value = line.partition(":")
            field = _HEAD_FIELDS.get(key.strip()) if sep else None
            if field and out[field] is None:
                out[field] = value.strip()
    m = re.match(r"(-?\d+)", out.get("exit") or "")
    out["exit_code"] = int(m.group(1)) if m else None
    return out


def list_results(directory: Path, limit: int = 100, offset: int = 0) -> Dict[str, object]:
    """기록 파일 목록이다. 이름(=시작 시각) 내림차순으로 최신이 먼저 오고, 디렉터리가 없으면 빈 목록이다."""
    try:
        names = sorted((n for n in os.listdir(directory) if valid_name(n)), reverse=True)
    except FileNotFoundError:
        return {"dir": str(directory), "exists": False, "total": 0, "rows": []}
    rows: List[Dict[str, object]] = []
    for name in names[offset:offset + limit]:
        path = directory / name
        try:
            st = path.stat()
            with open(path, "rb") as fp:
                head = fp.read(_HEAD_BYTES).decode("utf-8", errors="replace")
        except OSError:
            continue  # 목록을 읽는 사이 지워진 파일
        row: Dict[str, object] = {"name": name, "size": st.st_size,
                                  "modified_at": datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds")}
        row.update(parse_head(head))
        rows.append(row)
    return {"dir": str(directory), "exists": True, "total": len(names), "rows": rows}


def read_result(directory: Path, name: str, *, full: bool = True) -> str:
    """기록 파일 본문이다. ``full=False`` 면 결과 JSON 절 앞까지만 돌려준다(화면에서 읽을 부분).

    이름이 기록 파일 모양이 아니거나 디렉터리 밖을 가리키면 ValueError, 없으면 FileNotFoundError 다.
    """
    if not valid_name(name):
        raise ValueError(f"기록 파일 이름이 아니다: {name!r}")
    base = directory.resolve()
    path = (directory / name).resolve()
    if path.parent != base:
        raise ValueError(f"기록 디렉터리 밖의 파일이다: {name!r}")
    text = path.read_text(encoding="utf-8", errors="replace")
    if not full:
        cut = text.find("\n" + JSON_MARKER)
        if cut >= 0:
            text = text[:cut + 1] + f"\n({JSON_MARKER} 절은 대시보드의 '원본' 링크로 볼 수 있습니다)\n"
    return text
