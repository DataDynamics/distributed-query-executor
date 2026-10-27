"""부하 테스트 한 번의 기록을 ``<로그 디렉터리>/test_results/YYYYMMDD-HHMMSS.txt`` 로 남긴다.

콘솔 요약은 터미널을 닫으면 사라지고 ``--out`` 은 매번 지정해야 하므로, 무엇을 어떤 조건으로 돌렸고 결과가
어땠는지를 테스트마다 자동으로 파일 하나에 묶어 둔다. 나중에 두 실행을 비교할 때 이 파일 하나만 있으면 다시
돌릴 수 있도록 실행한 명령, 합쳐진 입력 파라미터, 요청 본문, 콘솔 요약, 전체 결과 JSON 을 모두 담는다.

명령행과 본문에는 비밀번호·토큰이 섞일 수 있으므로 ``core.masking`` 으로 가린 뒤 쓴다. 이 파일은 운영자가
돌려 보는 로그 디렉터리에 쌓이기 때문이다. 파일 이름은 테스트 시작 시각이며, 같은 초에 둘이 시작하면 뒤에
``-2``·``-3`` 을 붙여 덮어쓰지 않는다.
"""

from __future__ import annotations

import json
import os
import shlex
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

#: 값을 다음 인자로 받는 헤더 옵션이다. 헤더 값은 이름을 보고 통째로 가린다.
_HEADER_FLAGS = ("-H", "--header")
RESULT_SUBDIR = "test_results"


def _mask(text: str) -> str:
    """비밀 필드를 가린다. core 를 못 불러오는 환경(도구만 복사한 경우)에서는 그대로 둔다."""
    try:
        from core.masking import mask_text
    except Exception:  # pragma: no cover - 저장소 밖 실행
        return text
    return mask_text(text)


def _mask_header(raw: str) -> str:
    """``이름: 값`` 형태의 헤더 인자를 가린다. Authorization 같은 민감 헤더는 값을 통째로 숨긴다."""
    name, sep, value = raw.partition(":")
    if not sep:
        return _mask(raw)
    try:
        from core.masking import mask_header_value
    except Exception:  # pragma: no cover
        return raw
    return f"{name}:{' ' if value.startswith(' ') else ''}{mask_header_value(name.strip(), value.strip())}"


def default_dir() -> Path:
    """설정의 로그 디렉터리(``log.dir``) 아래 ``test_results`` 다. 설정을 못 읽으면 ``./logs`` 를 쓴다."""
    try:
        from core.config import settings

        base = Path(settings.log_dir)
    except Exception:
        base = Path("logs")
    return base / RESULT_SUBDIR


def masked_command(argv: Sequence[str], prog: str) -> str:
    """실행한 명령을 셸에 그대로 붙여 넣을 수 있는 한 줄로 만든다(비밀값은 가린다)."""
    out = [prog]
    expect_header = False
    for arg in argv:
        if expect_header:
            out.append(_mask_header(arg))
            expect_header = False
            continue
        if arg in _HEADER_FLAGS:
            out.append(arg)
            expect_header = True
            continue
        flag, eq, value = arg.partition("=")
        if eq and flag in _HEADER_FLAGS:
            out.append(f"{flag}={_mask_header(value)}")
            continue
        out.append(_mask(arg))
    return " ".join(shlex.quote(a) for a in out)


def _json(value: Any) -> str:
    return _mask(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def render(result: Dict[str, Any], *, summary: str, command: str, cwd: str, exit_code: int,
           exit_reason: str, files: Dict[str, str], params: Dict[str, Any]) -> str:
    """기록 파일 본문이다. 사람이 위에서부터 읽도록 개요 → 명령 → 파라미터 → 본문 → 결과 → JSON 순으로 쓴다."""
    meta = result.get("meta", {})
    s = result.get("summary", {})
    lines = [
        "=" * 72,
        f" 부하 테스트 결과  run_id={meta.get('run_id', '-')}",
        "=" * 72,
        f"시작        : {meta.get('started_at', '-')}",
        f"종료        : {meta.get('finished_at', '-')}",
        f"부하 시간   : {meta.get('load_elapsed', 0):.1f}초 (전체 {meta.get('total_elapsed', 0):.1f}초)",
        f"종료 코드   : {exit_code} ({exit_reason})",
        f"종료 사유   : {meta.get('stop_reason') or '-'}",
        f"대상        : {', '.join(meta.get('coordinators') or []) or '-'}",
        f"시나리오    : {meta.get('scenario', '-')}",
        f"한 줄 요약  : 제출 {s.get('submitted', 0):,}건 · 완료 TPS {s.get('complete_tps') or 0:.2f}"
        f" · 오류율 {(s.get('error_rate') or 0) * 100:.2f}%"
        f" · 429 {(s.get('submit_outcomes') or {}).get('rejected_429', 0):,}건",
        "",
        "[테스트 커맨드]",
        command,
        f"(작업 디렉터리: {cwd})",
        "",
        "[입력 파라미터]  (명령행 > 시나리오 YAML > 기본값 순으로 합친 값)",
    ]
    width = max((len(k) for k in params), default=0)
    for key in sorted(params):
        val = params[key]
        text = json.dumps(val, ensure_ascii=False, default=str) if isinstance(val, (dict, list)) else str(val)
        lines.append(f"  {key.ljust(width)} = {_mask(text)}")
    lines += ["", "[요청 본문]", _json(meta.get("body")), "", "[테스트 결과]", summary.strip("\n"), ""]
    if files:
        lines.append("[함께 만든 파일]")
        lines += [f"  {k:<14} {v}" for k, v in files.items()]
        lines.append("")
    lines += ["[결과 JSON]  (--out 과 같은 내용: meta·summary·resources·saturation·timeline·samples)",
              _json(result), ""]
    return "\n".join(lines)


def write(text: str, started: datetime, directory: Optional[os.PathLike] = None) -> Path:
    """``YYYYMMDD-HHMMSS.txt`` 로 쓴다. 디렉터리가 없으면 만들고, 같은 이름이 있으면 번호를 붙인다."""
    d = Path(directory) if directory else default_dir()
    d.mkdir(parents=True, exist_ok=True)
    stem = started.strftime("%Y%m%d-%H%M%S")
    path = d / f"{stem}.txt"
    n = 2
    while path.exists():
        path = d / f"{stem}-{n}.txt"
        n += 1
    # 쓰는 도중 끊겨도 반쯤 쓰인 결과 파일이 남지 않게 임시 파일에 쓰고 이름을 바꾼다.
    tmp = path.with_suffix(".txt.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)
    return path
