"""부하를 걸 요청 하나의 명세다. 요청이 끝나는 방식에 따라 type 을 둘로 나눈다.

- ``async`` 는 제출 응답이 "접수됐다"는 뜻일 뿐인 요청이다. 응답에서 id 를 꺼내 상태 URL 을
  폴링하다가 상태 필드가 종료 값 중 하나가 되면 완료로 본다. ``POST /jobs`` 가 여기에 해당한다.
- ``sync`` 는 응답을 받는 순간이 곧 완료인 요청이다. ``POST /query-execute`` 나 dry_run 이 여기에
  해당한다.

**type 은 응답을 보고 추측하지 않고 명시한다.** "응답에 id 가 있으면 폴링한다"처럼 추측하게 두면,
async 로 의도한 요청이 id 없이 200 을 받았을 때 성공으로 조용히 집계되어 설정 실수가 묻힌다. 명시해
두면 그 경우를 ``missing_id`` 오류로 잡을 수 있다. 다만 기본값은 경로에서 정한다(``/jobs`` 는 async,
그 밖은 sync). 그래야 ``--body job.json`` 만 주던 예전 사용법이 그대로 동작한다.

폴링 규칙(상태 경로, id·상태 필드, 종료·성공 값, 취소 경로)은 설정으로 뺐다. 기본값은 이 저장소의
``/jobs`` 규칙이라 대부분 건드릴 일이 없고, 다른 비동기 API 에 쓸 때만 바꾼다.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote

REQUEST_TYPES = ("async", "sync")
METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE")

#: 이 저장소 ``/jobs`` 의 종료·성공 상태다. 폴링 설정의 기본값이다.
JOBS_TERMINAL = ("DONE", "PARTIAL", "FAILED", "CANCELLED")
JOBS_SUCCESS = ("DONE",)

_ID_PLACEHOLDER = "${id}"


@dataclass
class PollSpec:
    """async 요청의 완료 판정 규칙이다. 경로의 ``${id}`` 는 제출 응답에서 꺼낸 id 로 바뀐다."""

    path: str = "/jobs/${id}/status"
    id_field: str = "job_id"
    status_field: str = "status"
    terminal: Tuple[str, ...] = JOBS_TERMINAL
    success: Tuple[str, ...] = JOBS_SUCCESS
    # 테스트를 멈출 때 끝나지 않은 요청을 취소하는 경로다. 비우면 취소하지 않는다.
    cancel_path: Optional[str] = "/jobs/${id}/cancel"
    # 아래 필드가 상태 응답에 있으면 서버 기준 대기·실행 시간과 적재 행 수를 함께 잰다.
    # 없는 API 라면 해당 지표가 비어 나올 뿐 동작에는 지장이 없다.
    created_field: str = "created_at"
    started_field: str = "started_at"
    finished_field: str = "finished_at"
    rows_field: str = "total_rows_written"
    error_field: str = "error"

    def status_path(self, request_id: str) -> str:
        return self.path.replace(_ID_PLACEHOLDER, quote(str(request_id), safe=""))

    def cancel_url(self, request_id: str) -> Optional[str]:
        if not self.cancel_path:
            return None
        return self.cancel_path.replace(_ID_PLACEHOLDER, quote(str(request_id), safe=""))


@dataclass
class RequestSpec:
    """부하를 걸 요청이다. body 가 None 이면 본문 없이 보낸다(GET 등)."""

    type: str = "async"
    method: str = "POST"
    path: str = "/jobs"
    body: Optional[Dict[str, Any]] = None
    # sync 요청에서 성공으로 볼 HTTP 상태 코드다. 비어 있으면 2xx 전체를 성공으로 본다.
    success_status: Tuple[int, ...] = ()
    poll: PollSpec = field(default_factory=PollSpec)

    @property
    def is_async(self) -> bool:
        return self.type == "async"

    def is_success_status(self, code: int) -> bool:
        if self.success_status:
            return code in self.success_status
        return 200 <= code < 300

    def validate(self) -> None:
        """시작 전에 명세의 모순을 잡는다. 틀리면 ValueError 를 던진다."""
        if self.type not in REQUEST_TYPES:
            raise ValueError(f"type 은 {'/'.join(REQUEST_TYPES)} 중 하나여야 합니다: {self.type!r}")
        if self.method not in METHODS:
            raise ValueError(f"method 는 {'/'.join(METHODS)} 중 하나여야 합니다: {self.method!r}")
        if not self.path.startswith("/"):
            raise ValueError(f"path 는 '/' 로 시작해야 합니다: {self.path!r}")
        if self.is_async:
            p = self.poll
            if _ID_PLACEHOLDER not in p.path:
                raise ValueError(f"폴링 경로에 {_ID_PLACEHOLDER} 가 없습니다: {p.path!r}")
            if not p.terminal:
                raise ValueError("폴링 종료 상태(terminal)가 비어 있습니다")
            missing = [s for s in p.success if s not in p.terminal]
            if missing:
                raise ValueError(f"성공 상태가 종료 상태에 없습니다: {', '.join(missing)}")
            if self.body and self.body.get("dry_run"):
                raise ValueError(
                    "dry_run 요청은 job 을 만들지 않고 계획만 돌려주므로 sync 로 보내야 합니다")

    def describe(self) -> str:
        return f"{self.type} {self.method} {self.path}"


def default_type(path: str, body: Optional[Dict[str, Any]]) -> str:
    """type 을 주지 않았을 때의 기본값이다. ``/jobs`` 제출만 async 이고 나머지는 sync 다.

    dry_run 은 ``/jobs`` 여도 job 을 만들지 않고 계획만 200 으로 돌려주므로 sync 다.
    """
    if path.rstrip("/") == "/jobs" and not (body and body.get("dry_run")):
        return "async"
    return "sync"


def split_list(value: Any) -> List[str]:
    """``"A,B"`` 나 YAML 목록을 문자열 목록으로 바꾼다."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(v).strip() for v in value if str(v).strip()]
    return [v.strip() for v in str(value).split(",") if v.strip()]


_STATUS_RANGE = re.compile(r"^([1-5])xx$", re.IGNORECASE)


def parse_status_codes(value: Any) -> Tuple[int, ...]:
    """``200,201`` 이나 ``2xx`` 같은 성공 상태 코드 표기를 정수 튜플로 바꾼다."""
    codes: List[int] = []
    for item in split_list(value):
        m = _STATUS_RANGE.match(item)
        if m:
            base = int(m.group(1)) * 100
            codes.extend(range(base, base + 100))
            continue
        try:
            code = int(item)
        except ValueError:
            raise ValueError(f"HTTP 상태 코드가 아닙니다: {item!r} (예: 200,201 또는 2xx)") from None
        if not 100 <= code <= 599:
            raise ValueError(f"HTTP 상태 코드 범위를 벗어났습니다: {code}")
        codes.append(code)
    return tuple(dict.fromkeys(codes))


def build_spec(opts: Dict[str, Any], body: Optional[Dict[str, Any]]) -> RequestSpec:
    """cli 가 합친 옵션 dict 에서 요청 명세를 만든다. 키가 없으면 PollSpec 기본값을 쓴다."""
    path = str(opts.get("path") or "/jobs")
    method = str(opts.get("method") or "POST").upper()
    rtype = str(opts.get("type") or default_type(path, body)).lower()
    poll = PollSpec()
    for key, attr in POLL_OPTION_MAP.items():
        value = opts.get(key)
        if value is None:
            continue
        if attr in ("terminal", "success"):
            setattr(poll, attr, tuple(split_list(value)))
        elif attr == "cancel_path":
            poll.cancel_path = str(value) or None
        else:
            setattr(poll, attr, str(value))
    spec = RequestSpec(
        type=rtype, method=method, path=path, body=body,
        success_status=parse_status_codes(opts.get("success_status")),
        poll=poll,
    )
    spec.validate()
    return spec


#: 옵션 키(명령행·평평한 YAML) → PollSpec 속성
POLL_OPTION_MAP: Dict[str, str] = {
    "poll_path": "path",
    "poll_id_field": "id_field",
    "poll_status_field": "status_field",
    "poll_terminal": "terminal",
    "poll_success": "success",
    "cancel_path": "cancel_path",
}

#: 시나리오 YAML 의 ``request:`` 블록 키 → 옵션 키. ``poll:`` 하위 블록은 POLL_YAML_MAP 을 쓴다.
REQUEST_YAML_MAP: Dict[str, str] = {
    "type": "type",
    "method": "method",
    "path": "path",
    "body": "body",
    "success_status": "success_status",
}
POLL_YAML_MAP: Dict[str, str] = {
    "path": "poll_path",
    "id_field": "poll_id_field",
    "status_field": "poll_status_field",
    "terminal": "poll_terminal",
    "success": "poll_success",
    "cancel_path": "cancel_path",
}


def flatten_request_block(block: Dict[str, Any]) -> Dict[str, Any]:
    """시나리오의 중첩 ``request:`` 블록을 평평한 옵션 키로 편다. 모르는 키는 ValueError 다."""
    if not isinstance(block, dict):
        raise ValueError("request 는 key: value 매핑이어야 합니다")
    out: Dict[str, Any] = {}
    for k, v in block.items():
        key = str(k).replace("-", "_")
        if key == "poll":
            if not isinstance(v, dict):
                raise ValueError("request.poll 은 key: value 매핑이어야 합니다")
            for pk, pv in v.items():
                pkey = str(pk).replace("-", "_")
                if pkey not in POLL_YAML_MAP:
                    raise ValueError(f"request.poll 에 알 수 없는 키가 있습니다: {pk}")
                out[POLL_YAML_MAP[pkey]] = pv
        elif key in REQUEST_YAML_MAP:
            out[REQUEST_YAML_MAP[key]] = v
        else:
            raise ValueError(f"request 에 알 수 없는 키가 있습니다: {k}")
    return out


def spec_to_yaml_block(spec: RequestSpec, body_ref: Any) -> Dict[str, Any]:
    """명세를 시나리오 YAML 의 ``request:`` 블록으로 되돌린다. 기본값과 같은 폴링 항목은 생략한다."""
    block: Dict[str, Any] = {"type": spec.type, "method": spec.method, "path": spec.path}
    if body_ref is not None:
        block["body"] = body_ref
    if spec.success_status:
        block["success_status"] = _compact_codes(spec.success_status)
    if spec.is_async:
        default = PollSpec()
        poll: Dict[str, Any] = {}
        for yaml_key, opt_key in POLL_YAML_MAP.items():
            attr = POLL_OPTION_MAP[opt_key]
            value = getattr(spec.poll, attr)
            if value != getattr(default, attr):
                poll[yaml_key] = list(value) if isinstance(value, tuple) else (value or "")
        if poll:
            block["poll"] = poll
    return block


def _compact_codes(codes: Sequence[int]) -> str:
    """``200..299`` 처럼 백 단위를 통째로 담은 코드는 ``2xx`` 로 줄여 적는다."""
    s = set(codes)
    parts: List[str] = []
    for base in range(100, 600, 100):
        block = set(range(base, base + 100))
        if block <= s:
            parts.append(f"{base // 100}xx")
            s -= block
    parts.extend(str(c) for c in sorted(s))
    return ",".join(parts)
