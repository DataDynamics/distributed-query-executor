"""coordinator HTTP 호출을 감싼다. 요청 전송·상태 폴링·취소·클러스터 조회 네 가지만 쓴다.

응답을 성공·실패로 분류하는 일은 여기서 하지 않는다. 같은 200 이라도 async 요청이면 id 가 있어야
성공이고 sync 요청이면 설정한 상태 코드에 드는지를 봐야 하므로, 판정은 요청 명세를 아는 runner 가
맡는다. 이 모듈은 상태 코드·JSON 본문·지연·오류 사유를 그대로 넘긴다.

coordinator 를 여럿 주면 요청은 라운드로빈으로 나눠 보내고, 상태 폴링과 취소는 그 요청을 받은
coordinator 로 보낸다. 단일 coordinator 의 InMemory 저장소는 다른 인스턴스가 만든 job 을 모르기
때문이다(공유 저장소를 쓰는 멀티 coordinator 여도 이 방식이 틀리지 않는다).

예외는 밖으로 던지지 않고 결과 객체에 담는다. 부하 테스트에서는 연결 실패도 측정할 결과이지 도구를
멈출 이유가 아니기 때문이다.
"""

from __future__ import annotations

import itertools
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

import httpx


@dataclass
class Response:
    """HTTP 호출 한 번의 결과다. 연결 오류면 status_code 가 None 이다."""

    coordinator: str
    status_code: Optional[int] = None
    data: Any = None                  # JSON 본문(파싱 실패나 본문 없음이면 None)
    latency: Optional[float] = None   # 응답을 받았을 때만 있다
    error: Optional[str] = None       # 연결 오류이거나 2xx 가 아닐 때의 사유
    retry_after: Optional[float] = None


def _error_text(resp: httpx.Response, data: Any) -> str:
    """오류 응답에서 사람이 읽을 사유를 뽑는다(error_code·detail 순)."""
    if isinstance(data, dict):
        if data.get("error_code"):
            return f"HTTP {resp.status_code} {data['error_code']}: {data.get('message', '')}"
        if data.get("detail") is not None:
            return f"HTTP {resp.status_code} {data['detail']}"
    if data is not None:
        return f"HTTP {resp.status_code} {str(data)[:200]}"
    return f"HTTP {resp.status_code} {resp.text[:200]}"


def _retry_after(resp: httpx.Response) -> Optional[float]:
    value = resp.headers.get("Retry-After")
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None  # HTTP-date 형식은 coordinator 가 쓰지 않으므로 무시한다


def _to_response(base: str, resp: httpx.Response, latency: float) -> Response:
    try:
        data = resp.json() if resp.content else None
    except ValueError:
        data = None
    ok = 200 <= resp.status_code < 300
    return Response(
        coordinator=base, status_code=resp.status_code, data=data, latency=latency,
        error=None if ok else _error_text(resp, data),
        retry_after=_retry_after(resp) if resp.status_code == 429 else None,
    )


def parse_server_time(value: Any) -> Optional[datetime]:
    """서버 응답의 ``*_at`` 값(``yyyy-MM-dd HH:mm:ss.sss`` 또는 ISO)을 datetime 으로 바꾼다.

    서버는 KST naive 로 표기하므로 같은 응답 안의 두 시각을 빼는 데만 쓴다(로컬 시계와 섞지 않는다).
    """
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def server_seconds(start: Any, end: Any) -> Optional[float]:
    a, b = parse_server_time(start), parse_server_time(end)
    if a is None or b is None:
        return None
    return (b - a).total_seconds()


class CoordinatorClient:
    """부하 테스트용 coordinator 클라이언트다. ``async with`` 로 연결 풀을 관리한다."""

    def __init__(
        self,
        base_urls: Sequence[str],
        timeout: float = 30.0,
        max_connections: int = 100,
        headers: Optional[Dict[str, str]] = None,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ) -> None:
        if not base_urls:
            raise ValueError("coordinator URL 이 하나 이상 필요합니다")
        self.base_urls: List[str] = [u.rstrip("/") for u in base_urls]
        self._rr = itertools.cycle(self.base_urls)
        self._client = httpx.AsyncClient(
            timeout=timeout,
            headers=headers or {},
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_connections,
            ),
            transport=transport,
        )

    async def __aenter__(self) -> "CoordinatorClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _call(self, base: str, method: str, path: str, body: Any = None,
                    headers: Optional[Dict[str, str]] = None,
                    timeout: Optional[float] = None) -> Response:
        kwargs: Dict[str, Any] = {"headers": headers}
        if body is not None:
            kwargs["json"] = body
        if timeout is not None:
            kwargs["timeout"] = timeout
        start = time.monotonic()
        try:
            resp = await self._client.request(method, f"{base}{path}", **kwargs)
        except httpx.HTTPError as exc:
            return Response(base, error=f"{type(exc).__name__}: {exc}")
        return _to_response(base, resp, time.monotonic() - start)

    async def send(self, method: str, path: str, body: Any = None,
                   headers: Optional[Dict[str, str]] = None) -> Response:
        """부하 요청 한 건을 다음 차례의 coordinator 로 보낸다."""
        return await self._call(next(self._rr), method, path, body, headers)

    async def poll(self, base: str, path: str, timeout: Optional[float] = None) -> Response:
        """상태 URL 을 조회한다. 폴링은 짧아야 하므로 요청 타임아웃과 따로 줄 수 있다."""
        return await self._call(base, "GET", path, timeout=timeout)

    async def cancel(self, base: str, path: str) -> bool:
        """취소를 요청한다. 이미 끝난 요청의 409 도 목적은 이룬 것이라 성공으로 본다."""
        r = await self._call(base, "POST", path)
        return r.status_code in (200, 202, 204, 409)

    async def health(self, base: Optional[str] = None, timeout: float = 3.0) -> Response:
        """``GET /health`` 로 접속을 확인한다. 마법사가 시작 전에 부른다."""
        return await self._call(base or self.base_urls[0], "GET", "/health", timeout=timeout)

    async def cluster(self, base: Optional[str] = None, refresh: bool = True) -> Dict[str, Any]:
        """``GET /cluster`` 응답을 돌려준다. 실패하면 httpx 예외를 그대로 던진다(sampler 가 센다)."""
        url = f"{base or self.base_urls[0]}/cluster"
        resp = await self._client.get(url, params={"refresh": "true" if refresh else "false"})
        resp.raise_for_status()
        return resp.json()
