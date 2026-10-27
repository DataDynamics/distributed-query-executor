"""coordinator 의 지정 파일이 바뀌면 모든 executor 에 한 번에 반영하는 동기화 관리자다.

``sync.interval_s`` 마다 앱 홈 아래 ``sync.paths`` 에서 ``sync.patterns`` 에 맞는 파일을 훑어 내용 해시(버전)를
만든다. 버전이 바뀌고 ``sync.settle_s`` 동안 그대로이며 ``.py`` 문법 검사를 통과하면 새 원본 버전으로 확정하고
다음 순서로 전환한다.

1. **prepare** — 버전이 다른 모든 executor 에 번들을 보내 풀고 검증하게 한다. 현재 버전은 아직 그대로다.
2. 하나라도 준비에 실패하면 준비한 executor 에 **abort** 를 보내 모두 원래대로 두고, ``sync.retry_backoff_s``
   뒤에 다시 시도한다. 일부만 바뀌는 일은 없다.
3. 모두 준비되면 **drain** — 새 job 의 실행 시작을 막고(PENDING 으로 대기) 실행 중인 job 이 끝나기를
   ``sync.drain_timeout_s`` 까지 기다린다. 한 job 의 task 가 두 버전에 걸치지 않게 하려는 것이다.
4. **commit** — 모든 executor 를 새 버전으로 바꾸고, 전환된 버전(``active_version``)을 갱신한 뒤 job 실행을
   다시 연다. drain 이 시간을 넘겼으면 그대로 전환하고 두 버전에 걸친 job 을 경고로 남긴다.

전환된 버전과 다른 executor 는 디스패치 후보에서 빠진다(:meth:`eligible`). 내려가 있다가 돌아왔거나 commit
에 실패한 executor 는 그 executor 하나에만 prepare·commit 을 다시 해 따라잡게 하고, 맞춰지면 다시 후보가
된다. 디스패처는 task 에 전환된 버전을 실어 보내고 executor 는 자기 버전과 다르면 409 로 거절하므로, 이
coordinator 가 아직 모르는 사이 버전이 어긋난 executor 에도 task 가 들어가지 않는다.

coordinator 가 여러 대면 원본으로 삼을 한 대만 ``sync.push=true`` 로 둔다. 나머지는 감시·배포하지 않고 버전
관문도 걸지 않는다.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from urllib.parse import quote

import httpx

from core import bundle as bundle_mod
from core.bundle import Manifest, SyncRules
from core.http_logging import outgoing_headers
from core.perf import perf
from core.timeutil import now_iso

logger = logging.getLogger(__name__)


def app_home() -> str:
    """앱 홈(src/ 의 상위)이다."""
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(bundle_mod.__file__))))


@dataclass
class ExecutorSync:
    """executor 하나의 동기화 상태다(coordinator 가 본 값)."""

    url: str
    version: Optional[str] = None
    reachable: bool = False
    enabled: bool = False
    shared: bool = False
    last_error: Optional[str] = None
    checked_at: Optional[str] = None
    next_try: float = 0.0

    def view(self, active: Optional[str]) -> dict:
        return {
            "url": self.url, "version": self.version, "reachable": self.reachable, "enabled": self.enabled,
            "shared": self.shared, "in_sync": bool(active) and (self.shared or self.version == active),
            "last_error": self.last_error, "checked_at": self.checked_at,
        }


@dataclass
class _Pending:
    version: str = ""
    seen_at: float = 0.0
    errors: List[str] = field(default_factory=list)


class BundleSyncer:
    """감시·배포 루프와 디스패치 버전 관문이다. 디스패처의 ``bundle_gate`` 로 붙는다."""

    def __init__(self, settings, admission, *, base: Optional[str] = None, transport=None):
        """설정에서 감시 규칙과 주기를 읽고 executor 마다 상태 칸을 만든다.

        ``admission`` 은 drain 할 때 새 job 의 실행 시작을 막고 실행 중인 job 수를 기다리는 데 쓴다.
        ``base`` 는 원본 트리의 앱 홈이며 비우면 ``sync.base_dir``, 그것도 비면 설치 위치를 쓴다. 비교가
        심링크와 상대 경로에 흔들리지 않도록 realpath 로 고정한다. ``transport`` 는 테스트에서 executor 앱을
        ASGI 로 직접 붙이려고 두는 자리다.
        """
        self.settings = settings
        self.admission = admission
        self.rules = SyncRules.from_settings(settings)
        self.base = os.path.realpath(base or getattr(settings, "sync_base_dir", "") or app_home())
        self.interval_s = max(0.5, float(settings.sync_interval_s))
        self.settle_s = max(0.0, float(settings.sync_settle_s))
        self.drain_timeout_s = max(0.0, float(settings.sync_drain_timeout_s))
        self.retry_backoff_s = max(1.0, float(settings.sync_retry_backoff_s))
        self.max_bytes = int(float(settings.sync_max_bundle_mb) * 1048576)
        self.host = bundle_mod.host_id()
        self.executors: Dict[str, ExecutorSync] = {u: ExecutorSync(u) for u in settings.executors if u}
        self._transport = transport  # 테스트에서 httpx.MockTransport 를 넣는다
        self._cache = bundle_mod._HashCache()
        self._pending = _Pending()
        self.source_version: Optional[str] = None
        self.source_files = 0
        self.source_bytes = 0
        self.source_errors: List[str] = []
        self.active_version: Optional[str] = None
        self.last_transition: Optional[dict] = None
        self.last_error: Optional[str] = None
        self._retry_at = 0.0
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._wake = asyncio.Event()
        perf.gauge("sync", self.perf_gauges)

    # ───────── 디스패치 관문 ─────────

    @property
    def enforcing(self) -> bool:
        """전환된 버전이 정해진 뒤부터 관문을 건다. 그 전에는(처음 맞추기 전) 모든 executor 를 쓴다."""
        return self.active_version is not None

    def eligible(self, url: str) -> bool:
        """이 executor 에 task 를 보내도 되는지 판정한다.

        목록에 없는 주소이거나 아직 전환한 버전이 없으면 막지 않는다. 공유 모드 executor 는 coordinator 와
        같은 파일을 보므로 버전과 무관하게 언제나 후보다.
        """
        st = self.executors.get(url)
        if st is None or self.active_version is None:
            return True
        return st.shared or st.version == self.active_version

    def perf_gauges(self) -> dict:
        """성능 지표 로그의 ``sync.*`` 게이지다. 메모리 값만 읽으므로 기록 주기마다 불려도 부담이 없다."""
        stale = sum(1 for u in self.executors if not self.eligible(u))
        return {"stale_executors": stale, "executors": len(self.executors),
                "draining": 1 if self.admission.paused else 0}

    # ───────── 상태 ─────────

    def status(self) -> dict:
        """``GET /sync/status`` 응답이다. 원본·대기·전환 버전과 executor 별 상태를 한 번에 보여 준다.

        ``pending_version`` 은 settle 이나 재시도를 기다리는 버전만 담고, 이미 확정된 원본 버전과 같으면
        None 으로 둬서 "아직 나가지 않은 변경"이 있는지 바로 읽히게 한다.
        """
        return {
            "enabled": True, "base": self.base, "paths": list(self.rules.paths),
            "patterns": list(self.rules.patterns), "exclude": list(self.rules.exclude),
            "source_version": self.source_version, "source_files": self.source_files,
            "source_bytes": self.source_bytes, "source_errors": self.source_errors,
            "pending_version": self._pending.version if self._pending.version != self.source_version else None,
            "active_version": self.active_version, "draining": self.admission.paused,
            "last_transition": self.last_transition, "last_error": self.last_error,
            "executors": [st.view(self.active_version) for st in self.executors.values()],
        }

    # ───────── 루프 ─────────

    def start(self) -> None:
        """lifespan 에서 부른다. 감시 루프를 백그라운드 task 로 띄운다."""
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """감시 루프를 멈춘다. drain 도중에 멈춰도 job 실행이 막힌 채 남지 않도록 관문을 반드시 연다."""
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None
        self.admission.resume()

    def wake(self) -> None:
        """다음 주기를 기다리지 않고 루프를 깨운다."""
        self._wake.set()

    async def _loop(self) -> None:
        """``interval_s`` 마다 :meth:`run_once` 를 돌린다. 한 주기의 실패가 루프를 끝내지 않게 예외는 로그로만 남긴다."""
        while True:
            try:
                await self.run_once()
            except Exception:
                logger.exception("파일 동기화 주기 처리 실패")
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.interval_s)
            except asyncio.TimeoutError:
                pass

    def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=5.0), transport=self._transport,
                                 headers=outgoing_headers())

    async def run_once(self, *, force: bool = False) -> dict:
        """한 주기: 원본을 훑고, executor 상태를 읽고, 필요하면 전환하거나 뒤처진 executor 를 맞춘다."""
        async with self._lock:
            manifest = await asyncio.to_thread(bundle_mod.scan, self.base, self.rules, self._cache)
            self._observe_source(manifest, force=force)
            async with self._client() as client:
                await asyncio.gather(*(self._refresh(client, st) for st in self.executors.values()))
                self._adopt_initial_version()
                if self.source_version and self.source_version != self.active_version:
                    if force or time.monotonic() >= self._retry_at:
                        await self._transition(client, manifest)
                elif self.active_version:
                    await self._catch_up(client, manifest)
            return self.status()

    def _observe_source(self, manifest: Manifest, *, force: bool) -> None:
        """원본 버전이 settle_s 동안 그대로이고 문법 오류가 없을 때만 확정한다."""
        now = time.monotonic()
        if manifest.version != self._pending.version:
            self._pending = _Pending(manifest.version, now, bundle_mod.syntax_errors(manifest))
            if manifest.version != self.source_version:
                logger.info("동기화 대상 변경 감지 version=%s (파일 %d개) — %.0f초 동안 그대로면 배포",
                            manifest.version, len(manifest.files), self.settle_s)
        if self._pending.errors:
            if self.source_errors != self._pending.errors:
                logger.warning("동기화 대상에 문법 오류가 있어 배포하지 않음: %s", "; ".join(self._pending.errors))
            self.source_errors = self._pending.errors
            return
        self.source_errors = []
        if (force or now - self._pending.seen_at >= self.settle_s) and self.source_version != manifest.version:
            self.source_version = manifest.version
            self.source_files, self.source_bytes = len(manifest.files), manifest.total_bytes
            self._retry_at = 0.0

    def _adopt_initial_version(self) -> None:
        """기동 직후 아직 전환한 적이 없을 때, 닿는 executor 가 모두 같은 버전이면 그것을 현재 버전으로 삼는다."""
        if self.active_version is not None:
            return
        versions = {st.version for st in self.executors.values() if st.reachable and st.enabled and not st.shared}
        shared_only = all(st.shared for st in self.executors.values() if st.reachable and st.enabled)
        if len(versions) == 1:
            self.active_version = versions.pop()
        elif not versions and shared_only and any(st.reachable for st in self.executors.values()):
            self.active_version = self.source_version

    async def _refresh(self, client: httpx.AsyncClient, st: ExecutorSync) -> None:
        """executor 의 ``/sync/status`` 를 읽어 도달 여부·버전·공유 모드를 갱신한다.

        응답이 없으면 ``reachable=False`` 로 두고 버전은 지우지 않는다. 내려간 executor 는 어차피 디스패치
        후보에서 빠지고, 돌아오면 다음 주기에 새 버전을 다시 읽기 때문이다.
        """
        st.checked_at = now_iso()
        try:
            resp = await client.get(f"{st.url}/sync/status", timeout=5.0)
            resp.raise_for_status()
            body = resp.json()
        except Exception as exc:
            st.reachable = False
            st.last_error = f"상태 조회 실패: {exc}"
            return
        st.reachable = True
        st.enabled = bool(body.get("enabled")) and bool(body.get("configured"))
        st.version = body.get("version")
        # 공유 모드는 prepare 를 한 번 해 봐야 안다. executor 가 알려 준 값이 없으면 우리 쪽에서 판정한다.
        st.shared = bool(body.get("shared")) or (
            body.get("host_id") == self.host and os.path.realpath(body.get("base") or "/") == self.base
        )
        if not st.enabled:
            st.last_error = "executor 에서 sync.enabled 가 꺼져 있거나 paths·patterns 가 비어 있다"

    # ───────── 2단계 전환 ─────────

    def _targets(self) -> List[ExecutorSync]:
        """지금 번들을 보낼 수 있는 executor 다(닿고, 동기화를 켜고 경로·패턴을 설정한 것)."""
        return [st for st in self.executors.values() if st.reachable and st.enabled]

    async def _prepare(self, client: httpx.AsyncClient, st: ExecutorSync, version: str, data: bytes) -> bool:
        """executor 에 번들을 보내 풀고 검증하게 한다(현재 버전은 그대로다). 성공 여부만 돌려준다.

        원본 호스트와 앱 홈을 함께 보내 executor 가 공유 모드인지 스스로 판정하게 하고, 그 판정을 응답에서
        받아 기록한다. 실패 이유는 ``last_error`` 에 남겨 상태 화면에서 바로 보이게 한다.
        """
        try:
            resp = await client.post(
                f"{st.url}/sync/prepare", content=data,
                headers={"Content-Type": "application/gzip", "X-Bundle-Version": version,
                         # 헤더는 ASCII 만 실을 수 있으므로 경로는 URL 인코딩한다(한글 경로 등).
                         "X-Source-Host": quote(self.host, safe=""), "X-Source-Base": quote(self.base, safe="/")},
            )
            if resp.status_code >= 400:
                raise RuntimeError(_error_text(resp))
            st.shared = bool(resp.json().get("shared"))
            return True
        except Exception as exc:
            st.last_error = f"prepare 실패: {exc}"
            logger.warning("executor %s 동기화 준비 실패 version=%s: %s", st.url, version, exc)
            return False

    async def _post(self, client: httpx.AsyncClient, st: ExecutorSync, action: str, version: str) -> bool:
        """``/sync/commit`` 이나 ``/sync/abort`` 를 부른다. 실패는 예외 대신 False 와 ``last_error`` 로 알린다."""
        try:
            resp = await client.post(f"{st.url}/sync/{action}", json={"version": version})
            if resp.status_code >= 400:
                raise RuntimeError(_error_text(resp))
            return True
        except Exception as exc:
            st.last_error = f"{action} 실패: {exc}"
            logger.warning("executor %s 동기화 %s 실패 version=%s: %s", st.url, action, version, exc)
            return False

    async def _bundle(self, manifest: Manifest) -> bytes:
        """원본 목록으로 tar.gz 를 만든다. 크기 상한을 먼저 확인해 큰 묶음을 만들고 나서 버리는 일을 피한다."""
        if manifest.total_bytes > self.max_bytes:
            raise bundle_mod.BundleError(
                f"동기화 대상이 {manifest.total_bytes / 1048576:.1f}MB 로 sync.max_bundle_mb 를 넘는다")
        return await asyncio.to_thread(bundle_mod.build_bundle, manifest)

    async def _transition(self, client: httpx.AsyncClient, manifest: Manifest) -> None:
        """확정된 원본 버전으로 모든 executor 를 함께 옮긴다(prepare → drain → commit).

        한 대라도 prepare 에 실패하면 준비를 마친 executor 에 abort 를 보내고 아무것도 바꾸지 않은 채
        ``retry_backoff_s`` 뒤에 다시 시도한다. 모두 준비되면 admission 관문을 닫아 새 job 의 실행 시작을 막고
        실행 중인 job 이 끝나기를 ``drain_timeout_s`` 까지 기다린 뒤 commit 한다. 관문은 ``finally`` 에서 반드시
        다시 연다. commit 에 실패한 executor 는 버전이 어긋난 채로 남으므로 디스패치 후보에서 빠지고, 다음
        주기부터 :meth:`_catch_up` 이 따로 맞춘다. 공유 모드 executor 는 파일은 그대로 두고 모듈만 비우면 되므로
        버전과 무관하게 매번 대상에 넣는다.
        """
        version = self.source_version
        if manifest.version != version:
            return  # 확정 뒤 또 바뀌었다. 다음 주기에 다시 settle 을 기다린다.
        targets = [st for st in self._targets() if st.shared or st.version != version]
        started = time.monotonic()
        if not self._targets():
            self.last_error = "닿는 executor 가 없어 전환하지 못했다"
            self._retry_at = time.monotonic() + self.retry_backoff_s
            return
        if not targets:
            # 닿는 executor 가 모두 이미 이 버전이다. 옮길 것도 drain 할 것도 없다.
            self.active_version, self.last_error = version, None
            return
        try:
            data = await self._bundle(manifest)
        except Exception as exc:
            self.last_error = f"번들 생성 실패: {exc}"
            self._retry_at = time.monotonic() + self.retry_backoff_s
            logger.warning("동기화 번들 생성 실패: %s", exc)
            return

        results = await asyncio.gather(*(self._prepare(client, st, version, data) for st in targets))
        if not all(results):
            failed = [st.url for st, ok in zip(targets, results) if not ok]
            await asyncio.gather(*(self._post(client, st, "abort", version)
                                   for st, ok in zip(targets, results) if ok))
            self.last_error = f"version={version} 준비 실패로 전환하지 않음: {', '.join(failed)}"
            self._retry_at = time.monotonic() + self.retry_backoff_s
            perf.inc("sync.prepare_failures")
            logger.warning("동기화 %s — %.0f초 뒤 다시 시도", self.last_error, self.retry_backoff_s)
            return

        # 모두 준비됐다. 새 job 을 막고 실행 중인 job 이 끝나기를 기다린 뒤 한 번에 바꾼다.
        self.admission.pause(f"sync {version}")
        drained, commits, previous = True, [], self.active_version
        try:
            logger.info("동기화 version=%s 준비 완료(executor %d대) — 실행 중인 job 대기(최대 %.0f초)",
                        version, len(targets), self.drain_timeout_s)
            drained = await self.admission.wait_idle(self.drain_timeout_s)
            if not drained:
                logger.warning("동기화 drain 이 %.0f초를 넘겨 실행 중인 job 이 있는 채로 전환한다(실행 중 job 은 남은 "
                               "task 를 새 버전으로 돌린다)", self.drain_timeout_s)
            commits = await asyncio.gather(*(self._post(client, st, "commit", version) for st in targets))
            for st, ok in zip(targets, commits):
                if ok:
                    st.version, st.last_error = version, None
            self.active_version = version
        finally:
            self.admission.resume()
        failed = [st.url for st, ok in zip(targets, commits) if not ok]
        self.last_transition = {
            "from": previous, "to": version, "at": now_iso(), "executors": len(targets),
            "commit_failed": failed, "drained": drained, "elapsed_s": round(time.monotonic() - started, 2),
        }
        self.last_error = f"commit 실패한 executor 는 따라잡을 때까지 제외: {', '.join(failed)}" if failed else None
        perf.inc("sync.transitions")
        logger.info("동기화 전환 완료 %s → %s (executor %d대, 실패 %d, %.1f초)", previous, version,
                    len(targets), len(failed), time.monotonic() - started)

    async def _catch_up(self, client: httpx.AsyncClient, manifest: Manifest) -> None:
        """전환된 버전과 다른 executor 를 하나씩 맞춘다. 그 executor 는 후보에서 빠져 있으므로 drain 하지 않는다."""
        version = self.active_version
        if manifest.version != version:
            return  # 원본이 이미 다음 버전으로 가는 중이다. 전환이 끝나면 함께 맞춰진다.
        now = time.monotonic()
        lagging = [st for st in self._targets() if not st.shared and st.version != version and now >= st.next_try]
        if not lagging:
            return
        try:
            data = await self._bundle(manifest)
        except Exception as exc:
            self.last_error = f"번들 생성 실패: {exc}"
            for st in lagging:
                st.next_try = time.monotonic() + self.retry_backoff_s
            logger.warning("동기화 따라잡기 번들 생성 실패: %s", exc)
            return
        for st in lagging:
            ok = await self._prepare(client, st, version, data) and await self._post(client, st, "commit", version)
            if ok:
                st.version, st.last_error, st.next_try = version, None, 0.0
                perf.inc("sync.catch_ups")
                logger.info("executor %s 를 동기화 버전 %s 로 맞춤", st.url, version)
            else:
                st.next_try = time.monotonic() + self.retry_backoff_s


def _error_text(resp: httpx.Response) -> str:
    """executor 오류 응답을 한 줄로 줄인다. ``detail`` 이 ``{code, message}`` 이면 코드를 앞에 붙인다."""
    try:
        detail = resp.json().get("detail")
        if isinstance(detail, dict):
            return f"{resp.status_code} {detail.get('code')}: {detail.get('message')}"
        return f"{resp.status_code} {detail}"
    except Exception:
        return f"{resp.status_code} {resp.text[:200]}"


def build_syncer(settings, runner) -> Optional[BundleSyncer]:
    """원본 coordinator 이고 대상이 설정돼 있으면 동기화 관리자를 만들어 디스패처에 관문으로 붙인다."""
    if not getattr(settings, "sync_enabled", False) or not getattr(settings, "sync_push", True):
        return None
    rules = SyncRules.from_settings(settings)
    if not rules.configured:
        logger.warning("sync.enabled=true 지만 sync.paths·sync.patterns 가 비어 있어 동기화를 켜지 않음")
        return None
    admission = getattr(runner, "admission", None)
    if admission is None or not getattr(settings, "executors", None):
        return None
    syncer = BundleSyncer(settings, admission)
    runner.bundle_gate = syncer
    logger.info("파일 동기화 켜짐: %s 의 %s (패턴 %s, 제외 %s)", syncer.base, ", ".join(rules.paths),
                ", ".join(rules.patterns), ", ".join(rules.exclude) or "없음")
    return syncer
