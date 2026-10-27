"""coordinator 가 보낸 동기화 번들을 받아 executor 의 지정 파일을 한 번에 바꾸는 저장소다.

동기화 대상 경로(``sync.paths``, 예: ``customs``)는 처음 전환할 때 실제 디렉터리에서 심링크로 바뀐다.

    <앱 홈>/customs -> .sync/current/customs
    <앱 홈>/.sync/current -> releases/<버전>
    <앱 홈>/.sync/releases/<버전>/customs/...

그 뒤로 새 버전을 적용하는 일은 ``current`` 심링크 하나를 ``os.replace`` 로 바꾸는 것뿐이다. rename 은
원자적이라 어떤 순간에도 반쯤 바뀐 트리가 보이지 않고, 동기화 경로가 여러 개여도 모두 같은 순간에 바뀐다.
``PYTHONPATH`` 는 그대로 앱 홈을 가리키므로 런처나 systemd 유닛을 고칠 필요가 없다.

coordinator 와 2단계로 맞춘다. prepare 는 번들을 ``releases/<버전>`` 에 풀고 검증까지 마칠 뿐 현재 버전을
건드리지 않는다. 모든 executor 가 준비를 마쳐야 coordinator 가 commit 을 보내고, 하나라도 실패하면 abort 로
준비한 것을 지운다. 그래서 일부만 바뀌는 일이 생기지 않는다.

commit 할 때는 ``current`` 를 바꾼 뒤 동기화 경로에 속한 모듈을 ``sys.modules`` 에서 빼고 커스텀 함수
캐시를 비운다. ``importlib.reload`` 는 쓰지 않는다. reload 는 같은 모듈 객체를 다시 실행해 globals 를 덮어쓰므로,
실행 중인 함수가 도중에 바뀐 globals 를 보게 된다. 모듈을 빼고 다시 import 하면 새 모듈 객체가 생기고,
실행 중인 task 는 옛 함수와 옛 globals 를 쥔 채 끝까지 간다. 옛 release 디렉터리도 ``sync.keep_releases``
만큼 남기므로 실행 중인 코드가 파일을 경로로 읽어도 사라지지 않는다.

coordinator 와 같은 호스트에서 같은 앱 홈을 쓰는 executor(개발 환경, 한 서버에 함께 띄운 경우)는 동기화
경로가 곧 coordinator 의 원본이다. 이때 심링크로 바꾸면 원본을 망가뜨리므로 파일은 건드리지 않고 모듈
캐시만 비운다(공유 모드).
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import uuid
from typing import Dict, List, Optional

from core import bundle as bundle_mod
from core.bundle import BundleError, SyncRules
from core.timeutil import now_iso

logger = logging.getLogger(__name__)

_VERSION_RE = re.compile(r"^[0-9a-f]{%d}$" % bundle_mod.VERSION_LEN)

# 커스텀 함수 import 와 모듈 교체가 겹치지 않게 하는 락이다. 커스텀 함수를 불러오는 곳(executor 의
# query.func.module·fetch_module 로더)이 이 락을 잡고 import 한다.
IMPORT_LOCK = threading.RLock()
# commit 때 비울 캐시들이다. 커스텀 함수를 캐시하는 모듈이 비우는 함수를 등록한다.
_cache_clearers: List = []


def register_cache_clearer(fn) -> None:
    if fn not in _cache_clearers:
        _cache_clearers.append(fn)


def app_home() -> str:
    """앱 홈(src/ 의 상위)이다. 배포에서는 /data1/distributed-query-executor 다."""
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(bundle_mod.__file__))))


def purge_modules(prefixes: List[str]) -> List[str]:
    """동기화 경로에 속한 모듈을 sys.modules 에서 빼고 import 캐시와 커스텀 함수 캐시를 비운다."""
    import importlib

    removed = []
    for name in list(sys.modules):
        if any(name == p or name.startswith(p + ".") for p in prefixes):
            del sys.modules[name]
            removed.append(name)
    importlib.invalidate_caches()
    for clear in list(_cache_clearers):
        try:
            clear()
        except Exception:
            logger.warning("커스텀 함수 캐시를 비우지 못함", exc_info=True)
    return removed


class SyncDisabled(RuntimeError):
    pass


class BundleStore:
    """executor 쪽 동기화 상태와 release 디렉터리를 관리한다. 메서드는 모두 블로킹이라 스레드에서 부른다."""

    def __init__(self, settings, base: Optional[str] = None):
        """설정을 읽고 ``<앱 홈>/.sync`` 아래 경로를 정한다. 디렉터리는 처음 prepare 할 때 만든다.

        ``base`` 는 coordinator 와 같은 규칙(``sync.base_dir`` → 설치 위치)으로 정하고 realpath 로 고정한다.
        공유 모드 판정이 coordinator 가 보낸 경로와 문자열로 비교하기 때문이다. 직전 commit 시각은
        ``state.json`` 에서 되살려 재기동 뒤에도 상태 화면에 남게 한다.
        """
        self.enabled = bool(getattr(settings, "sync_enabled", False))
        self.rules = SyncRules.from_settings(settings)
        self.base = os.path.realpath(base or getattr(settings, "sync_base_dir", "") or app_home())
        self.state_dir = os.path.join(self.base, ".sync")
        self.releases_dir = os.path.join(self.state_dir, "releases")
        self.current_link = os.path.join(self.state_dir, "current")
        self.keep = max(1, int(getattr(settings, "sync_keep_releases", 3) or 3))
        self.max_bytes = int(float(getattr(settings, "sync_max_bundle_mb", 100) or 100) * 1048576)
        self.verify_import = bool(getattr(settings, "sync_verify_import", True))
        self.verify_timeout_s = float(getattr(settings, "sync_verify_timeout_s", 60) or 60)
        self.verify_modules = self._verify_targets(settings)
        self.host = bundle_mod.host_id()
        self.shared = False
        self.last_error: Optional[str] = None
        self.committed_at: Optional[str] = None
        self._staged: Dict[str, str] = {}
        self._lock = threading.Lock()  # prepare·commit·abort 를 한 번에 하나만
        self._cache = bundle_mod._HashCache()
        self._version: Optional[str] = None
        self._load_state()

    # ───────── 조회 ─────────

    def _verify_targets(self, settings) -> List[str]:
        """commit 전에 import 해 볼 대상이다. 지정이 없으면 동기화 경로 안에 있는 커스텀 함수 설정을 쓴다."""
        explicit = [x for x in getattr(settings, "sync_verify_modules", []) or [] if x]
        if explicit:
            return explicit
        prefixes = bundle_mod.module_prefixes(self.rules.paths)
        out = []
        for dotted in (getattr(settings, "query_func_module", ""), getattr(settings, "query_func_fetch_module", "")):
            dotted = (dotted or "").strip()
            mod = dotted.partition(":")[0] if ":" in dotted else dotted.rpartition(".")[0]
            if mod and any(mod == p or mod.startswith(p + ".") for p in prefixes):
                out.append(dotted)
        return out

    def version(self, refresh: bool = False) -> str:
        """현재 트리의 동기화 버전이다. refresh 면 다시 훑는다(해시는 바뀐 파일만 다시 계산한다)."""
        if refresh or self._version is None:
            self._version = bundle_mod.scan(self.base, self.rules, self._cache).version
        return self._version

    def status(self) -> dict:
        """``GET /sync/status`` 응답이다. coordinator 는 이 값으로 버전 일치와 공유 모드를 판정한다.

        ``version`` 은 저장해 둔 값이 아니라 지금 트리를 훑어 계산한다. 운영자가 동기화 경로의 파일을 손으로
        고쳤다면 버전이 달라져 coordinator 가 이 executor 를 후보에서 빼고 다시 맞추게 하려는 것이다.
        """
        return {
            "enabled": self.enabled,
            "configured": self.rules.configured,
            "version": self.version(refresh=True) if self.rules.configured else None,
            "paths": list(self.rules.paths),
            "shared": self.shared,
            "staged": sorted(self._staged),
            "releases": self._releases(),
            "current_release": self._current_release(),
            "committed_at": self.committed_at,
            "last_error": self.last_error,
            "host_id": self.host,
            "base": self.base,
        }

    def _releases(self) -> List[str]:
        try:
            return sorted(d for d in os.listdir(self.releases_dir) if ".partial" not in d)
        except OSError:
            return []

    def _current_release(self) -> Optional[str]:
        try:
            return os.path.basename(os.readlink(self.current_link))
        except OSError:
            return None

    def _load_state(self) -> None:
        """직전 commit 기록을 읽는다. 파일이 없거나 깨졌으면 처음 기동한 것으로 본다."""
        try:
            with open(os.path.join(self.state_dir, "state.json"), encoding="utf-8") as fp:
                state = json.load(fp)
            self.committed_at = state.get("committed_at")
        except (OSError, ValueError):
            pass

    def _save_state(self, version: str) -> None:
        """commit 기록을 임시 파일에 쓰고 rename 해 반쯤 쓰인 state.json 이 남지 않게 한다."""
        path = os.path.join(self.state_dir, "state.json")
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fp:
            json.dump({"version": version, "committed_at": self.committed_at}, fp)
        os.replace(tmp, path)

    def _require(self, version: Optional[str] = None) -> None:
        """요청을 받기 전 공통 확인이다. 동기화가 꺼졌으면 :class:`SyncDisabled`, 버전 모양이 틀리면 BundleError 다."""
        if version is not None and not _VERSION_RE.match(version):
            # 버전은 release 디렉터리 이름이 되므로 해시 모양이 아니면 받지 않는다(경로 조작 방지).
            raise BundleError(f"잘못된 버전 값이다: {version!r}")
        if not self.enabled:
            raise SyncDisabled("이 executor 는 동기화를 받지 않는다(sync.enabled=false)")
        if not self.rules.configured:
            raise SyncDisabled("sync.paths 와 sync.patterns 가 설정되지 않았다")

    # ───────── prepare / commit / abort ─────────

    def prepare(self, data: bytes, version: str, source_host: str = "", source_base: str = "") -> dict:
        """번들을 release 디렉터리에 풀고 검증한다. 현재 버전은 바꾸지 않는다."""
        self._require(version)
        with self._lock:
            if source_host and source_host == self.host and os.path.realpath(source_base or "/") == self.base:
                # coordinator 원본과 같은 디렉터리다. 파일은 이미 같으므로 commit 때 캐시만 비운다.
                self.shared = True
                self._staged[version] = ""
                return {"prepared": True, "shared": True, "version": version}
            self.shared = False
            release = os.path.join(self.releases_dir, version)
            if os.path.isdir(release):
                # 예전에 받은 버전(되돌리기 등)이면 다시 받지 않고 내용만 확인한다.
                if self._release_version(release) == version:
                    self._staged[version] = release
                    return {"prepared": True, "reused": True, "version": version}
                shutil.rmtree(release, ignore_errors=True)
            os.makedirs(self.releases_dir, exist_ok=True)
            partial = f"{release}.partial-{uuid.uuid4().hex[:8]}"
            os.makedirs(partial)
            try:
                got = bundle_mod.extract_bundle(data, partial, self.rules, max_bytes=self.max_bytes)
                if got != version:
                    raise BundleError(f"번들 내용의 버전({got})이 알린 버전({version})과 다르다")
                for p in self.rules.paths:  # 파일이 하나도 없는 경로도 디렉터리는 있어야 import 가 깨지지 않는다
                    if not os.path.exists(os.path.join(partial, p)) and not p.endswith(".py"):
                        os.makedirs(os.path.join(partial, p), exist_ok=True)
                self._verify_imports(partial)
                os.rename(partial, release)
            except Exception as exc:
                shutil.rmtree(partial, ignore_errors=True)
                self.last_error = f"prepare {version}: {exc}"
                raise
            self._staged[version] = release
            logger.info("동기화 번들 준비 완료 version=%s (%s)", version, release)
            return {"prepared": True, "version": version}

    def _release_version(self, release: str) -> str:
        return bundle_mod.scan(release, self.rules).version

    def _verify_imports(self, root: str) -> None:
        """별도 파이썬 프로세스에서 새 release 를 앞세워 커스텀 함수를 import 해 본다.

        서비스 프로세스의 모듈은 건드리지 않는다. 문법 오류뿐 아니라 빠진 의존성이나 import 시점 오류를
        commit 전에 잡는다.
        """
        if not self.verify_import or not self.verify_modules:
            return
        src_dir = os.path.dirname(os.path.dirname(os.path.abspath(bundle_mod.__file__)))
        code = (
            "import importlib, sys\n"
            "for d in sys.argv[1:]:\n"
            "    mod, sep, attr = d.partition(':')\n"
            "    if not sep:\n"
            "        mod, _, attr = d.rpartition('.')\n"
            "    m = importlib.import_module(mod)\n"
            "    if attr and not callable(getattr(m, attr)):\n"
            "        raise SystemExit(d + ' 는 호출 가능하지 않다')\n"
        )
        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join([root, src_dir, self.base, env.get("PYTHONPATH", "")]).rstrip(os.pathsep)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        try:
            proc = subprocess.run(
                [sys.executable, "-c", code, *self.verify_modules], env=env, cwd=root,
                capture_output=True, text=True, timeout=self.verify_timeout_s,
            )
        except subprocess.TimeoutExpired as exc:
            raise BundleError(f"import 검증이 {self.verify_timeout_s:.0f}초 안에 끝나지 않았다") from exc
        if proc.returncode != 0:
            tail = (proc.stderr or proc.stdout or "").strip().splitlines()[-3:]
            raise BundleError(f"import 검증 실패({', '.join(self.verify_modules)}): {' | '.join(tail)}")

    def commit(self, version: str) -> dict:
        """준비한 버전으로 current 를 원자적으로 바꾸고 모듈 캐시를 비운다."""
        self._require(version)
        with self._lock:
            if version not in self._staged:
                raise BundleError(f"준비하지 않은 버전이다: {version}")
            release = self._staged.pop(version)
            with IMPORT_LOCK:
                if release:  # 공유 모드면 파일은 이미 원본 그대로다
                    tmp = f"{self.current_link}.tmp-{uuid.uuid4().hex[:8]}"
                    os.symlink(os.path.join("releases", version), tmp)
                    os.replace(tmp, self.current_link)
                    self._ensure_links()
                removed = purge_modules(bundle_mod.module_prefixes(self.rules.paths))
            self.committed_at = now_iso()
            self.last_error = None
            self._version = None
            if release:
                self._save_state(version)
                self._prune()
            logger.info("동기화 버전 전환 version=%s (모듈 %d개 다시 불러옴%s)", version, len(removed),
                        ", 공유 모드" if not release else "")
            return {"committed": True, "version": version, "shared": not release}

    def abort(self, version: str) -> dict:
        """준비만 하고 쓰지 않을 버전을 지운다. 현재 버전이 가리키는 release 는 지우지 않는다.

        같은 내용으로 다시 준비해 기존 release 를 재사용한 경우에도 abort 가 올 수 있으므로, 지우기 전에
        그 디렉터리가 지금 ``current`` 가 아닌지 확인한다.
        """
        self._require(version)
        with self._lock:
            release = self._staged.pop(version, None)
            current = self._current_release()
            if release and os.path.basename(release) != current and os.path.isdir(release):
                shutil.rmtree(release, ignore_errors=True)
            return {"aborted": True, "version": version}

    def _ensure_links(self) -> None:
        """동기화 경로를 ``.sync/current/<경로>`` 를 가리키는 심링크로 맞춘다(처음 한 번은 실제 디렉터리를 옮긴다)."""
        for p in self.rules.paths:
            link = os.path.join(self.base, p)
            target = os.path.relpath(os.path.join(self.current_link, p), os.path.dirname(link))
            if os.path.islink(link):
                if os.readlink(link) == target:
                    continue
            elif os.path.exists(link):
                # 처음 전환하는 실제 디렉터리·파일은 지우지 않고 옮겨 둔다(패턴에 없던 로컬 파일 보존).
                backup = os.path.join(self.state_dir, "displaced", time.strftime("%Y%m%d-%H%M%S"), p)
                os.makedirs(os.path.dirname(backup), exist_ok=True)
                os.rename(link, backup)
                logger.warning("동기화 경로 %s 를 심링크로 바꾸며 원래 내용을 %s 로 옮김", link, backup)
            os.makedirs(os.path.dirname(link), exist_ok=True)
            tmp = f"{link}.sync-tmp-{uuid.uuid4().hex[:8]}"
            os.symlink(target, tmp)
            os.replace(tmp, link)

    def _prune(self) -> None:
        """현재 버전과 준비 중인 버전을 빼고, 최근 것부터 keep 개까지만 남긴다."""
        current = self._current_release()
        keep_set = {current, *self._staged}
        items = []
        for name in self._releases():
            path = os.path.join(self.releases_dir, name)
            try:
                items.append((os.stat(path).st_mtime, name))
            except OSError:
                continue
        items.sort(reverse=True)
        kept = 0
        for _, name in items:
            if name in keep_set:
                continue
            kept += 1
            if kept >= self.keep:  # current 를 포함해 keep 개
                shutil.rmtree(os.path.join(self.releases_dir, name), ignore_errors=True)

