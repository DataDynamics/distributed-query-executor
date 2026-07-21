"""기존 설치본 config.properties 의 사용자 변경분을 찾아 새 기본 설정에 적용한다.

업그레이드 시나리오를 위한 도구다. 새 버전을 배포하면 저장소의 ``config/config.properties``
(새 기본값·새 키·새 주석)가 갱신되지만, 설치 경로(``/data1/distributed-query-executor/
config``)의 설정은 운영자가 손으로 고친 값들을 담고 있어 그대로 덮어쓸 수 없다. 이 도구는
둘을 비교해 **사용자 변경분만** 뽑아 새 기본 파일 위에 얹는다:

* **사용자 변경분** = 기존 설정에서 새 기본값과 값이 다른 키 + 새 기본 파일에 없는 키
  (예: 운영자가 추가한 ``query.func.config.*``).
* 병합 결과는 **새 기본 파일을 베이스**로 한다 — 새 버전에서 추가된 키·주석·배치를 그대로
  얻고, 사용자 변경분은 제자리에서 값만 바뀌며(:func:`core.config_tui.merge_properties_lines`),
  새 파일에 없는 사용자 추가 키는 끝의 마커 아래로 붙는다.
* 기존 값이 새 기본값과 **같은** 키는 적용하지 않는다(새 파일 원문 유지). 반대로 값이
  다르면 "운영자가 명시한 값"으로 간주해 보존한다 — 새 버전에서 기본값 자체가 바뀐 키도
  기존 값이 이긴다(기본값을 따르고 싶으면 병합 후 해당 키만 지우면 된다).

사용::

    python -m core.config_migrate                  # 설치 설정을 제자리 갱신(.bak 백업)
    python -m core.config_migrate --dry-run        # 무엇이 적용될지 보고만
    python -m core.config_migrate --old /path/config.properties \
        --new config/config.properties --out /tmp/merged.properties

기본 경로: ``--old`` 는 ``$QUERY_EXECUTOR_CONFIG_DIR/config.properties``(미설정 시
``/data1/distributed-query-executor/config/config.properties``), ``--new`` 는 이 도구를
실행하는 (새 버전) 소스 트리의 ``config/config.properties``, ``--out`` 은 ``--old`` 와 동일
(제자리 업그레이드). 런처는 ``bin/migrate-config.sh``.
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

from core.config_loader import load_properties
from core.config_tui import _is_secret, merge_properties_lines

# 기본 설치 트리의 설정 디렉터리(환경변수 미설정 시).
_DEFAULT_INSTALL_CONF = Path("/data1/distributed-query-executor/config")


def _default_old_path() -> Path:
    """기존(운영) config.properties 의 기본 위치를 정한다: 환경변수 → 설치 트리."""
    env = os.getenv("QUERY_EXECUTOR_CONFIG_DIR")
    base = Path(env) if env else _DEFAULT_INSTALL_CONF
    return base / "config.properties"


def _default_new_path() -> Path:
    """새 기본 config.properties 의 기본 위치: 이 패키지가 속한 트리의 ``config/``.

    src 레이아웃이므로 ``src/core/config_migrate.py`` 기준 두 단계 위가 트리 루트다.
    업그레이드 시엔 새로 받은 (새 버전) 소스 트리에서 이 도구를 실행해 그 트리의
    ``config/config.properties`` 를 새 기본값으로 삼고, ``--old`` 로 설치 경로의 라이브
    설정을 가리킨다.
    """
    return Path(__file__).resolve().parents[2] / "config" / "config.properties"


@dataclass
class MigrationPlan:
    """비교 결과 요약. 보고 출력과 테스트가 이 구조를 공유한다."""

    changed: dict[str, str] = field(default_factory=dict)  # 새 기본값과 값이 다른 키 → 기존 값
    added: dict[str, str] = field(default_factory=dict)    # 새 파일에 없는 사용자 추가 키 → 기존 값
    same: list[str] = field(default_factory=list)          # 기존 값 == 새 기본값(적용 불필요)
    new_keys: list[str] = field(default_factory=list)      # 새 버전에서 새로 생긴 키(참고용)

    @property
    def to_apply(self) -> dict[str, str]:
        """병합 시 실제로 얹을 값들(변경 유지 + 사용자 추가)."""
        return {**self.changed, **self.added}


def build_plan(old_props: dict[str, str], new_props: dict[str, str]) -> MigrationPlan:
    """기존/새 properties 매핑을 비교해 적용 계획을 만든다(순수 함수)."""
    plan = MigrationPlan()
    for key, value in old_props.items():
        if key not in new_props:
            plan.added[key] = value
        elif new_props[key] != value:
            plan.changed[key] = value
        else:
            plan.same.append(key)
    plan.new_keys = [k for k in new_props if k not in old_props]
    return plan


def merge_files(old_path: Path, new_path: Path) -> tuple[MigrationPlan, list[str]]:
    """두 파일을 비교해 (계획, 병합된 새 파일 줄 목록)을 반환한다.

    병합 줄은 **새 파일 원문**에 사용자 변경분을 얹은 결과다(주석·순서 보존,
    없는 키는 끝의 마커 아래 추가).
    """
    old_props = load_properties(old_path)
    new_props = load_properties(new_path)
    plan = build_plan(old_props, new_props)
    new_lines = new_path.read_text(encoding="utf-8").splitlines()
    merged = merge_properties_lines(new_lines, plan.to_apply)
    return plan, merged


def _mask(key: str, value: str) -> str:
    """보고 출력용 값 — 자격증명성 키는 가린다(파일에는 원본이 기록된다)."""
    return "*****" if _is_secret(key) and value else value


def _print_report(plan: MigrationPlan, new_props: dict[str, str]) -> None:
    """무엇을 찾았고 무엇이 적용되는지 사람이 읽을 요약을 출력한다."""
    if plan.changed:
        print(f"== 변경 유지 ({len(plan.changed)}건) — 기존 값이 새 기본값과 달라 보존")
        for k, v in plan.changed.items():
            print(f"  {k} = {_mask(k, v)}   (새 기본값: {_mask(k, new_props.get(k, ''))})")
    if plan.added:
        print(f"== 사용자 추가 키 ({len(plan.added)}건) — 새 기본 파일에 없어 끝에 추가")
        for k, v in plan.added.items():
            print(f"  {k} = {_mask(k, v)}")
    if plan.new_keys:
        print(f"== 새 버전에서 추가된 키 ({len(plan.new_keys)}건) — 기본값으로 들어감")
        for k in plan.new_keys:
            print(f"  {k}")
    print(f"== 동일 ({len(plan.same)}건) — 새 기본값과 같아 적용 불필요")


def migrate(old_path: Path, new_path: Path, out_path: Path, dry_run: bool = False) -> MigrationPlan:
    """비교 → 보고 → (dry-run 이 아니면) ``out_path`` 에 병합 결과를 기록한다.

    ``out_path`` 에 기존 파일이 있으면 ``.bak`` 으로 백업한 뒤 덮어쓴다.
    """
    plan, merged = merge_files(old_path, new_path)
    _print_report(plan, load_properties(new_path))
    if dry_run:
        print("\n(dry-run) 파일을 쓰지 않았습니다.")
        return plan
    if out_path.is_file():
        backup = out_path.with_suffix(out_path.suffix + ".bak")
        backup.write_text(out_path.read_text(encoding="utf-8"), encoding="utf-8")
        print(f"\n백업: {backup}")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(merged) + "\n", encoding="utf-8")
    print(f"기록: {out_path} (변경 유지 {len(plan.changed)}건, 추가 {len(plan.added)}건)")
    return plan


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m core.config_migrate",
        description="기존 설치본 config.properties 의 사용자 변경분을 새 기본 설정에 적용한다.",
    )
    parser.add_argument("--old", type=Path, default=None,
                        help="기존(운영) config.properties (기본: $QUERY_EXECUTOR_CONFIG_DIR "
                             "또는 /data1/distributed-query-executor/config)")
    parser.add_argument("--new", type=Path, default=None,
                        help="새 기본 config.properties (기본: 트리 루트의 config/config.properties)")
    parser.add_argument("--out", type=Path, default=None,
                        help="병합 결과 기록 경로 (기본: --old 와 동일, 제자리 업그레이드)")
    parser.add_argument("--dry-run", action="store_true", help="보고만 하고 기록하지 않는다")
    args = parser.parse_args(argv)

    old_path = args.old or _default_old_path()
    new_path = args.new or _default_new_path()
    out_path = args.out or old_path

    for label, p in (("기존 설정", old_path), ("새 기본 설정", new_path)):
        if not p.is_file():
            print(f"오류: {label} 파일이 없습니다: {p}", file=sys.stderr)
            return 1
    if old_path.resolve() == new_path.resolve():
        print(f"오류: 기존 설정과 새 기본 설정이 같은 파일입니다: {old_path}", file=sys.stderr)
        return 1

    print(f"기존 설정  : {old_path}")
    print(f"새 기본 설정: {new_path}")
    print(f"기록 대상  : {out_path}{' (dry-run)' if args.dry_run else ''}\n")
    migrate(old_path, new_path, out_path, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":  # pragma: no cover - 진입점
    raise SystemExit(main())
