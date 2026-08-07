"""업그레이드할 때 운영자 자산(설정·템플릿·커스텀 쿼리 함수)을 새 버전에 맞춰 반영한다.

새 버전을 배포하면 저장소의 기본값(``config/``·``templates/``·``customs/``)이 갱신되지만,
설치 경로(``/data1/distributed-query-executor``)에는 운영자가 손으로 고친 설정·직접 추가한
템플릿·커스텀 함수가 들어 있어 그대로 덮어쓸 수 없다. 그래서 설치 스크립트의 rsync 는 이
세 디렉터리(``/config``·``/templates``·``/customs``)를 **제외**하고, 이 도구가 새 소스 트리와
설치 트리를 비교해 파일별 전략으로 반영한다.

## 대상 트리와 파일별 전략

* **config/** — ``config.properties`` 는 **properties 병합**(운영자 변경분만 새 기본 파일 위에
  얹음), 그 외(``config.yml``·``*.sql``)는 **새 버전으로 교체 + ``.bak`` 백업**. config.yml 은
  ``${변수:기본값}`` 구조 파일이라 운영자 값은 properties 에 있고, **새 버전이 추가한 설정의
  YAML 구조**를 반영하려면 교체가 맞다(이게 안 돼서 새 설정이 먹지 않던 문제를 해결).
* **templates/** — 새 버전 예제 템플릿을 반영(바뀐 파일은 ``.bak`` 백업), **운영자가 추가한
  사이트 템플릿은 보존**(삭제하지 않음).
* **customs/**(커스텀 쿼리 함수) — 예제(``trino_runner`` 등)를 반영, **운영자 모듈은 보존**.

공통 원칙은 이렇다. 소스에만 있는 파일은 새로 복사하고, 양쪽에 있으면서 다르면 교체하거나
병합한 뒤 ``.bak`` 을 남기며, 같으면 그대로 둔다. **설치 트리에만 있는 파일은 운영자 자산이므로
절대 삭제하지 않고 보존한다.** 순회할 때 ``__pycache__``·``*.pyc``·``*.bak`` 은 건너뛴다.

## properties 병합 규칙(config.properties)

* **사용자 변경분** = 기존 설정에서 새 기본값과 값이 다른 키 + 새 기본 파일에 없는 키
  (예: 운영자가 추가한 ``query.func.config.*``). 새 기본 파일을 베이스로 값만 바꾸고
  (:func:`core.config_tui.merge_properties_lines`), 새 파일에 없는 키는 끝의 마커 아래로 붙는다.
* 새 기본값과 **같은** 키는 손대지 않고, 다르면 "운영자가 명시한 값"으로 간주해 보존한다.

사용::

    python -m core.config_migrate                  # config+templates+customs 전체를 제자리 갱신
    python -m core.config_migrate --dry-run        # 무엇이 적용될지 보고만(.bak/교체 없음)
    # (하위 호환) config.properties 한 파일만 병합:
    python -m core.config_migrate --old /path/config.properties \
        --new config/config.properties --out /tmp/merged.properties

기본 경로: 설치 트리는 ``$QUERY_EXECUTOR_CONFIG_DIR`` 의 부모(미설정 시
``/data1/distributed-query-executor``), 소스 트리는 이 도구가 속한 (새 버전) 트리 루트.
``--deploy-base``/``--source-base`` 로 명시 지정 가능. 런처는 ``bin/migrate-config.sh``.
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path

from core.config_loader import load_properties
from core.config_tui import _is_secret, merge_properties_lines

# 환경변수가 없을 때 쓸 기본 설치 트리의 설정 디렉터리와 그 부모인 트리 루트다.
_DEFAULT_INSTALL_CONF = Path("/data1/distributed-query-executor/config")

# 트리 순회 시 건너뛸 디렉터리/확장자(파이썬 캐시·컴파일·백업).
_SKIP_DIR_PARTS = {"__pycache__"}
_SKIP_SUFFIXES = {".pyc", ".pyo", ".bak"}
# 멀티 타깃 모드에서 반영할 목록이며 (트리 이름, properties 병합 대상 파일명) 쌍으로 적는다.
_TREES = (
    ("config", frozenset({"config.properties"})),
    ("templates", frozenset()),
    ("customs", frozenset()),
)


def _default_old_path() -> Path:
    """기존 운영 config.properties 의 기본 위치를 정한다. 환경변수를 먼저 보고 없으면 설치 트리를 쓴다."""
    env = os.getenv("QUERY_EXECUTOR_CONFIG_DIR")
    base = Path(env) if env else _DEFAULT_INSTALL_CONF
    return base / "config.properties"


def _default_new_path() -> Path:
    """새 기본 config.properties 의 위치를 정한다. 이 패키지가 속한 트리의 ``config/`` 를 쓴다.

    src 레이아웃이므로 ``src/core/config_migrate.py`` 기준 두 단계 위가 트리 루트다.
    업그레이드 시엔 새로 받은 (새 버전) 소스 트리에서 이 도구를 실행해 그 트리의
    ``config/config.properties`` 를 새 기본값으로 삼고, ``--old`` 로 설치 경로의 라이브
    설정을 가리킨다.
    """
    return Path(__file__).resolve().parents[2] / "config" / "config.properties"


@dataclass
class MigrationPlan:
    """비교 결과 요약. 보고 출력과 테스트가 이 구조를 공유한다."""

    changed: dict[str, str] = field(default_factory=dict)  # 새 기본값과 값이 다른 키를 기존 값과 함께 담는다
    added: dict[str, str] = field(default_factory=dict)    # 새 파일에 없는 사용자 추가 키를 기존 값과 함께 담는다
    same: list[str] = field(default_factory=list)          # 기존 값이 새 기본값과 같아 적용할 필요가 없는 키다
    new_keys: list[str] = field(default_factory=list)      # 새 버전에서 새로 생긴 키이며 참고용이다

    @property
    def to_apply(self) -> dict[str, str]:
        """병합할 때 실제로 얹을 값들이다. 운영자가 바꾼 값과 직접 추가한 키를 합친다."""
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
    """두 파일을 비교해 보고하고, dry-run 이 아니면 ``out_path`` 에 병합 결과를 기록한다.

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


# ── 디렉터리 트리 마이그레이션(config·templates·customs 공통) ────────────────

def _iter_rel_files(root: Path) -> set[str]:
    """``root`` 아래 모든 파일의 상대 경로를 문자열 집합으로 모은다. 캐시와 컴파일 산출물, 백업은 뺀다."""
    if not root.is_dir():
        return set()
    out: set[str] = set()
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        rel = p.relative_to(root)
        if any(part in _SKIP_DIR_PARTS for part in rel.parts):
            continue
        if p.suffix in _SKIP_SUFFIXES:
            continue
        out.add(str(rel))
    return out


def _backup_file(path: Path) -> Path:
    """``path`` 를 ``path.bak`` 으로 복사(백업)하고 백업 경로를 돌려준다."""
    backup = path.with_suffix(path.suffix + ".bak")
    backup.write_bytes(path.read_bytes())
    return backup


def _copy_file(src: Path, dst: Path) -> None:
    """``src`` 를 ``dst`` 로 복사한다(상위 디렉터리 생성, 메타데이터 유지)."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


@dataclass
class TreeReport:
    """디렉터리 트리 하나의 마이그레이션 결과 요약이다."""

    label: str
    new: list[str] = field(default_factory=list)          # 소스에만 있어 새로 복사한 파일이다
    updated: list[str] = field(default_factory=list)      # 내용이 달라 새 버전으로 교체한 파일이다(.bak 을 남긴다)
    merged: list[str] = field(default_factory=list)       # properties 를 병합한 파일이다(운영자 변경분을 보존하고 .bak 을 남긴다)
    unchanged: list[str] = field(default_factory=list)    # 양쪽 동일 — 손대지 않음
    operator_only: list[str] = field(default_factory=list)  # 설치 트리에만 있어 그대로 보존한 파일이다
    plans: dict = field(default_factory=dict)             # properties 파일의 상대경로별 MigrationPlan 이다

    @property
    def change_count(self) -> int:
        return len(self.new) + len(self.updated) + len(self.merged)


def migrate_tree(
    new_dir: Path, old_dir: Path, *, merge_props: frozenset[str] = frozenset(), dry_run: bool = False
) -> TreeReport:
    """새 소스 트리(``new_dir``)를 설치 트리(``old_dir``)에 반영한다. 재조정만 하고 삭제는 하지 않는다.

    파일별 전략:
      - 설치 트리에 없으면 **새로 복사**한다.
      - 파일명이 ``merge_props`` 에 있으면(config.properties) **properties 를 병합**한다(운영자 변경분을
        보존, 결과가 기존과 다르면 ``.bak`` 백업 후 기록).
      - 그 밖에 내용이 다르면 **새 버전으로 교체**하고 ``.bak`` 을 남긴다.
      - 내용이 같으면 **그대로** 둔다.
    설치 트리에만 있는 파일(운영자 자산)은 ``operator_only`` 로 보고만 하고 **보존한다**.
    ``dry_run`` 이면 파일을 쓰지 않고 분류만 한다.
    """
    report = TreeReport(label=old_dir.name)
    if not new_dir.is_dir():
        return report  # 소스에 없는 트리는 건너뛴다(예를 들어 이 버전에 customs 가 없을 수 있다)
    new_files = _iter_rel_files(new_dir)
    old_files = _iter_rel_files(old_dir)
    for rel in sorted(new_files):
        src = new_dir / rel
        dst = old_dir / rel
        if not dst.exists():
            if not dry_run:
                _copy_file(src, dst)
            report.new.append(rel)
        elif Path(rel).name in merge_props:
            # properties 를 병합한다. old 는 설치 트리(운영자 값)이고 new 는 소스 트리(기본값)다.
            plan, merged = merge_files(dst, src)
            merged_text = "\n".join(merged) + "\n"
            if merged_text == dst.read_text(encoding="utf-8"):
                report.unchanged.append(rel)
            else:
                if not dry_run:
                    _backup_file(dst)
                    dst.write_text(merged_text, encoding="utf-8")
                # 운영자 변경분이 있으면 병합으로 보고, 없으면 단순히 새 버전을 반영한 것으로 본다.
                (report.merged if plan.to_apply else report.updated).append(rel)
                report.plans[rel] = plan
        elif src.read_bytes() == dst.read_bytes():
            report.unchanged.append(rel)
        else:
            if not dry_run:
                _backup_file(dst)
                _copy_file(src, dst)
            report.updated.append(rel)
    report.operator_only = sorted(old_files - new_files)
    return report


def migrate_all(
    deploy_base: Path, source_base: Path, *, dry_run: bool = False
) -> list[tuple[Path, Path, TreeReport]]:
    """설치 트리 루트와 소스 트리 루트를 받아 config/templates/customs 를 모두 반영한다."""
    results = []
    for name, merge_props in _TREES:
        new_dir = source_base / name
        old_dir = deploy_base / name
        report = migrate_tree(new_dir, old_dir, merge_props=merge_props, dry_run=dry_run)
        results.append((new_dir, old_dir, report))
    return results


def _print_tree_report(new_dir: Path, old_dir: Path, report: TreeReport) -> None:
    """트리 하나의 반영 결과를 사람이 읽을 요약으로 출력한다."""
    print(f"[{report.label}]  {old_dir}  ← {new_dir}")
    if not new_dir.is_dir():
        print("  (소스에 해당 디렉터리 없음 — 건너뜀)\n")
        return
    print(
        f"  신규 {len(report.new)} / 갱신 {len(report.updated)} / 병합 {len(report.merged)}"
        f" / 동일 {len(report.unchanged)} / 운영자 보존 {len(report.operator_only)}"
    )
    for rel in report.new:
        print(f"    + {rel}  (신규)")
    for rel in report.updated:
        print(f"    ~ {rel}  (새 버전으로 갱신, .bak 백업)")
    for rel in report.merged:
        plan = report.plans.get(rel)
        detail = f" — 변경유지 {len(plan.changed)}·추가 {len(plan.added)}건" if plan else ""
        print(f"    ⋈ {rel}  (properties 병합{detail}, .bak 백업)")
        if plan:
            for k, v in {**plan.changed, **plan.added}.items():
                print(f"        {k} = {_mask(k, v)}")
    for rel in report.operator_only:
        print(f"    = {rel}  (운영자 파일 — 보존)")
    print()


def _default_deploy_base() -> Path:
    """설치 트리 루트를 정한다. ``$QUERY_EXECUTOR_CONFIG_DIR`` 의 부모를 쓰고, 미설정이면 기본 설치 트리를 쓴다."""
    env = os.getenv("QUERY_EXECUTOR_CONFIG_DIR")
    config_dir = Path(env) if env else _DEFAULT_INSTALL_CONF
    return config_dir.parent


def _default_source_base() -> Path:
    """소스 트리 루트를 정한다. 이 도구가 속한 트리이며 src/core/config_migrate.py 에서 두 단계 위다."""
    return Path(__file__).resolve().parents[2]


def _run_single_file(old_path: Path, new_path: Path, out_path: Path, dry_run: bool) -> int:
    """하위 호환을 위해 config.properties 한 파일만 병합하는 레거시 경로다(--old·--new·--out)."""
    for label, p in (("기존 설정", old_path), ("새 기본 설정", new_path)):
        if not p.is_file():
            print(f"오류: {label} 파일이 없습니다: {p}", file=sys.stderr)
            return 1
    if old_path.resolve() == new_path.resolve():
        print(f"오류: 기존 설정과 새 기본 설정이 같은 파일입니다: {old_path}", file=sys.stderr)
        return 1
    print(f"기존 설정  : {old_path}")
    print(f"새 기본 설정: {new_path}")
    print(f"기록 대상  : {out_path}{' (dry-run)' if dry_run else ''}\n")
    migrate(old_path, new_path, out_path, dry_run=dry_run)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m core.config_migrate",
        description="업그레이드 시 설정·템플릿·커스텀 함수를 새 버전으로 반영한다"
                    "(운영자 변경분·추가 파일 보존).",
    )
    # 기본값인 멀티 타깃 모드에서는 트리 루트를 지정한다.
    parser.add_argument("--deploy-base", type=Path, default=None,
                        help="설치 트리 루트(기본: $QUERY_EXECUTOR_CONFIG_DIR 의 부모 "
                             "또는 /data1/distributed-query-executor)")
    parser.add_argument("--source-base", type=Path, default=None,
                        help="새 버전 소스 트리 루트(기본: 이 도구가 속한 트리 루트)")
    # 하위 호환용 레거시 단일 파일 모드에서는 config.properties 만 병합한다.
    parser.add_argument("--old", type=Path, default=None,
                        help="[단일 파일 모드] 기존(운영) config.properties")
    parser.add_argument("--new", type=Path, default=None,
                        help="[단일 파일 모드] 새 기본 config.properties")
    parser.add_argument("--out", type=Path, default=None,
                        help="[단일 파일 모드] 병합 결과 기록 경로(기본: --old 와 동일)")
    parser.add_argument("--dry-run", action="store_true", help="보고만 하고 기록하지 않는다")
    args = parser.parse_args(argv)

    # --old·--new·--out 중 하나라도 주어지면 레거시 단일 파일 모드로 동작한다.
    if args.old or args.new or args.out:
        old_path = args.old or _default_old_path()
        new_path = args.new or _default_new_path()
        out_path = args.out or old_path
        return _run_single_file(old_path, new_path, out_path, args.dry_run)

    # 기본 동작은 config 와 templates, customs 를 모두 반영하는 것이다.
    deploy_base = args.deploy_base or _default_deploy_base()
    source_base = args.source_base or _default_source_base()
    if deploy_base.resolve() == source_base.resolve():
        print(f"오류: 설치 트리와 소스 트리가 같습니다: {deploy_base}\n"
              f"(새로 받은 소스 트리에서 실행하고 --deploy-base 로 설치 경로를 가리키세요)",
              file=sys.stderr)
        return 1
    print(f"설치 트리 : {deploy_base}")
    print(f"소스 트리 : {source_base}")
    print(f"모드      : {'dry-run(보고만)' if args.dry_run else '적용(.bak 백업)'}\n")
    total = 0
    for new_dir, old_dir, report in migrate_all(deploy_base, source_base, dry_run=args.dry_run):
        _print_tree_report(new_dir, old_dir, report)
        total += report.change_count
    if args.dry_run:
        print(f"(dry-run) 반영 예정 {total}건 — 파일을 쓰지 않았습니다.")
    else:
        print(f"완료: 총 {total}개 파일 신규/갱신/병합(.bak 백업). 서비스 재기동 시 적용됩니다.")
    return 0


if __name__ == "__main__":  # pragma: no cover - 스크립트 진입점이다
    raise SystemExit(main())
