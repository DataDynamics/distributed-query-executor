#!/usr/bin/env bash
# 배포 사전 점검: OS 패키지(rpm) + 파이썬 휠(.venv) 설치 여부를 "확인만" 한다.
# 아무것도 설치하지 않으며, 무엇이 빠졌는지만 보고한다.
#
# 점검 대상
#   1) OS 패키지 — README 의 빌드/런타임 의존성(rpm -q 로 확인):
#        gcc gcc-c++ make python3-devel python3 python3-pip
#        krb5-workstation krb5-devel cyrus-sasl-devel cyrus-sasl-gssapi
#   2) 파이썬 휠 — packaging/wheels/<그룹>/ 의 .whl/.tar.gz 파일이 .venv 에
#      (이름+버전 일치로) 설치되어 있는지 확인.
#
# 사용법
#   check-prereqs.sh                  # OS 패키지 + coordinator,executor 휠
#   check-prereqs.sh coordinator      # 휠은 coordinator 그룹만
#   check-prereqs.sh coordinator executor dev
#   OS_ONLY=1   check-prereqs.sh      # OS 패키지만
#   WHEELS_ONLY=1 check-prereqs.sh    # 휠만
# 환경변수
#   VENV_PY      점검할 파이썬(기본: <루트>/.venv/bin/python)
#   WHEELS_ROOT  휠 묶음 루트(기본: <루트>/packaging/wheels)
# 종료코드: 모두 충족 0, 하나라도 누락/오류 1.
set -uo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# deploy/bin/ → 루트는 두 단계 위(개발: 저장소 루트, 배포: /data1/query-executor).
ROOT="$(cd "$DIR/../.." && pwd)"
VENV_PY="${VENV_PY:-$ROOT/.venv/bin/python}"
WHEELS_ROOT="${WHEELS_ROOT:-$ROOT/packaging/wheels}"

# README 가 명시한 OS 패키지 목록(빌드 툴체인 + 파이썬 + Kerberos/SASL).
OS_PACKAGES=(
    gcc gcc-c++ make python3-devel python3 python3-pip
    krb5-workstation krb5-devel cyrus-sasl-devel cyrus-sasl-gssapi
)

# 점검할 휠 그룹(인자로 주면 그 그룹만, 없으면 런타임 기본값).
WHEEL_GROUPS=("$@")
[[ ${#WHEEL_GROUPS[@]} -eq 0 ]] && WHEEL_GROUPS=(coordinator executor)

missing=0   # 누락 누계(종료코드 결정용)

# ── PEP 503 이름 정규화: 소문자 + [-_.] 연속을 '-' 하나로 ─────────────────
normalize() {
    echo "$1" | tr '[:upper:]' '[:lower:]' | sed -E 's/[-_.]+/-/g'
}

# ── 1) OS 패키지 점검 ────────────────────────────────────────────────────
check_os() {
    echo "== OS 패키지(rpm) =="
    if ! command -v rpm >/dev/null 2>&1; then
        echo "  ! rpm 명령을 찾을 수 없음(RHEL/CentOS 계열이 아님?) — OS 점검 생략"
        missing=$((missing + 1))
        return
    fi
    for pkg in "${OS_PACKAGES[@]}"; do
        if rpm -q "$pkg" >/dev/null 2>&1; then
            printf "  [OK]      %s (%s)\n" "$pkg" "$(rpm -q "$pkg" 2>/dev/null | head -1)"
        else
            printf "  [MISSING] %s\n" "$pkg"
            missing=$((missing + 1))
        fi
    done
}

# ── 2) 파이썬 휠 점검 ────────────────────────────────────────────────────
# .venv 에 설치된 패키지 목록을 "정규화이름 -> 버전" 으로 읽어 둔다.
declare -A INSTALLED
load_installed() {
    if [[ ! -x "$VENV_PY" ]]; then
        return 1
    fi
    local line name ver
    while IFS= read -r line; do
        # pip freeze 형식: name==version (URL/editable 설치는 무시)
        [[ "$line" == *"=="* ]] || continue
        name="$(normalize "${line%%==*}")"
        ver="${line##*==}"
        INSTALLED["$name"]="$ver"
    done < <("$VENV_PY" -m pip list --format=freeze 2>/dev/null)
    return 0
}

# 휠/슬랫지 파일명에서 (이름, 버전) 추출.
#   foo_bar-1.2.3-cp39-...whl   → foo_bar 1.2.3
#   gssapi-1.11.1.tar.gz        → gssapi 1.11.1
parse_filename() {
    local base="$1" stem
    if [[ "$base" == *.whl ]]; then
        stem="${base%.whl}"
        # whl 은 '-' 로 구분: 1=이름 2=버전 (이름 내부 '_' 는 정규화로 흡수)
        WHEEL_NAME="${stem%%-*}"
        stem="${stem#*-}"
        WHEEL_VER="${stem%%-*}"
    elif [[ "$base" == *.tar.gz ]]; then
        stem="${base%.tar.gz}"
        WHEEL_NAME="${stem%-*}"   # 마지막 '-' 이전 = 이름
        WHEEL_VER="${stem##*-}"   # 마지막 '-' 이후 = 버전
    elif [[ "$base" == *.zip ]]; then
        stem="${base%.zip}"
        WHEEL_NAME="${stem%-*}"
        WHEEL_VER="${stem##*-}"
    else
        return 1
    fi
    return 0
}

check_wheels() {
    echo "== 파이썬 휠(.venv) =="
    if [[ ! -x "$VENV_PY" ]]; then
        echo "  ! venv 파이썬 없음: $VENV_PY — 휠 점검 불가"
        missing=$((missing + 1))
        return
    fi
    if ! load_installed; then
        echo "  ! pip list 실패 — 휠 점검 불가"
        missing=$((missing + 1))
        return
    fi
    echo "  파이썬: $VENV_PY"

    local group dir f base nname ivers
    for group in "${WHEEL_GROUPS[@]}"; do
        dir="$WHEELS_ROOT/$group"
        echo "  -- 그룹: $group ($dir)"
        if [[ ! -d "$dir" ]]; then
            echo "     ! 디렉터리 없음 — 건너뜀"
            missing=$((missing + 1))
            continue
        fi
        shopt -s nullglob
        local files=("$dir"/*.whl "$dir"/*.tar.gz "$dir"/*.zip)
        shopt -u nullglob
        if [[ ${#files[@]} -eq 0 ]]; then
            echo "     (휠 파일 없음)"
            continue
        fi
        for f in "${files[@]}"; do
            base="$(basename "$f")"
            if ! parse_filename "$base"; then
                printf "     [SKIP]    %s (형식 미인식)\n" "$base"
                continue
            fi
            nname="$(normalize "$WHEEL_NAME")"
            ivers="${INSTALLED[$nname]:-}"
            if [[ -z "$ivers" ]]; then
                printf "     [MISSING] %s (%s)\n" "$nname" "$WHEEL_VER"
                missing=$((missing + 1))
            elif [[ "$ivers" == "$WHEEL_VER" ]]; then
                printf "     [OK]      %s==%s\n" "$nname" "$ivers"
            else
                # pip/setuptools/wheel 부트스트랩처럼 버전 차이는 흔하므로 경고만.
                printf "     [VER ?]   %s: 설치=%s, 번들=%s\n" "$nname" "$ivers" "$WHEEL_VER"
            fi
        done
    done
}

# ── 실행 ────────────────────────────────────────────────────────────────
[[ "${WHEELS_ONLY:-0}" == "1" ]] || check_os
[[ "${OS_ONLY:-0}" == "1" ]]     || check_wheels

echo
if [[ "$missing" -eq 0 ]]; then
    echo "결과: 모든 점검 통과 (누락 없음)"
    exit 0
else
    echo "결과: 누락/문제 $missing 건 — 위 [MISSING]/! 항목을 설치하세요."
    echo "  OS:   sudo dnf install -y ${OS_PACKAGES[*]}"
    echo "  휠:   sudo WHEELHOUSE=$WHEELS_ROOT/coordinator[:...] ./deploy/install.sh"
    exit 1
fi
