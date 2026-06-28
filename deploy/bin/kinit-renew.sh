#!/usr/bin/env bash
# Impala 접속용 Kerberos 티켓 발급/갱신. cron 등으로 주기 실행(예: 0 */4 * * *).
# principal/keytab 은 환경변수로 덮어쓸 수 있다.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/env.sh"

PRINCIPAL="${KRB5_PRINCIPAL:-svc-query@EXAMPLE.LOCAL}"
KEYTAB="${KRB5_KEYTAB:-$APP_HOME/config/impala.keytab}"

# 자리표시(빈) keytab 이면 조용히 건너뛴다 — 설치 직후 cron 이 매번 실패 로그를 남기지
# 않도록. 실제 keytab 을 배치하면 자동으로 동작한다.
if [[ ! -s "$KEYTAB" ]]; then
    echo "$(date '+%F %T') keytab 없음/비어있음($KEYTAB) → kinit 건너뜀"
    exit 0
fi

kinit -kt "$KEYTAB" "$PRINCIPAL"
klist
