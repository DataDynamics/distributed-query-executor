#!/usr/bin/env bash
# Impala 접속용 Kerberos 티켓 발급/갱신. cron 등으로 주기 실행(예: 0 */4 * * *).
# principal/keytab 은 환경변수로 덮어쓸 수 있다.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$DIR/env.sh"

PRINCIPAL="${KRB5_PRINCIPAL:-svc-query@EXAMPLE.LOCAL}"
KEYTAB="${KRB5_KEYTAB:-$APP_HOME/config/impala.keytab}"

kinit -kt "$KEYTAB" "$PRINCIPAL"
klist
