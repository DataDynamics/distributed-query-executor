#!/usr/bin/env bash
# 런처 공통 환경 + 헬퍼 함수(/data1 트리). 모든 start/stop/status 스크립트가 source 한다.
# 시스템 디렉터리(/etc·/opt·/var)를 쓰지 않고 모든 경로를 /data1 아래에 둔다.
APP_HOME="/data1/distributed-query-executor"
VENV_PY="$APP_HOME/.venv/bin/python"
LOG_DIR="$APP_HOME/logs"
RUN_DIR="$APP_HOME/run"

# 설정 디렉터리(코드 기본값과 동일하지만 명시적으로 고정)
export QUERY_EXECUTOR_CONFIG_DIR="$APP_HOME/config"
# 소스 트리에서 직접 실행하므로(패키지 미설치) coordinator/executor 모듈을 찾도록 경로 지정.
# src/ 가 패키지 루트이고, 커스텀 쿼리 함수(customs.*)는 APP_HOME 루트에 있어 둘 다 넣는다.
export PYTHONPATH="$APP_HOME/src:$APP_HOME${PYTHONPATH:+:$PYTHONPATH}"

# 기동할 executor 포트(config 의 coordinator.executors 와 일치시킬 것)
EXECUTOR_PORTS="${EXECUTOR_PORTS:-8087}"
# HA self-report 시 executor 가 알릴 자기 호스트(coordinator.executors 의 호스트와 일치).
# EXECUTOR_ADVERTISE_URL=http://$ADVERTISE_HOST:$port 로 self-report 에 기록된다.
ADVERTISE_HOST="${ADVERTISE_HOST:-127.0.0.1}"
# coordinator health 확인용 포트(config 의 coordinator.port 와 일치)
COORDINATOR_PORT="${COORDINATOR_PORT:-8088}"

mkdir -p "$LOG_DIR" "$RUN_DIR"
cd "$APP_HOME"

# ── 공통 헬퍼 ────────────────────────────────────────────────────────────
# 프로세스 1개 기동(nohup + PID 파일). 추가 환경변수는 호출 측에서 prefix 로 준다.
start_proc() {
    local name="$1"; shift
    local pidf="$RUN_DIR/$name.pid"
    if [[ -f "$pidf" ]] && kill -0 "$(cat "$pidf" 2>/dev/null)" 2>/dev/null; then
        echo "이미 실행 중: $name (pid $(cat "$pidf"))"; return 0
    fi
    nohup "$@" >>"$LOG_DIR/$name.out" 2>&1 &
    local pid=$!
    echo "$pid" > "$pidf"
    echo "기동: $name (pid $pid)"
}

# 프로세스 1개 중지(PID 파일 기준).
stop_proc() {
    local name="$1"
    local pidf="$RUN_DIR/$name.pid"
    if [[ -f "$pidf" ]]; then
        local pid; pid="$(cat "$pidf" 2>/dev/null || true)"
        if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" && echo "중지: $name (pid $pid)"
        else
            echo "미실행: $name (죽은 PID 파일 정리)"
        fi
        rm -f "$pidf"
    else
        echo "미실행: $name"
    fi
}

# 주어진 PID 가 종료될 때까지 대기한다($2 초, 기본 40). 재기동(restart) 시 옛 프로세스가
# 포트를 놓기 전에 새 프로세스를 띄우면 바인드에 실패하므로, 중지 후 이 함수로 기다린다.
# executor 는 graceful drain(executor.shutdown_drain_timeout_s, 기본 25초)이 걸릴 수 있어
# 넉넉히 기다리고, 제한 시간 내 안 죽으면 SIGKILL 로 강제 종료해 포트를 확실히 비운다.
wait_pid_gone() {
    local pid="$1" timeout="${2:-40}" waited=0
    [[ -z "$pid" ]] && return 0
    while kill -0 "$pid" 2>/dev/null; do
        if (( waited >= timeout )); then
            echo "경고: pid $pid 가 ${timeout}s 내 종료되지 않아 강제 종료(SIGKILL)"
            kill -9 "$pid" 2>/dev/null || true
            sleep 1
            return 0
        fi
        sleep 1; (( waited++ ))
    done
    return 0
}

# 프로세스 상태 한 줄.
proc_status() {
    local name="$1"
    local pidf="$RUN_DIR/$name.pid"
    if [[ -f "$pidf" ]] && kill -0 "$(cat "$pidf" 2>/dev/null)" 2>/dev/null; then
        echo "$name: RUNNING (pid $(cat "$pidf"))"
    else
        echo "$name: STOPPED"
    fi
}

# health 엔드포인트 확인. $1=라벨 $2=포트
health_check() {
    curl -s -o /dev/null -w "$1 health: %{http_code}\n" "http://127.0.0.1:$2/health" 2>/dev/null || true
}
