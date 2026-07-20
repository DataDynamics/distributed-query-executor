"""coordinator 모니터링 대시보드(인라인 HTML + vanilla JS, npm/빌드 불필요).

이 모듈은 별도 프런트엔드 빌드 없이 단일 HTML 문자열(DASHBOARD_HTML)로 모니터링 UI 를
제공한다. 서버는 '/' 경로에서 이 HTML 을 그대로 서빙하고, 브라우저의 vanilla JS 가
/jobs·/cluster·/config·/info·/history 등의 JSON API 를 주기적으로 폴링해 각 탭(처리중인 쿼리,
실행 이력, Executor, 환경설정, 그외 정보)을 갱신한다.

executor 대시보드와 공유하는 스타일/헬퍼(포맷터·표·타임라인·모달·페이저·탭 배선)는
``core/static/dashboard-common.css``/``dashboard-common.js`` 로 추출돼 ``/assets`` 로
서빙된다(에어갭 내장, core/webassets). 이 파일에는 coordinator 전용 로직(job 목록/이력/
Executor 탭 렌더, job 타임라인 모달, 취소/재실행 액션)만 남는다.

Python 쪽 코드는 masked_config()(환경설정 탭 행 목록, 비밀값 마스킹) 하나이며,
mask_dsn 은 core.masking 으로 승격돼 여기서는 재수출만 한다(기존 임포트 경로 호환).
"""

from __future__ import annotations

from core.masking import mask_dsn  # noqa: F401 — 재수출(기존 `coordinator.dashboard.mask_dsn` 호환)


def masked_config(settings) -> list[dict]:
    """환경설정 탭에 표시할 (section, key, value) 행 목록을 만든다.

    현재 설정값을 섹션별로 평탄화한 행 리스트로 변환한다. DSN/비밀번호처럼 민감한 값은
    mask_dsn() 또는 '***' 로 가려 노출하지 않는다. 미설정 항목은 사람이 읽기 좋은 안내
    문자열('(없음)', '(미설정→Mock)' 등)로 대체한다.

    Args:
        settings: 노출할 설정 속성을 담은 설정 객체.

    Returns:
        list[dict]: 각 항목이 {"section", "key", "value", "desc"} 형태인 행 목록.
        desc 는 각 설정 항목의 의미를 설명하는 한 줄 안내다.
    """
    # (section, key, value, desc) — desc 는 환경설정 탭의 "설명" 컬럼에 표시된다.
    rows: list[tuple[str, str, object, str]] = [
        ("app", "name", settings.app_name, "애플리케이션 이름"),
        ("app", "debug", settings.debug, "디버그 모드(상세 로깅/검증)"),
        ("app", "query.sql_dialect", settings.query_default_dialect,
         "쿼리 파싱 기본 방언(요청에서 sql_dialect 로 재정의 가능)"),
        ("coordinator", "host", settings.coordinator_host, "coordinator 바인드 주소"),
        ("coordinator", "port", settings.coordinator_port, "coordinator 수신 포트"),
        ("coordinator", "id", settings.coordinator_id,
         "멀티 coordinator 식별자(미지정 시 host:port)"),
        ("coordinator", "executor_mode", settings.executor_mode,
         "remote(HTTP 디스패치) | local(in-process 직접 실행)"),
        ("coordinator", "max_concurrent_jobs", settings.max_concurrent_jobs,
         "동시에 RUNNING 가능한 job 수(실행 슬롯). 0 이하면 무제한"),
        ("coordinator", "max_pending_jobs", settings.max_pending_jobs,
         "슬롯이 찼을 때 PENDING 으로 대기 가능한 job 수. 실행+대기 합 초과 시 429 거부"),
        ("coordinator", "max_dispatch_concurrency", settings.max_dispatch_concurrency,
         "동시 task 디스패치 상한(코루틴 동시성)"),
        ("coordinator", "poll_interval_s", settings.poll_interval_s,
         "task 상태 폴링 간격(초)"),
        ("coordinator", "task_timeout_s", settings.task_timeout_s,
         "executor 호출(task) 전체(read) 타임아웃(초)"),
        ("coordinator", "task_connect_timeout_s", settings.task_connect_timeout_s,
         "executor 접속(connect) 타임아웃(초). 죽은 executor 를 빠르게 실패시킴"),
        ("coordinator", "task_max_retries", settings.task_max_retries,
         "연결 실패 시 같은 executor 재시도 횟수(지수 백오프)"),
        ("coordinator", "task_retry_backoff_s", settings.task_retry_backoff_s,
         "재시도 백오프 기준(초): 대기 = backoff * 2**시도"),
        ("coordinator", "task_failover", settings.task_failover,
         "재시도 소진 시 다른 executor 로 재배정(failover) 여부"),
        ("coordinator", "executor_select", settings.executor_select,
         "executor 선택 정책: round_robin | least_loaded | p2c(HA 권장)"),
        ("coordinator", "executor_health_source", settings.executor_health_source,
         "부하 뷰 소스: auto(멀티=self_report, 단일=monitor) | monitor | self_report"),
        ("coordinator", "executor_reservation", settings.executor_reservation,
         "공유 TTL 예약(엄격 균형). dispatch 중 task 를 예약해 전역 부하를 공유"),
        ("coordinator", "reservation_ttl_s", settings.reservation_ttl_s,
         "예약 만료(초). 죽은 coordinator 의 예약 누수 방지"),
        ("coordinator", "heartbeat_interval_s", settings.heartbeat_interval_s,
         "coordinator 자기 생존 heartbeat 주기(초)"),
        ("coordinator", "coordinator_stale_s", settings.coordinator_stale_s,
         "coordinator 생존 판정 임계(초). 초과 시 죽은 것으로 간주"),
        ("coordinator", "orphan_reconcile_interval_s", settings.orphan_reconcile_interval_s,
         "죽은 coordinator 소유 job 정합 주기(초). 0=비활성"),
        ("db", "schema", settings.db_schema, "메타 테이블 공통 스키마(모든 메타 테이블명 한정)"),
        ("store", "backend", settings.store_backend,
         "Job 저장소: memory(단일) | postgres(멀티 coordinator 공유)"),
        ("store", "table", settings.store_table, "공유 store 테이블명(postgres backend, 스키마 한정)"),
        ("monitor", "enabled", settings.monitor_enabled, "executor 헬스/메트릭 모니터링 사용 여부"),
        ("monitor", "health_interval_s", settings.monitor_health_interval_s,
         "executor 헬스 체크 주기(초)"),
        ("monitor", "record_interval_s", settings.monitor_record_interval_s,
         "메트릭 DB 기록 주기(초)"),
        ("monitor", "db_dsn", mask_dsn(settings.monitor_db_dsn),
         "메트릭 기록 DB DSN(미설정 시 폴링만, DB 기록 생략)"),
        ("monitor", "table", settings.monitor_table, "메트릭 테이블명"),
        ("monitor", "disk_path", settings.monitor_disk_path, "디스크 사용량 측정 경로"),
        ("history", "db_dsn", mask_dsn(settings.history_db_dsn),
         "실행 이력/공유 상태 DB DSN(미설정 시 이력 비활성)"),
        ("history", "table", settings.history_table, "job 실행 이력 테이블"),
        ("history", "task_table", settings.task_history_table, "task 실행 이력 테이블"),
        ("executor", "host", settings.executor_host,
         "executor 바인드 주소(포트는 EXECUTOR_PORT 환경변수)"),
        ("executor", "self_report", settings.executor_self_report,
         "executor 가 자기 상태를 공유 DB에 직접 기록(멀티 coordinator)"),
        ("executor", "advertise_url", settings.executor_advertise_url or "(없음)",
         "self-report 에 기록할 자기 base URL(HA URL 키 부하 뷰). coordinator.executors 와 일치"),
        ("executor", "status_table", settings.executor_status_table, "executor self-report 상태 테이블"),
        ("executor", "status_interval_s", settings.executor_status_interval_s,
         "executor self-report 주기(초)"),
        ("executor", "max_concurrent_tasks", settings.executor_max_concurrent_tasks,
         "executor 1대가 동시에 실행하는 task 수(0=무제한)"),
        ("executor", "shutdown_drain_timeout_s", settings.executor_shutdown_drain_timeout_s,
         "종료(SIGTERM) 시 진행 중 task 완료를 기다리는 최대 시간(초)"),
        ("executor", "executors", ", ".join(settings.executors) or "(없음)",
         "디스패치 대상 executor 베이스 URL 목록"),
        ("source", "type", settings.source_type,
         "소스 엔진: impala 전용. executor 의 task 읽기(SELECT)가 이 소스를 사용"),
        ("impala", "host", settings.impala_host or "(미설정→Mock)",
         "Impala(소스) 호스트. 미설정 시 MockBackend"),
        ("impala", "port", settings.impala_port, "Impala 포트(HiveServer2)"),
        ("impala", "database", settings.impala_database, "Impala 기본 데이터베이스"),
        ("impala", "auth_mechanism", settings.impala_auth_mechanism,
         "Impala 인증 방식(LDAP 기본 | PLAIN | NOSASL)"),
        ("impala", "use_ssl", settings.impala_use_ssl, "Impala 접속 TLS 사용 여부"),
        ("impala", "ca_cert", settings.impala_ca_cert, "TLS 검증용 CA 인증서 경로"),
        ("impala", "user", settings.impala_user, "Impala 사용자(LDAP 등)"),
        ("impala", "password", "***" if settings.impala_password else "",
         "Impala 비밀번호(설정 시 *** 로 마스킹)"),
        ("impala", "query_options",
         ", ".join(f"{k}={v}" for k, v in (settings.impala_query_options or {}).items()) or "(없음)",
         "Impala 쿼리 옵션 전역 기본값(SET). 요청별 옵션이 이 위에 병합됨"),
        ("greenplum", "dsn", mask_dsn(settings.greenplum_dsn),
         "Greenplum(타깃) 적재 DSN. 미설정 시 MockBackend"),
        ("greenplum", "copy_batch_size", settings.copy_batch_size, "COPY 배치 크기(행)"),
        ("greenplum", "copy_preflight", settings.copy_preflight,
         "COPY 전 SELECT 컬럼이 대상 테이블에 있는지 사전검증(불일치 조기 실패)"),
        ("greenplum", "copy_pipeline", getattr(settings, "copy_pipeline", True),
         "Impala 읽기와 GP COPY 를 별도 스레드로 겹쳐 실행(벽시계 단축)"),
        ("greenplum", "copy_queue_size", getattr(settings, "copy_queue_size", 8),
         "파이프라인 큐 크기(배치 개수). 메모리 ≈ queue_size × batch_size 행"),
        ("greenplum", "copy_format", getattr(settings, "copy_format", "text"),
         "COPY 포맷 text|binary. binary 는 인코딩 CPU 절감(타입 해석 실패 시 text 폴백)"),
        ("logging", "level", settings.log_level, "메인 로그 레벨(이 레벨 이상 기록)"),
        ("logging", "dir", str(settings.log_dir), "로그 디렉터리(일 단위 롤링)"),
        ("logging", "rolling.backup_count", settings.log_rolling_backup_count,
         "로그 보관 일수(초과분 자동 삭제)"),
    ]
    return [{"section": s, "key": k, "value": v, "desc": d} for s, k, v, d in rows]


# 대시보드 페이지 전체(HTML + coordinator 전용 JS)를 담은 문자열. '/' 핸들러가 이 값을 그대로
# 응답으로 내보낸다. 공용 스타일/헬퍼는 /assets/dashboard-common.css·js 에서 로드된다.
# 주의: 아래 문자열 내부는 브라우저로 전송되는 코드이므로 Python 주석을 끼워 넣지 말 것.
DASHBOARD_HTML = """<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Query Coordinator 모니터링</title>
<link rel="stylesheet" href="/assets/fonts.css">
<link rel="stylesheet" href="/assets/dashboard-common.css">
</head>
<body>
<header>
  <h1>🛰 Query Coordinator 모니터링</h1>
  <span class="meta" id="hdr"></span>
  <span class="meta" id="upd" style="margin-left:auto"></span>
</header>
<div class="tabs">
  <button data-tab="jobs" class="active">처리중인 Query</button>
  <button data-tab="hist">실행 이력</button>
  <button data-tab="exec">Executor</button>
  <button data-tab="tpl">템플릿</button>
  <button data-tab="qe">쿼리 실행</button>
  <button data-tab="ds">데이터소스</button>
  <button data-tab="conf">환경설정</button>
  <button data-tab="info">그외 정보</button>
</div>
<main>
  <section class="panel active" id="p-jobs"></section>
  <section class="panel" id="p-hist">
    <div class="filters" id="hist-filter" style="display:none">
      상태 <select id="hf-status" onchange="histSearch()">
        <option value="">(전체)</option>
        <option>DONE</option><option>PARTIAL</option><option>FAILED</option>
        <option>CANCELLED</option><option>RUNNING</option>
      </select>
      사용자 <input id="hf-user" placeholder="정확 일치" onkeydown="if(event.key==='Enter')histSearch()">
      작업 ID <input id="hf-job" placeholder="전방 일치" onkeydown="if(event.key==='Enter')histSearch()">
      <button class="btn" onclick="histSearch()">검색</button>
      <button class="btn" onclick="histReset()">초기화</button>
    </div>
    <div id="hist-body"></div>
  </section>
  <section class="panel" id="p-exec"></section>
  <section class="panel" id="p-tpl"></section>
  <section class="panel" id="p-qe">
    <div class="filters">
      템플릿 <select id="qe-tpl" onchange="qeOnTemplate()"></select>
      데이터소스 <select id="qe-ds"></select>
      상위 <input id="qe-limit" type="number" value="100" min="1" max="10000" style="width:80px"> 행
      <button class="btn" onclick="runQe()">실행</button>
      <span class="mut" id="qe-meta"></span>
    </div>
    <div id="qe-params" class="qe-params"></div>
    <div id="qe-out" class="mut">템플릿을 선택하고 파라미터를 입력한 뒤 실행하면 결과가 표시됩니다.</div>
  </section>
  <section class="panel" id="p-ds">
    <div class="filters">
      데이터소스 <select id="ds-name"></select>
      실행 위치 <select id="ds-exec"></select>
      상위 <input id="ds-limit" type="number" value="100" min="1" max="10000" style="width:80px"> 행
      <button class="btn" onclick="runDs()">실행</button>
      <span class="mut" id="ds-meta"></span>
    </div>
    <textarea id="ds-sql" class="sqlbox" placeholder="SELECT ...  (연결 확인/미리보기용 — 상위 N행만 반환됩니다. impala 는 executor 경유를 선택하세요)"></textarea>
    <div id="ds-out" class="mut">데이터소스를 선택하고 SELECT 를 실행하면 결과 미리보기가 표시됩니다.</div>
  </section>
  <section class="panel" id="p-conf"></section>
  <section class="panel" id="p-info"></section>
</main>
<div class="modal" id="modal" onclick="if(event.target===this)closeModal()">
  <div class="modal-box">
    <div class="modal-head"><b id="modal-title"></b><button onclick="closeModal()">✕</button></div>
    <pre id="modal-sql"></pre>
  </div>
</div>
<div class="modal" id="pmodal" onclick="if(event.target===this)closePhases()">
  <div class="modal-box">
    <div class="modal-head"><b id="pmodal-title"></b><button onclick="closePhases()">✕</button></div>
    <div id="pmodal-body"></div>
  </div>
</div>
<script src="/assets/dashboard-common.js"></script>
<script>
// 작업 ID → 쿼리문 매핑/모달
const sqlMap = {};
function showSql(id){ showTextModal(id, sqlMap[id] || '(쿼리문 없음)'); }
const jobLink = id => `<a class="lnk" onclick="showSql('${id}');return false">${esc(id)}</a>`;
const bar = p => `<span class="bar"><i style="width:${p||0}%"></i></span> ${p||0}%`;
// 현재 단계 집계({STREAM_COPY:3,...})를 라벨 칩들로 요약 표기.
const PHASE_LABELS = {QUEUE_WAIT:"대기", IMPALA_SUBMIT:"조회", STAGING_DDL:"staging",
  PREFLIGHT:"검증", DELETE:"선삭제", STREAM_COPY:"COPY", INSERT:"INSERT", COMMIT:"커밋"};
function phaseSummary(s){
  const keys = Object.keys(s||{});
  if(!keys.length) return '<span class="mut">-</span>';
  return keys.map(k=>`<span class="phdot">${PHASE_LABELS[k]||esc(k)} ${s[k]}</span>`).join(' ');
}
// job 상세 모달: /jobs/{id} 를 받아 task 별 단계 타임라인을 세로로 쌓아 보여준다.
async function showJobPhases(id){
  showPhasesModal(id + ' · task별 단계 진행/소요', '<p class="mut">불러오는 중…</p>');
  try{
    const d = await getJSON(`/jobs/${id}`);
    const tasks = d.tasks||[];
    if(!tasks.length){ $("#pmodal-body").innerHTML = '<p class="mut">task 가 없습니다.</p>'; return; }
    $("#pmodal-body").innerHTML = tasks.map(t=>
      `<div class="tkhd"><code>${fmt(t.task_id)}</code> ${pill(t.status)} · `+
      `<span class="mut">executor</span> ${fmt(t.executor_url)}`+
      (t.attempt>1?` · <span class="mut">시도 ${t.attempt}회</span>`:'')+` · `+
      `읽은 ${fmtNum(t.rows_read)} / 적재 ${fmtNum(t.rows_written)} · `+
      `조회완료 ${fmtDate(t.impala_done_at)} · `+
      `<a class="lnk" onclick="showTaskSql('${id}','${t.task_id}');return false">SQL</a></div>`+
      (t.error?`<div class="err" style="margin:0 0 6px">${esc(t.error)}</div>`:'')+
      renderPhases(t.phases)
    ).join('');
  }catch(e){ $("#pmodal-body").innerHTML = `<span class="err">불러오기 실패: ${esc(e)}</span>`; }
}
const jobPhaseLink = id => `<a class="lnk" onclick="showJobPhases('${id}');return false">타임라인</a>`;
// 타임라인 모달에서 task 하나가 실제 실행한 sub-query 전문을 SQL 모달로 띄운다
// (executor 대시보드의 task SQL 열람과 대칭). SQL 모달은 타임라인 모달 위에 겹쳐 뜬다.
async function showTaskSql(jobId, taskId){
  try{
    const t = await getJSON(`/jobs/${jobId}/tasks/${taskId}`);
    showTextModal(taskId, (t && t.sub_query) || '(쿼리문 없음)');
  }catch(e){ showTextModal(taskId, '불러오기 실패: ' + e); }
}

// 진행 중 job 취소: 각 executor 로 취소가 전파되고 job 은 CANCELLED 로 종료된다.
async function cancelJob(id){
  if(!confirm('작업 ' + id + ' 을(를) 취소할까요?')) return;
  try{ await postJSON(`/jobs/${id}/cancel`); }
  catch(e){ alert('취소 실패: ' + e.message); }
  refresh();
}
// 실패 파티션만 재실행: 성공 파티션은 건너뛰고 새 job_id 로 복제 실행된다(202).
async function retryJob(id){
  if(!confirm('작업 ' + id + ' 의 실패/취소 파티션만 재실행할까요?')) return;
  try{
    const d = await postJSON(`/jobs/${id}/retry`);
    alert('재실행 시작: 새 작업 ' + d.job_id + ' (task ' + d.retried_tasks + '개)');
  }catch(e){ alert('재실행 실패: ' + e.message); }
  refresh();
}
// 액션 버튼: 상태에 따라 취소(활성 job) / 재실행(종료됐지만 실패분이 있는 job)만 노출.
const ACTIVE_JOB = ["SPLITTING","PENDING","RUNNING"];
const RETRIABLE_JOB = ["PARTIAL","FAILED","CANCELLED"];
const cancelBtn = r => ACTIVE_JOB.includes(r.status)
  ? `<button class="btn danger" onclick="cancelJob('${r.job_id}')">취소</button>` : fmt(null);
const retryBtn = r => RETRIABLE_JOB.includes(r.status)
  ? `<button class="btn" onclick="retryJob('${r.job_id}')">재실행</button>` : fmt(null);

async function loadJobs(){
  const d = await getJSON("/jobs?status=active&limit=0");
  (d.jobs||[]).forEach(r=>{ if(r.original_sql) sqlMap[r.job_id]=r.original_sql; });
  const cols = [
    {t:"작업 ID", f:r=>jobLink(r.job_id)},
    {t:"사용자", k:"username"},
    {t:"상태", f:r=>pill(r.status)},
    {t:"진행률", f:r=>bar(r.progress_percent)},
    {t:"완료/전체", f:r=>`${r.completed}/${r.total}`},
    {t:"현재 단계", f:r=>phaseSummary(r.phase_summary)},
    {t:"단계", f:r=>jobPhaseLink(r.job_id)},
    {t:"읽은 행수", f:r=>fmtNum(r.total_rows_read)},
    {t:"적재 행수", f:r=>fmtNum(r.total_rows_written)},
    {t:"실행 방식", k:"exec_mode"},
    {t:"대상 테이블", k:"target_table"},
    {t:"시작 시간", f:r=>`<span class="mut">${fmtDate(r.started_at)}</span>`},
    {t:"종료 시간", f:r=>`<span class="mut">${fmtDate(r.finished_at)}</span>`},
    {t:"소요 시간", f:r=>dur(r.started_at, r.finished_at)},
    {t:"에러", cls:"col-err", f:r=>r.error?`<span class="err">${esc(r.error)}</span>`:fmt(null)},
    {t:"액션", f:cancelBtn},
  ];
  $("#p-jobs").innerHTML =
    `<div class="cards">
       <div class="card"><div class="k">총 작업</div><div class="v">${d.total}</div></div>
       <div class="card"><div class="k">실행중</div><div class="v">${d.running}</div></div>
       <div class="card"><div class="k">활성(대기+실행)</div><div class="v">${d.active}</div></div>
       <div class="card"><div class="k">실행 슬롯</div><div class="v">${concBar(d.running, d.max_concurrent_jobs)}</div></div>
       <div class="card"><div class="k">대기 큐</div><div class="v">${concBar(d.pending, d.max_pending_jobs)}</div></div>
     </div>` + table(cols, d.jobs);
}
async function loadHist(){
  const d = await getJSON(`/history?limit=${HIST_LIMIT}&offset=${histOffset}${histFilterQS()}`);
  $("#hist-filter").style.display = d.enabled ? 'flex' : 'none';
  if(!d.enabled){
    $("#hist-body").innerHTML = `<p class="mut">이력 DB(history.db_dsn)가 설정되지 않았습니다. ` +
      `PostgreSQL 설정 시 과거 실행 이력이 표시됩니다.</p>`;
    return;
  }
  histTotal = d.total || 0;
  (d.rows||[]).forEach(r=>{ if(r.original_sql) sqlMap[r.job_id]=r.original_sql; });
  const cols = [
    {t:"작업 ID", f:r=>jobLink(r.job_id)},
    {t:"사용자", k:"username"},
    {t:"상태", f:r=>pill(r.status)},
    {t:"대상 테이블", k:"target_table"},
    {t:"완료/전체", f:r=>`${fmt(r.completed_tasks)}/${fmt(r.total_tasks)}`},
    {t:"적재 행수", f:r=>fmtNum(r.total_rows_written)},
    {t:"시작 시간", f:r=>`<span class="mut">${fmtDate(r.started_at)}</span>`},
    {t:"종료 시간", f:r=>`<span class="mut">${fmtDate(r.finished_at)}</span>`},
    {t:"소요 시간", cls:"col-dur", f:r=>dur(r.started_at, r.finished_at)},
    {t:"에러", cls:"col-err", f:r=>r.error?`<span class="err">${esc(r.error)}</span>`:fmt(null)},
    {t:"액션", f:retryBtn},
  ];
  const n = d.rows ? d.rows.length : 0;
  $("#hist-body").innerHTML = table(cols, d.rows) + pagerHtml(n);
}

async function loadExec(){
  const d = await getJSON("/cluster");
  const cm = d.coordinator.metrics;
  const assign = d.assignment_counts || {};
  const cards = `<div class="cards">
    <div class="card"><div class="k">Coordinator CPU</div><div class="v">${cm.cpu_percent}%</div></div>
    <div class="card"><div class="k">MEM</div><div class="v">${cm.memory.percent}%</div></div>
    <div class="card"><div class="k">DISK</div><div class="v">${cm.disk.percent}%</div></div>
    <div class="card"><div class="k">Executor</div><div class="v">${d.executors_summary.healthy}/${d.executors_summary.total}</div></div>
    <div class="card"><div class="k">선택 정책</div><div class="v">${fmt(d.executor_select)}</div></div>
   </div>`;
  const cols = [
    {t:"Executor", f:r=>r.executor_url?`<a class="lnk" href="${r.executor_url}" target="_blank" rel="noopener" title="executor 대시보드 열기"><code>${fmt(r.executor_id||r.executor_url)}</code></a>`:`<code>${fmt(r.executor_id||r.executor_url)}</code>`},
    {t:"Healthy", f:r=>r.healthy?'<span class="ok">● UP</span>':'<span class="bad">● DOWN</span>'},
    {t:"CPU%", k:"cpu_percent"},
    {t:"MEM%", k:"memory_percent"},
    {t:"DISK%", k:"disk_percent"},
    {t:"동시 처리", f:r=>concBar(r.active_tasks, r.max_concurrent_tasks)},
    {t:"누적 배정", f:r=>fmtNum(assign[r.executor_url])},
    {t:"Last Seen", f:r=>`<span class="mut">${fmtDate(r.updated_at||r.last_checked)}</span>`},
    {t:"Error", cls:"col-err", f:r=>r.error?`<span class="err">${esc(r.error)}</span>`:fmt(null)},
  ];
  $("#p-exec").innerHTML = cards + table(cols, d.executors);
}
// 템플릿 탭: 서버에 배포된 쿼리 템플릿과 파라미터 스키마를 나열한다(GET /templates).
async function loadTpl(){
  const d = await getJSON("/templates");
  if(!d.enabled){
    $("#p-tpl").innerHTML = '<p class="mut">쿼리 템플릿 엔진(template.enabled)이 비활성입니다. ' +
      '활성화하면 template_id 로 실행 가능한 서버 템플릿 목록이 표시됩니다.</p>';
    return;
  }
  const paramChips = ps => (ps||[]).map(p=>
    `<span class="phdot">${esc(p.name)}: ${esc(p.type)}${p.required?' *':''}`+
    `${(p.default!==undefined&&p.default!==null)?' = '+esc(JSON.stringify(p.default)):''}</span>`
  ).join(' ');
  const cols = [
    {t:"template_id", f:r=>`<code>${fmt(r.template_id)}</code>`},
    {t:"설명", f:r=>`<div class="desc">${fmt(r.description)}</div>`},
    {t:"기본 exec_mode", k:"exec_mode"},
    {t:"파티션 컬럼", k:"partition_column"},
    {t:"파라미터 (* 필수)", cls:"desc", f:r=>paramChips(r.params)||fmt(null)},
  ];
  $("#p-tpl").innerHTML =
    '<p class="mut">클라이언트는 POST /jobs 에 SQL 전문 대신 template_id + params 를 보내 실행할 수 있습니다.</p>'
    + table(cols, d.templates||[]);
}
// 쿼리 실행 탭: 템플릿 선택 → 파라미터 입력 → POST /query-execute → 결과+executed_by 표시.
// 자동 갱신(refresh)마다 폼/결과가 지워지지 않도록 초기화(select 채움)는 1회만 하고, 결과는
// 실행 버튼을 누를 때만 갱신한다(loadDs 와 동일한 정적 영역 원칙).
let qeInit = false, qeTemplates = [];
async function loadQe(){
  if(qeInit) return;
  const d = await getJSON("/templates");
  if(!d.enabled){
    $("#p-qe").innerHTML = '<p class="mut">쿼리 템플릿 엔진(template.enabled)이 비활성입니다. ' +
      '활성화하면 템플릿을 골라 파라미터를 입력하고 POST /query-execute 로 실행할 수 있습니다.</p>';
    qeInit = true; return;
  }
  qeTemplates = d.templates || [];
  $("#qe-tpl").innerHTML = qeTemplates.map(t=>
    `<option value="${esc(t.template_id)}">${esc(t.template_id)}</option>`).join('');
  // 데이터소스 선택지는 query-execute 의 실행 라우팅에 맞춘 고정 목록이다(미리보기의 built-in
  // 소스 목록과 다르다). 소스 실행은 datasource 종류와 무관하게 /query-run(커스텀 함수)로 통일되므로
  // impala/trino 를 따로 나열하지 않고 '소스' 하나로 두고, greenplum/history 만 coordinator 직접 실행.
  $("#qe-ds").innerHTML =
    '<option value="">소스 (커스텀 함수 · 기본 source.type)</option>' +
    '<option value="greenplum">greenplum (coordinator 직접)</option>' +
    '<option value="history">history (coordinator 직접)</option>';
  qeOnTemplate();
  qeInit = true;
}
// 선택된 템플릿의 파라미터 스키마로 입력 필드를 생성한다(list 는 쉼표 구분 안내).
function qeOnTemplate(){
  const t = qeTemplates.find(x=>x.template_id===$("#qe-tpl").value);
  const params = (t && t.params) || [];
  if(!params.length){ $("#qe-params").innerHTML = '<p class="mut">이 템플릿은 파라미터가 없습니다.</p>'; return; }
  $("#qe-params").innerHTML = params.map(p=>{
    const req = p.required ? ' *' : '';
    const ph = p.type==='list' ? '쉼표로 구분(예: KR, US, JP)'
      : ((p.default!==undefined && p.default!==null) ? '기본: '+JSON.stringify(p.default) : p.type);
    return `<label class="qe-field"><span>${esc(p.name)}<em>${esc(p.type)}${req}</em></span>`+
           `<input data-pname="${esc(p.name)}" data-ptype="${esc(p.type)}" placeholder="${esc(ph)}"></label>`;
  }).join('');
}
// 입력값을 [{name,value}] 배열로 만든다. 빈 값은 생략(서버가 기본값/필수 검증 담당).
// list 는 쉼표로 분리한 배열, 숫자 타입은 Number 로 변환, 그 외는 문자열 그대로.
function qeCollectParams(){
  const out = [];
  document.querySelectorAll('#qe-params input[data-pname]').forEach(el=>{
    const raw = el.value.trim();
    if(raw==='') return;
    const type = el.dataset.ptype;
    let value;
    if(type==='list') value = raw.split(',').map(s=>s.trim()).filter(s=>s!=='');
    else if(type==='int'||type==='integer'||type==='float'||type==='number') value = Number(raw);
    else value = raw;
    out.push({name: el.dataset.pname, value: value});
  });
  return out;
}
async function runQe(){
  const tid = $("#qe-tpl").value;
  if(!tid){ alert('템플릿을 선택하세요'); return; }
  const body = { template_id: tid, params: qeCollectParams(),
                 limit: Math.max(1, parseInt($("#qe-limit").value, 10) || 100) };
  const ds = $("#qe-ds").value; if(ds) body.datasource = ds;
  $("#qe-meta").textContent = '';
  $("#qe-out").innerHTML = '<p class="mut">실행 중…</p>';
  try{
    const d = await postJSON("/query-execute", body);
    renderProbeResult(d, "#qe-meta", "#qe-out");
  }catch(e){ $("#qe-out").innerHTML = `<p class="err">${esc(e.message)}</p>`; }
}
// 데이터소스 탭 초기화(1회): 소스/실행 위치 select 를 채운다. 결과 영역은 실행 시에만 갱신
// (자동 갱신 주기마다 입력/결과가 지워지지 않도록 이 로더는 재호출 시 아무것도 안 한다).
let dsInit = false;
async function loadDs(){
  if(dsInit) return;
  const d = await getJSON("/datasources");
  const opts = (d.local||[]).map(s=>
    `<option value="${s.name}" ${s.configured?'':'disabled'}>${s.name}${s.configured?'':' (미구성)'}</option>`);
  // coordinator 에 드라이버가 없는 소스(impala)는 via_executor 목록에서 받아
  // executor 경유 전용 옵션으로 추가한다(서버가 소스를 늘리면 UI 는 자동 반영).
  const localNames = new Set((d.local||[]).map(s=>s.name));
  for(const n of (d.via_executor||[])){
    if(!localNames.has(n)) opts.push(`<option value="${esc(n)}">${esc(n)} (executor 경유 필수)</option>`);
  }
  $("#ds-name").innerHTML = opts.join('');
  $("#ds-exec").innerHTML = '<option value="">coordinator 직접</option>' +
    (d.executors||[]).map(u=>`<option value="${esc(u)}">${esc(u)} 경유</option>`).join('');
  dsInit = true;
}
// impala 등은 coordinator 에 드라이버가 없어 executor 경유(executor_url)로 프록시한다.
function runDs(){
  const ex = $("#ds-exec").value;
  return runDatasourceQuery(ex ? {executor_url: ex} : null);
}
async function loadConf(){
  const d = await getJSON("/config");
  const cols = [
    {t:"section", f:r=>`<span class="sec">${esc(r.section)}</span>`},
    {t:"key", k:"key"},
    {t:"value", f:r=>fmt(String(r.value))},
    {t:"설명", f:r=>`<div class="desc">${fmt(r.desc)}</div>`},
  ];
  $("#p-conf").innerHTML = table(cols, d.config);
}
// 그외 정보 탭의 각 key 에 대한 한 줄 설명. jobs.<status> 키는 동적이라 별도 처리한다.
const infoDesc = {
  version: "애플리케이션 버전",
  coordinator_id: "이 coordinator 식별자(멀티 인스턴스 구분)",
  executor_mode: "remote(HTTP 디스패치) | local(in-process 직접 실행)",
  store_backend: "Job 저장소 backend(memory | postgres)",
  executor_self_report: "executor 자기 상태 self-report 사용 여부",
  executor_select: "executor 선택 정책(round_robin | least_loaded | p2c)",
  executor_health_source: "부하 뷰 소스(auto | monitor | self_report)",
  executor_reservation: "공유 TTL 예약(엄격 균형) 사용 여부",
  started_at: "이 coordinator 기동 시각",
  uptime_seconds: "기동 후 경과 시간(초)",
  jobs_total: "저장소에 보관된 전체 job 수",
  executors_configured: "설정된 executor 수",
  max_concurrent_jobs: "동시 실행 슬롯 수(job)",
  max_pending_jobs: "대기 큐 상한(실행+대기 초과 시 429)",
  max_dispatch_concurrency: "동시 task 디스패치 상한",
};
function infoDescOf(k){ return k.startsWith("jobs.") ? "상태별 job 수" : (infoDesc[k] || ""); }
async function loadInfo(){
  const d = await getJSON("/info");
  const rows = Object.entries(d).filter(([k,v])=>typeof v!=="object")
    .map(([k,v])=>({key:k,value:v}));
  const byStatus = Object.entries(d.jobs_by_status||{}).map(([k,v])=>({key:"jobs."+k,value:v}));
  const cols = [{t:"key",k:"key"},
    {t:"value", f:r=> r.key.endsWith("_at") ? fmtDate(r.value) : fmt(String(r.value))},
    {t:"설명", f:r=>`<div class="desc">${fmt(infoDescOf(r.key))}</div>`}];
  $("#p-info").innerHTML = table(cols, rows.concat(byStatus));
}
getJSON("/info").then(d=>{ $("#hdr").textContent =
  `id=${d.coordinator_id} · mode=${d.executor_mode} · store=${d.store_backend} · select=${d.executor_select} · v${d.version}`; });
initDashboard({jobs:loadJobs, hist:loadHist, exec:loadExec, tpl:loadTpl, qe:loadQe, ds:loadDs,
               conf:loadConf, info:loadInfo}, "jobs");
</script>
</body>
</html>"""
