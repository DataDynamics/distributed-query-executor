"""executor 모니터링 대시보드(인라인 HTML + vanilla JS, npm/빌드 불필요).

remote mode 에서 각 executor 프로세스가 자신의 / 에 단일 HTML 을 서빙하고,
브라우저가 /tasks·/metrics·/history·/config·/info 를 폴링해 탭을 갱신한다.
local mode 에서는 executor 프로세스가 따로 없으므로 coordinator 대시보드만 보인다.

coordinator 대시보드와 공유하는 스타일/헬퍼(포맷터·표·타임라인·모달·페이저·탭 배선)는
``core/static/dashboard-common.css``/``dashboard-common.js`` 로 추출돼 ``/assets`` 로
서빙된다(에어갭 내장, core/webassets). 이 파일에는 executor 전용 로직(task 목록/이력
렌더, SQL 조합 모달, 취소 액션)만 남는다. mask_dsn 은 core.masking 재수출이다.
"""

from __future__ import annotations

from core.masking import mask_dsn  # noqa: F401 — 재수출(기존 `executor.dashboard.mask_dsn` 호환)


def masked_config(settings) -> list[dict]:
    """대시보드 환경설정 탭에 표시할 설정 행 목록을 만든다.

    설정을 (section, key, value) 묶음으로 평탄화해 ``{"section", "key", "value"}`` dict
    리스트로 반환한다. DSN(history/greenplum)은 ``mask_dsn`` 으로, impala 비밀번호는
    값 존재 여부만 ``***`` 로 노출해 비밀값이 화면에 그대로 드러나지 않게 한다.

    인자:
        settings: app/executor/history/impala/greenplum/logging 등 설정 속성을 가진 객체.

    반환:
        ``[{"section": ..., "key": ..., "value": ..., "desc": ...}, ...]`` 형태의 행 목록.
        desc 는 각 설정 항목의 의미를 설명하는 한 줄 안내다.
    """
    # (section, key, value, desc) — desc 는 환경설정 탭의 "설명" 컬럼에 표시된다.
    rows: list[tuple[str, str, object, str]] = [
        ("app", "name", settings.app_name, "애플리케이션 이름"),
        ("app", "debug", settings.debug, "디버그 모드(상세 로깅/검증)"),
        ("executor", "host", settings.executor_host,
         "executor 바인드 주소(포트는 EXECUTOR_PORT 환경변수)"),
        ("executor", "self_report", settings.executor_self_report,
         "자기 상태를 공유 DB에 직접 기록(멀티 coordinator)"),
        ("executor", "advertise_url", settings.executor_advertise_url or "(없음)",
         "self-report 에 기록할 자기 base URL(HA URL 키 부하 뷰). coordinator.executors 와 일치"),
        ("db", "schema", settings.db_schema, "메타 테이블 공통 스키마(상태/이력 테이블명 한정)"),
        ("executor", "status_table", settings.executor_status_table, "self-report 상태 테이블(스키마 한정)"),
        ("executor", "status_interval_s", settings.executor_status_interval_s,
         "self-report 주기(초)"),
        ("executor", "max_concurrent_tasks", settings.executor_max_concurrent_tasks,
         "이 executor 가 동시에 실행하는 task 수(0=무제한)"),
        ("executor", "shutdown_drain_timeout_s", settings.executor_shutdown_drain_timeout_s,
         "종료(SIGTERM) 시 진행 중 task 완료를 기다리는 최대 시간(초)"),
        ("executor", "gp_hostname",
         getattr(settings, "executor_gp_hostname", "") or "(미설정→hostname 사용)",
         "local_stage: 이 executor 의 GP 세그먼트 호스트명(file:// URI 조립). "
         "gp_segment_configuration.hostname 과 일치해야 함"),
        ("history", "db_dsn", mask_dsn(settings.history_db_dsn),
         "task 이력/공유 상태 DB DSN(미설정 시 이력 비활성)"),
        ("history", "task_table", settings.task_history_table, "task 실행 이력 테이블"),
        ("monitor", "disk_path", settings.monitor_disk_path, "디스크 사용량 측정 경로"),
        ("source", "type", settings.source_type,
         "소스 엔진: impala 전용. task 읽기(SELECT)가 이 소스를 사용"),
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
        ("greenplum", "pool_max", settings.greenplum_pool_max,
         "GP 커넥션 풀 최대 크기(동시 GP 연결 상한). 0이면 max_concurrent_tasks 와 동일"),
        ("greenplum", "copy_batch_size", settings.copy_batch_size, "COPY 배치 크기(행)"),
        ("greenplum", "copy_preflight", settings.copy_preflight,
         "COPY 전 SELECT 컬럼이 대상 테이블에 있는지 사전검증(불일치 조기 실패)"),
        ("greenplum", "copy_pipeline", getattr(settings, "copy_pipeline", True),
         "Impala 읽기와 GP COPY 를 별도 스레드로 겹쳐 실행(벽시계 단축)"),
        ("greenplum", "copy_queue_size", getattr(settings, "copy_queue_size", 8),
         "파이프라인 큐 크기(배치 개수). 메모리 ≈ queue_size × batch_size 행"),
        ("greenplum", "copy_format", getattr(settings, "copy_format", "text"),
         "COPY 포맷 text|binary. binary 는 인코딩 CPU 절감(타입 해석 실패 시 text 폴백)"),
        # local_stage(file:// 세그먼트 로컬 스테이징). executor 는 Phase 1(Impala→로컬 CSV write)
        # 을 맡고, 나머지 값(호스트 검증·파일 예산)은 coordinator 의 Phase 2 용이지만 설정을
        # 공유하므로 함께 보인다. CSV 방언은 write 와 외부테이블 FORMAT 이 반드시 일치해야 한다.
        ("stage", "local_dir", getattr(settings, "stage_local_dir", ""),
         "local_stage 로컬 CSV 저장 루트(GP 세그먼트가 file:// 로 읽는 경로, job_id 하위 격리)"),
        ("stage", "csv_delimiter", getattr(settings, "stage_csv_delimiter", "`"),
         "CSV 컬럼 구분자. 이 write 방언이 외부테이블 FORMAT 과 일치해야 함(s3_stage 도 동일)"),
        ("stage", "csv_null", getattr(settings, "stage_csv_null", "") or "(빈 문자열)",
         "CSV NULL 표현"),
        ("stage", "csv_quote", getattr(settings, "stage_csv_quote", '"'), "CSV 인용문자"),
        ("stage", "cleanup", getattr(settings, "stage_cleanup", True),
         "Phase 3 에서 이 호스트의 로컬 CSV 디렉터리를 정리할지 여부"),
        ("stage", "validate_hosts", getattr(settings, "stage_validate_hosts", True),
         "coordinator 가 Phase 2 전 file:// 호스트를 gp_segment_configuration 과 대조 검증"),
        ("stage", "max_files_per_host", getattr(settings, "stage_max_files_per_host", 0),
         "호스트당 최대 파일 수(0=호스트별 primary 세그먼트 수 S_h)"),
        ("stage", "impala_convert_types",
         getattr(settings, "stage_impala_convert_types", False),
         "export fetch 형변환(false=끔: timestamp/date/decimal 을 wire 문자열 그대로 CSV write)"),
        # s3_stage(2-phase). executor 는 Phase 1(Impala→로컬 CSV→S3 업로드)과 Phase 3(정리)에서
        # 이 값들을 쓴다. PXF 관련 값은 coordinator 의 Phase 2 용이지만 설정을 공유하므로 함께 보인다.
        ("s3", "bucket", getattr(settings, "s3_bucket", "") or "(미설정→s3_stage 비활성)",
         "s3_stage 스테이징 버킷. 비우면 s3_stage 요청 시에만 실패(다른 모드 무영향)"),
        ("s3", "prefix", getattr(settings, "s3_prefix", "dqe-stage"),
         "객체 키 프리픽스 → s3://<bucket>/<prefix>/<job_id>/<task_id>.csv"),
        ("s3", "external_schema",
         getattr(settings, "s3_external_schema", "") or "(없음→search_path)",
         "Phase 2 외부테이블 스키마 → <schema>.s3ext_<job_id>(coordinator 가 사용)"),
        ("s3", "endpoint_url", getattr(settings, "s3_endpoint_url", "") or "(AWS 기본)",
         "온프렘 S3 호환(MinIO/Ceph) 엔드포인트"),
        ("s3", "region", getattr(settings, "s3_region", "") or "(기본)", "S3 리전"),
        ("s3", "access_key",
         "***" if getattr(settings, "s3_access_key", "") else "(boto3 기본 체인)",
         "업로드 자격증명(설정 시 *** 로 마스킹). 비우면 boto3 기본 체인"),
        ("s3", "secret_key",
         "***" if getattr(settings, "s3_secret_key", "") else "(boto3 기본 체인)",
         "업로드 자격증명(설정 시 *** 로 마스킹)"),
        ("s3", "use_ssl", getattr(settings, "s3_use_ssl", True), "업로드 TLS 사용 여부"),
        ("s3", "pxf_server", getattr(settings, "s3_pxf_server", "") or "(없음)",
         "GP 읽기용 PXF SERVER 이름(coordinator 의 Phase 2 LOCATION 조립에 사용)"),
        ("s3", "pxf_profile", getattr(settings, "s3_pxf_profile", "s3:csv"),
         "PXF 프로파일(기본 s3:csv)"),
        ("s3", "delete_on_cleanup", getattr(settings, "s3_delete_on_cleanup", True),
         "Phase 3 에서 S3 스테이징 객체를 지울지 여부(false 면 수명주기 정책에 위임)"),
        ("logging", "level", settings.log_level, "메인 로그 레벨(이 레벨 이상 기록)"),
        ("logging", "dir", str(settings.log_dir), "로그 디렉터리(일 단위 롤링)"),
        ("logging", "rolling.backup_count", settings.log_rolling_backup_count,
         "로그 보관 일수(초과분 자동 삭제)"),
        ("logging", "sql.enabled", settings.log_sql_enabled,
         "실행 SQL 로깅(core.sql, INFO 로 항상 기록 — 로그 레벨과 무관)"),
        ("logging", "sql.max_length", settings.log_sql_max_length,
         "실행 SQL 로그 최대 길이(초과분 절단 표기)"),
        ("logging", "sql.params", settings.log_sql_params,
         "실행 SQL 의 바인드 파라미터 동반 기록"),
    ]
    return [{"section": s, "key": k, "value": v, "desc": d} for s, k, v, d in rows]


# 대시보드 페이지 전체(HTML + executor 전용 JS)를 담은 문자열. '/' 핸들러가 이 값을 그대로
# 응답으로 내보낸다. 공용 스타일/헬퍼는 /assets/dashboard-common.css·js 에서 로드된다.
# 주의: 아래 문자열 내부는 브라우저로 전송되는 코드이므로 Python 주석을 끼워 넣지 말 것.
DASHBOARD_HTML = """<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Query Executor 모니터링</title>
<link rel="stylesheet" href="/assets/fonts.css">
<link rel="stylesheet" href="/assets/dashboard-common.css">
</head>
<body>
<header>
  <h1>🛰 Query Executor 모니터링</h1>
  <span class="meta" id="hdr"></span>
  <span class="meta" id="upd" style="margin-left:auto"></span>
</header>
<div class="tabs">
  <button data-tab="tasks" class="active">처리중인 Task</button>
  <button data-tab="hist">실행 이력</button>
  <button data-tab="ds">데이터소스</button>
  <button data-tab="conf">환경설정</button>
  <button data-tab="info">그외 정보</button>
</div>
<main>
  <section class="panel active" id="p-tasks"></section>
  <section class="panel" id="p-hist">
    <div class="filters" id="hist-filter" style="display:none">
      상태 <select id="hf-status" onchange="histSearch()">
        <option value="">(전체)</option>
        <option>DONE</option><option>FAILED</option><option>CANCELLED</option>
        <option>WRITING</option><option>READING</option><option>QUEUED</option>
      </select>
      사용자 <input id="hf-user" placeholder="정확 일치" onkeydown="if(event.key==='Enter')histSearch()">
      작업 ID <input id="hf-job" placeholder="전방 일치" onkeydown="if(event.key==='Enter')histSearch()">
      <button class="btn" onclick="histSearch()">검색</button>
      <button class="btn" onclick="histReset()">초기화</button>
    </div>
    <div id="hist-body"></div>
  </section>
  <section class="panel" id="p-ds">
    <div class="filters">
      데이터소스 <select id="ds-name"></select>
      상위 <input id="ds-limit" type="number" value="100" min="1" max="10000" style="width:80px"> 행
      <button class="btn" onclick="runDs()">실행</button>
      <span class="mut" id="ds-meta"></span>
    </div>
    <textarea id="ds-sql" class="sqlbox" placeholder="SELECT ...  (연결 확인/미리보기용 — 상위 N행만 반환됩니다)"></textarea>
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
// task_id → phases 배열 매핑(처리중 + 이력에서 채운다). 단계 타임라인 모달이 참조한다.
const phaseMap = {};
function showPhases(id){ showPhasesModal(id + ' · 단계별 진행/소요', renderPhases(phaseMap[id])); }
const phaseLink = (id,label)=>`<a class="lnk" onclick="showPhases('${id}');return false">${label}</a>`;
// Task ID → 쿼리 정보 매핑/모달. 행 클릭 시 SELECT 와 (있으면) INSERT 를 함께 보여준다.
// 값은 {exec_mode, sub_query, staging_ddl, insert_sql} 형태.
const sqlMap = {};
// 적재 방식에 맞춰 SELECT / STAGING / INSERT 섹션을 하나의 텍스트로 조립한다.
function composeSql(q){
  if(!q) return '(쿼리문 없음)';
  const parts = [];
  if(q.exec_mode) parts.push(`-- 실행 방식: ${q.exec_mode}`);
  if(q.exec_mode === 'statement'){
    // statement 모드: sub_query 자체가 INSERT ... SELECT (대상 DB에서 그대로 실행)
    if(q.sub_query) parts.push(`-- SELECT + INSERT (대상 DB에서 실행)\\n${q.sub_query}`);
  } else {
    // copy / stage_insert: SELECT 는 sub_query
    if(q.sub_query) parts.push(`-- SELECT (Impala 읽기)\\n${q.sub_query}`);
    if(q.staging_ddl) parts.push(`-- STAGING DDL (Greenplum TEMP)\\n${q.staging_ddl}`);
    if(q.insert_sql) parts.push(`-- INSERT (staging → target)\\n${q.insert_sql}`);
  }
  return parts.join('\\n\\n') || '(쿼리문 없음)';
}
// SQL 모달: 이력 탭은 sqlMap(이력 응답에 포함된 SQL)을 쓰고, 처리중 task 는 목록 응답에
// SQL 이 없으므로 클릭 시점에 /tasks/{id}/detail 을 조회해 채운다(한 번 받으면 캐시).
async function showSql(id){
  if(!sqlMap[id]){
    try{
      const t = await getJSON(`/tasks/${id}/detail`);
      if(t && t.sub_query!==undefined)
        sqlMap[id] = {exec_mode:t.exec_mode, sub_query:t.sub_query,
                      staging_ddl:t.staging_ddl, insert_sql:t.insert_sql};
    }catch(_e){ /* 조회 실패 시 아래에서 '(쿼리문 없음)' 으로 표기 */ }
  }
  showTextModal(id, composeSql(sqlMap[id]));
}
const taskLink = id => `<a class="lnk" onclick="showSql('${id}');return false">${esc(id)}</a>`;

// task 취소(협력적): 실행 중이면 다음 안전 지점에서 CANCELLED 로 마무리된다.
async function cancelTask(id){
  if(!confirm('task ' + id + ' 을(를) 취소할까요?')) return;
  try{ await postJSON(`/tasks/${id}/cancel`); }
  catch(e){ alert('취소 실패: ' + e.message); }
  refresh();
}
// 액션 버튼: 아직 끝나지 않은(QUEUED/READING/WRITING) task 에만 취소를 노출.
const ACTIVE_TASK = ["QUEUED","READING","WRITING"];
const taskCancelBtn = r => ACTIVE_TASK.includes(r.status)
  ? `<button class="btn danger" onclick="cancelTask('${r.task_id}')">취소</button>` : fmt(null);

async function loadTasks(){
  const [d, m] = await Promise.all([
    getJSON("/tasks?status=active"),
    getJSON("/metrics"),
  ]);
  (d.tasks||[]).forEach(r=>{ phaseMap[r.task_id] = r.phases||[]; });
  const phLabel = r => r.current_phase
    ? `<span class="phdot">${fmt((phaseOf(r.phases,r.current_phase)||{}).label || r.current_phase)}</span>`
    : fmt(null);
  const cols = [
    {t:"작업 ID", f:r=>`<code>${fmt(r.job_id)}</code>`},
    {t:"Task ID", f:r=>taskLink(r.task_id)},
    {t:"사용자", k:"username"},
    {t:"상태", f:r=>pill(r.status)},
    {t:"현재 단계", f:phLabel},
    {t:"단계", f:r=>phaseLink(r.task_id, '타임라인')},
    {t:"실행 방식", k:"exec_mode"},
    {t:"대상 테이블", k:"target_table"},
    {t:"읽은 행수", f:r=>fmtNum(r.rows_read)},
    {t:"적재 행수", f:r=>fmtNum(r.rows_written)},
    {t:"조회완료", f:r=>`<span class="mut">${fmtDate(r.impala_done_at)}</span>`},
    {t:"시작 시간", f:r=>`<span class="mut">${fmtDate(r.started_at)}</span>`},
    {t:"종료 시간", f:r=>`<span class="mut">${fmtDate(r.finished_at)}</span>`},
    {t:"소요 시간", cls:"col-dur", f:r=>dur(r.started_at, r.finished_at)},
    {t:"에러", cls:"col-err", f:r=>r.error?`<span class="err">${esc(r.error)}</span>`:fmt(null)},
    {t:"액션", f:taskCancelBtn},
  ];
  const t = m.tasks || {};
  $("#p-tasks").innerHTML =
    `<div class="cards">
       <div class="card"><div class="k">보유 Task</div><div class="v">${d.total}</div></div>
       <div class="card"><div class="k">실행중</div><div class="v">${d.running}</div></div>
       <div class="card"><div class="k">활성(대기+실행)</div><div class="v">${d.active}</div></div>
       <div class="card"><div class="k">동시 처리</div><div class="v">${concBar(t.active, t.max)}</div></div>
       <div class="card"><div class="k">CPU</div><div class="v">${m.cpu_percent}%</div></div>
       <div class="card"><div class="k">MEM</div><div class="v">${m.memory.percent}%</div></div>
       <div class="card"><div class="k">DISK</div><div class="v">${m.disk.percent}%</div></div>
     </div>` + table(cols, d.tasks);
}
async function loadHist(){
  const d = await getJSON(`/history?limit=${HIST_LIMIT}&offset=${histOffset}${histFilterQS()}`);
  $("#hist-filter").style.display = d.enabled ? 'flex' : 'none';
  if(!d.enabled){
    $("#hist-body").innerHTML = `<p class="mut">이력 DB(history.db_dsn)가 설정되지 않았습니다. ` +
      `PostgreSQL 설정 시 과거 task 실행 이력이 표시됩니다.</p>`;
    return;
  }
  histTotal = d.total || 0;
  (d.rows||[]).forEach(r=>{
    if(r.sub_query || r.insert_sql || r.staging_ddl)
      sqlMap[r.task_id] = {exec_mode:r.exec_mode, sub_query:r.sub_query,
                           staging_ddl:r.staging_ddl, insert_sql:r.insert_sql};
    phaseMap[r.task_id] = r.phases||[];
  });
  const cols = [
    {t:"작업 ID", f:r=>`<code>${fmt(r.job_id)}</code>`},
    {t:"Task ID", f:r=>taskLink(r.task_id)},
    {t:"사용자", k:"username"},
    {t:"상태", f:r=>pill(r.status)},
    {t:"단계", f:r=>(r.phases&&r.phases.length)?phaseLink(r.task_id,'타임라인'):fmt(null)},
    {t:"읽은 행수", f:r=>fmtNum(r.rows_read)},
    {t:"적재 행수", f:r=>fmtNum(r.rows_written)},
    {t:"조회완료", f:r=>`<span class="mut">${fmtDate(r.impala_done_at)}</span>`},
    {t:"시작 시간", f:r=>`<span class="mut">${fmtDate(r.started_at)}</span>`},
    {t:"종료 시간", f:r=>`<span class="mut">${fmtDate(r.finished_at)}</span>`},
    {t:"소요 시간", cls:"col-dur", f:r=>dur(r.started_at, r.finished_at)},
    {t:"에러", cls:"col-err", f:r=>r.error?`<span class="err">${esc(r.error)}</span>`:fmt(null)},
  ];
  const n = d.rows ? d.rows.length : 0;
  $("#hist-body").innerHTML = table(cols, d.rows) + pagerHtml(n);
}

// 데이터소스 탭 초기화(1회): 소스 select 를 채운다. 결과 영역은 실행 시에만 갱신
// (자동 갱신 주기마다 입력/결과가 지워지지 않도록 이 로더는 재호출 시 아무것도 안 한다).
let dsInit = false;
async function loadDs(){
  if(dsInit) return;
  const d = await getJSON("/datasources");
  $("#ds-name").innerHTML = (d.datasources||[]).map(s=>
    `<option value="${s.name}" ${s.configured?'':'disabled'}>${s.name}${s.configured?'':' (미구성)'}</option>`
  ).join('');
  dsInit = true;
}
function runDs(){ return runDatasourceQuery(); }

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
// 그외 정보 탭의 각 key 에 대한 한 줄 설명. tasks.<status> 키는 동적이라 별도 처리한다.
const infoDesc = {
  version: "애플리케이션 버전",
  executor_id: "이 executor 인스턴스 식별자(host:port)",
  source_type: "소스 엔진(impala 전용) — task 읽기(SELECT)가 사용",
  self_report: "자기 상태 self-report 사용 여부",
  advertise_url: "self-report 에 기록하는 자기 base URL(HA URL 키 부하 뷰)",
  max_concurrent_tasks: "동시에 실행하는 task 상한(0=무제한)",
  active_tasks: "현재 실행 중(READING/WRITING) task 수",
  queued_tasks: "대기 중(QUEUED) task 수",
  started_at: "이 executor 기동 시각",
  uptime_seconds: "기동 후 경과 시간(초)",
  tasks_total: "보유 중인 전체 task 수",
};
function infoDescOf(k){ return k.startsWith("tasks.") ? "상태별 task 수" : (infoDesc[k] || ""); }
async function loadInfo(){
  const d = await getJSON("/info");
  const rows = Object.entries(d).filter(([k,v])=>typeof v!=="object")
    .map(([k,v])=>({key:k,value:v}));
  const byStatus = Object.entries(d.tasks_by_status||{}).map(([k,v])=>({key:"tasks."+k,value:v}));
  const cols = [{t:"key",k:"key"},
    {t:"value", f:r=> r.key.endsWith("_at") ? fmtDate(r.value) : fmt(String(r.value))},
    {t:"설명", f:r=>`<div class="desc">${fmt(infoDescOf(r.key))}</div>`}];
  $("#p-info").innerHTML = table(cols, rows.concat(byStatus));
}
getJSON("/info").then(d=>{ $("#hdr").textContent =
  `id=${d.executor_id} · max=${d.max_concurrent_tasks} · self_report=${d.self_report} · v${d.version}`; });
initDashboard({tasks:loadTasks, hist:loadHist, ds:loadDs, conf:loadConf, info:loadInfo}, "tasks");
</script>
</body>
</html>"""
