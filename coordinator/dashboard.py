"""coordinator 모니터링 대시보드(인라인 HTML + vanilla JS, npm/빌드 불필요).

이 모듈은 별도 프런트엔드 빌드 없이 단일 HTML 문자열(DASHBOARD_HTML)로 모니터링 UI 를
제공한다. 서버는 '/' 경로에서 이 HTML 을 그대로 서빙하고, 브라우저의 vanilla JS 가
/jobs·/cluster·/config·/info·/history 등의 JSON API 를 주기적으로 폴링해 각 탭(처리중인 쿼리,
실행 이력, Executor, 환경설정, 그외 정보)을 갱신한다.

Python 쪽 코드는 두 가지뿐이다.
  - mask_dsn(): DSN 문자열의 비밀번호 부분을 가린다.
  - masked_config(): 환경설정 탭에 뿌릴 (section, key, value) 행 목록을 만들며, 비밀값은 마스킹한다.
나머지(DASHBOARD_HTML)는 브라우저로 그대로 전달되는 HTML/CSS/JS 코드 문자열이다.
"""

from __future__ import annotations

import re


def mask_dsn(dsn: str | None) -> str:
    """DSN 의 비밀번호를 마스킹한다: scheme://user:pass@host → scheme://user:***@host.

    접속 문자열을 화면/응답에 노출할 때 자격증명이 새지 않도록, 'user:password@' 패턴의
    비밀번호 부분만 '***' 로 치환한다. 사용자명/호스트 등 나머지는 그대로 둔다. 값이 비어
    있으면 빈 문자열을 돌려준다.

    Args:
        dsn: 마스킹할 DSN 문자열(없을 수 있음).

    Returns:
        비밀번호가 가려진 DSN 문자열(입력이 없으면 "").
    """
    if not dsn:
        return ""
    return re.sub(r"://([^:/@]+):([^@]+)@", r"://\1:***@", dsn)


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
        ("store", "backend", settings.store_backend,
         "Job 저장소: memory(단일) | postgres(멀티 coordinator 공유)"),
        ("store", "table", settings.store_table, "공유 store 테이블명(postgres backend)"),
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
        ("executor", "status_table", settings.executor_status_table, "executor self-report 상태 테이블"),
        ("executor", "status_interval_s", settings.executor_status_interval_s,
         "executor self-report 주기(초)"),
        ("executor", "max_concurrent_tasks", settings.executor_max_concurrent_tasks,
         "executor 1대가 동시에 실행하는 task 수(0=무제한)"),
        ("executor", "executors", ", ".join(settings.executors) or "(없음)",
         "디스패치 대상 executor 베이스 URL 목록"),
        ("impala", "host", settings.impala_host or "(미설정→Mock)",
         "Impala(소스) 호스트. 미설정 시 MockBackend"),
        ("impala", "port", settings.impala_port, "Impala 포트(HiveServer2)"),
        ("impala", "database", settings.impala_database, "Impala 기본 데이터베이스"),
        ("impala", "auth_mechanism", settings.impala_auth_mechanism, "Impala 인증 방식(GSSAPI=Kerberos)"),
        ("impala", "kerberos_service_name", settings.impala_kerberos_service_name,
         "Kerberos 서비스 이름"),
        ("impala", "use_ssl", settings.impala_use_ssl, "Impala 접속 TLS 사용 여부"),
        ("impala", "ca_cert", settings.impala_ca_cert, "TLS 검증용 CA 인증서 경로"),
        ("impala", "user", settings.impala_user, "Impala 사용자(LDAP 등)"),
        ("impala", "password", "***" if settings.impala_password else "",
         "Impala 비밀번호(설정 시 *** 로 마스킹)"),
        ("greenplum", "dsn", mask_dsn(settings.greenplum_dsn),
         "Greenplum(타깃) 적재 DSN. 미설정 시 MockBackend"),
        ("greenplum", "copy_batch_size", settings.copy_batch_size, "COPY 배치 크기(행)"),
        ("logging", "level", settings.log_level, "메인 로그 레벨(이 레벨 이상 기록)"),
        ("logging", "dir", str(settings.log_dir), "로그 디렉터리(일 단위 롤링)"),
        ("logging", "rolling.backup_count", settings.log_rolling_backup_count,
         "로그 보관 일수(초과분 자동 삭제)"),
    ]
    return [{"section": s, "key": k, "value": v, "desc": d} for s, k, v, d in rows]


# 대시보드 페이지 전체(HTML/CSS/JS)를 담은 문자열. '/' 핸들러가 이 값을 그대로 응답으로 내보낸다.
# 주의: 아래 문자열 내부는 브라우저로 전송되는 코드이므로 Python 주석을 끼워 넣지 말 것.
DASHBOARD_HTML = """<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Query Coordinator 모니터링</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Roboto+Condensed:wght@400;600&display=swap" rel="stylesheet">
<style>
  :root { --bg:#ffffff; --panel:#ffffff; --line:#e1e4e8; --fg:#1f2328; --mut:#6e7781;
          --ok:#1a7f37; --bad:#cf222e; --warn:#9a6700; --acc:#0969da; }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--fg);
         font:14px/1.5 "Roboto Condensed",-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Noto Sans KR",sans-serif; }
  header { padding:12px 20px; border-bottom:1px solid var(--line); display:flex;
           align-items:center; gap:16px; flex-wrap:wrap; }
  header h1 { font-size:16px; margin:0; }
  header .meta { color:var(--mut); font-size:12px; }
  .tabs { display:flex; gap:4px; padding:10px 20px 0; flex-wrap:wrap; }
  .tabs button { background:var(--panel); color:var(--mut); border:1px solid var(--line);
                 border-bottom:none; padding:8px 14px; border-radius:6px 6px 0 0; cursor:pointer; }
  .tabs button.active { color:var(--fg); background:#ddf4ff; border-color:var(--acc); }
  main { padding:16px 20px; }
  .panel { display:none; }
  .panel.active { display:block; }
  .cards { display:flex; gap:12px; flex-wrap:wrap; margin-bottom:14px; }
  .card { background:#f6f8fa; border:1px solid var(--line); border-radius:8px;
          padding:12px 16px; min-width:150px; }
  .card .k { color:var(--mut); font-size:12px; }
  .card .v { font-size:20px; font-weight:600; }
  table { width:100%; border-collapse:collapse; background:var(--panel);
          border:1px solid var(--line); border-radius:8px; overflow:hidden; }
  th,td { text-align:center; padding:8px 10px; border-bottom:1px solid var(--line);
          font-size:13px; white-space:nowrap; }
  th { color:var(--mut); font-weight:600; background:#f6f8fa; position:sticky; top:0; }
  tr:hover td { background:#f6f8fa; }
  .pill { padding:1px 8px; border-radius:10px; font-size:12px; font-weight:600; }
  .s-DONE{color:var(--ok);} .s-RUNNING,.s-SPLITTING,.s-PENDING{color:var(--acc);}
  .s-FAILED{color:var(--bad);} .s-CANCELLED,.s-PARTIAL{color:var(--warn);}
  .ok{color:var(--ok);} .bad{color:var(--bad);}
  .bar { background:#eaeef2; border-radius:6px; height:8px; width:120px; display:inline-block;
         vertical-align:middle; overflow:hidden; }
  .bar > i { display:block; height:100%; background:var(--acc); }
  .mut{color:var(--mut);} .err{color:var(--bad);} code{color:var(--acc);}
  .desc { text-align:left; color:var(--mut); white-space:normal; max-width:560px; }
  .sec { color:var(--warn); font-weight:600; }
  .pager { display:flex; gap:10px; align-items:center; margin-top:10px; }
  .pager button { background:var(--panel); color:var(--fg); border:1px solid var(--line);
                  padding:6px 12px; border-radius:6px; cursor:pointer; }
  .pager button:disabled { color:var(--mut); cursor:default; opacity:.5; }
  .lnk { color:var(--acc); cursor:pointer; text-decoration:none; }
  .lnk:hover { text-decoration:underline; }
  .modal { display:none; position:fixed; inset:0; background:rgba(0,0,0,.4);
           align-items:center; justify-content:center; z-index:50; }
  .modal-box { background:var(--panel); border:1px solid var(--line); border-radius:8px;
               width:min(900px,90%); max-height:80%; overflow:auto; padding:16px; }
  .modal-head { display:flex; justify-content:space-between; align-items:center;
                gap:20px; margin-bottom:10px; }
  .modal-head button { background:none; border:none; color:var(--mut); font-size:18px; cursor:pointer; }
  #modal-sql { white-space:pre-wrap; word-break:break-all; background:#f6f8fa;
               border:1px solid var(--line); border-radius:6px; padding:12px;
               font:13px/1.55 ui-monospace,Menlo,Consolas,monospace; color:var(--fg); }
</style>
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
  <button data-tab="conf">환경설정</button>
  <button data-tab="info">그외 정보</button>
</div>
<main>
  <section class="panel active" id="p-jobs"></section>
  <section class="panel" id="p-hist"></section>
  <section class="panel" id="p-exec"></section>
  <section class="panel" id="p-conf"></section>
  <section class="panel" id="p-info"></section>
</main>
<div class="modal" id="modal" onclick="if(event.target===this)closeModal()">
  <div class="modal-box">
    <div class="modal-head"><b id="modal-title"></b><button onclick="closeModal()">✕</button></div>
    <pre id="modal-sql"></pre>
  </div>
</div>
<script>
const $ = s => document.querySelector(s);
let active = "jobs";
const fmt = v => (v===null||v===undefined||v==="") ? '<span class="mut">-</span>' : v;
const fmtNum = v => (v===null||v===undefined||v==="") ? '<span class="mut">-</span>' : Number(v).toLocaleString('en-US');
const pill = s => `<span class="pill s-${s}">${s}</span>`;
const pad = n => String(n).padStart(2,'0');
function fmtDate(s){
  if(!s) return '<span class="mut">-</span>';
  const d = new Date(s); if(isNaN(d)) return s;
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} `+
         `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}
function dur(start, end){
  if(!start) return '<span class="mut">-</span>';
  const s = new Date(start), e = end ? new Date(end) : new Date();
  let ms = e - s; if(isNaN(ms)||ms<0) return '<span class="mut">-</span>';
  const sec = Math.floor(ms/1000), h=Math.floor(sec/3600),
        m=Math.floor((sec%3600)/60), ss=sec%60;
  return (h?h+'h ':'')+(m||h?m+'m ':'')+ss+'s';
}
// 작업 ID → 쿼리문 매핑/모달
const sqlMap = {};
function showSql(id){
  $("#modal-title").textContent = id;
  $("#modal-sql").textContent = sqlMap[id] || '(쿼리문 없음)';
  $("#modal").style.display = 'flex';
}
function closeModal(){ $("#modal").style.display = 'none'; }
const jobLink = id => `<a class="lnk" onclick="showSql('${id}');return false">${id}</a>`;
const bar = p => `<span class="bar"><i style="width:${p||0}%"></i></span> ${p||0}%`;
function concBar(active, max){
  if(active===null||active===undefined) return '<span class="mut">-</span>';
  if(!max || max<=0) return `${active} <span class="mut">(무제한)</span>`;
  const pct = Math.min(100, Math.round(active/max*100));
  return `<span class="bar"><i style="width:${pct}%"></i></span> ${active}/${max}`;
}

function table(cols, rows){
  let h = "<table><thead><tr>" + cols.map(c=>`<th>${c.t}</th>`).join("") + "</tr></thead><tbody>";
  if(!rows.length) h += `<tr><td colspan="${cols.length}" class="mut">데이터 없음</td></tr>`;
  for(const r of rows){ h += "<tr>" + cols.map(c=>`<td>${c.f?c.f(r):fmt(r[c.k])}</td>`).join("") + "</tr>"; }
  return h + "</tbody></table>";
}
async function getJSON(u){ const r = await fetch(u); return r.json(); }

async function loadJobs(){
  const d = await getJSON("/jobs?status=active&limit=0");
  (d.jobs||[]).forEach(r=>{ if(r.original_sql) sqlMap[r.job_id]=r.original_sql; });
  const cols = [
    {t:"작업 ID", f:r=>jobLink(r.job_id)},
    {t:"사용자", k:"username"},
    {t:"상태", f:r=>pill(r.status)},
    {t:"진행률", f:r=>bar(r.progress_percent)},
    {t:"완료/전체", f:r=>`${r.completed}/${r.total}`},
    {t:"적재 행수", f:r=>fmtNum(r.total_rows_written)},
    {t:"실행 방식", k:"exec_mode"},
    {t:"대상 테이블", k:"target_table"},
    {t:"시작 시간", f:r=>`<span class="mut">${fmtDate(r.started_at)}</span>`},
    {t:"종료 시간", f:r=>`<span class="mut">${fmtDate(r.finished_at)}</span>`},
    {t:"소요 시간", f:r=>dur(r.started_at, r.finished_at)},
  ];
  $("#p-jobs").innerHTML =
    `<div class="cards">
       <div class="card"><div class="k">총 작업</div><div class="v">${d.total}</div></div>
       <div class="card"><div class="k">실행중</div><div class="v">${d.running}</div></div>
       <div class="card"><div class="k">활성(대기+실행)</div><div class="v">${d.active}</div></div>
     </div>` + table(cols, d.jobs);
}
let histOffset = 0; const HIST_LIMIT = 50; let histTotal = 0;
async function loadHist(){
  const d = await getJSON(`/history?limit=${HIST_LIMIT}&offset=${histOffset}`);
  if(!d.enabled){
    $("#p-hist").innerHTML = `<p class="mut">이력 DB(history.db_dsn)가 설정되지 않았습니다. ` +
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
    {t:"소요 시간", f:r=>dur(r.started_at, r.finished_at)},
    {t:"에러", f:r=>r.error?`<span class="err">${r.error}</span>`:fmt(null)},
  ];
  const n = d.rows ? d.rows.length : 0;
  const from = histTotal ? histOffset + 1 : 0;
  const to = histOffset + n;
  const pager = `<div class="pager">
      <button onclick="histPrev()" ${histOffset<=0?'disabled':''}>← 이전</button>
      <span class="mut">${from}–${to} / ${histTotal}</span>
      <button onclick="histNext()" ${to>=histTotal?'disabled':''}>다음 →</button>
    </div>`;
  $("#p-hist").innerHTML = table(cols, d.rows) + pager;
}
function histPrev(){ histOffset = Math.max(0, histOffset - HIST_LIMIT); loadHist(); }
function histNext(){ if(histOffset + HIST_LIMIT < histTotal){ histOffset += HIST_LIMIT; loadHist(); } }

async function loadExec(){
  const d = await getJSON("/cluster");
  const cm = d.coordinator.metrics;
  const cards = `<div class="cards">
    <div class="card"><div class="k">Coordinator CPU</div><div class="v">${cm.cpu_percent}%</div></div>
    <div class="card"><div class="k">MEM</div><div class="v">${cm.memory.percent}%</div></div>
    <div class="card"><div class="k">DISK</div><div class="v">${cm.disk.percent}%</div></div>
    <div class="card"><div class="k">Executor</div><div class="v">${d.executors_summary.healthy}/${d.executors_summary.total}</div></div>
   </div>`;
  const cols = [
    {t:"Executor", f:r=>`<code>${fmt(r.executor_id||r.executor_url)}</code>`},
    {t:"Healthy", f:r=>r.healthy?'<span class="ok">● UP</span>':'<span class="bad">● DOWN</span>'},
    {t:"CPU%", k:"cpu_percent"},
    {t:"MEM%", k:"memory_percent"},
    {t:"DISK%", k:"disk_percent"},
    {t:"동시 처리", f:r=>concBar(r.active_tasks, r.max_concurrent_tasks)},
    {t:"Last Seen", f:r=>`<span class="mut">${fmtDate(r.updated_at||r.last_checked)}</span>`},
    {t:"Error", f:r=>r.error?`<span class="err">${r.error}</span>`:fmt(null)},
  ];
  $("#p-exec").innerHTML = cards + table(cols, d.executors);
}
async function loadConf(){
  const d = await getJSON("/config");
  const cols = [
    {t:"section", f:r=>`<span class="sec">${r.section}</span>`},
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
const loaders = {jobs:loadJobs, hist:loadHist, exec:loadExec, conf:loadConf, info:loadInfo};
async function refresh(){
  try{ await loaders[active](); $("#upd").textContent = "갱신: " + new Date().toLocaleTimeString();}
  catch(e){ $("#upd").textContent = "갱신 실패: " + e; }
}
document.querySelectorAll(".tabs button").forEach(b=>b.onclick=()=>{
  document.querySelectorAll(".tabs button").forEach(x=>x.classList.remove("active"));
  document.querySelectorAll(".panel").forEach(x=>x.classList.remove("active"));
  b.classList.add("active"); $("#p-"+b.dataset.tab).classList.add("active");
  active = b.dataset.tab; refresh();
});
getJSON("/info").then(d=>{ $("#hdr").textContent =
  `id=${d.coordinator_id} · mode=${d.executor_mode} · store=${d.store_backend} · v${d.version}`; });
refresh();
setInterval(()=>{ if(!document.hidden) refresh(); }, 3000);
</script>
</body>
</html>"""
