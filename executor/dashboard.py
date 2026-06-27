"""executor 모니터링 대시보드(인라인 HTML + vanilla JS, npm/빌드 불필요).

remote mode 에서 각 executor 프로세스가 자신의 / 에 단일 HTML 을 서빙하고,
브라우저가 /tasks·/metrics·/history·/config·/info 를 폴링해 탭을 갱신한다.
local mode 에서는 executor 프로세스가 따로 없으므로 coordinator 대시보드만 보인다.
"""

from __future__ import annotations

import re


def mask_dsn(dsn: str | None) -> str:
    """DSN 의 비밀번호를 마스킹: scheme://user:pass@host → scheme://user:***@host."""
    if not dsn:
        return ""
    return re.sub(r"://([^:/@]+):([^@]+)@", r"://\1:***@", dsn)


def masked_config(settings) -> list[dict]:
    """환경설정 탭용 (section, key, value) 행 목록. 비밀값은 마스킹한다."""
    rows: list[tuple[str, str, object]] = [
        ("app", "name", settings.app_name),
        ("app", "debug", settings.debug),
        ("executor", "host", settings.executor_host),
        ("executor", "self_report", settings.executor_self_report),
        ("executor", "status_table", settings.executor_status_table),
        ("executor", "status_interval_s", settings.executor_status_interval_s),
        ("executor", "max_concurrent_tasks", settings.executor_max_concurrent_tasks),
        ("history", "db_dsn", mask_dsn(settings.history_db_dsn)),
        ("history", "task_table", settings.task_history_table),
        ("monitor", "disk_path", settings.monitor_disk_path),
        ("impala", "host", settings.impala_host or "(미설정→Mock)"),
        ("impala", "port", settings.impala_port),
        ("impala", "database", settings.impala_database),
        ("impala", "auth_mechanism", settings.impala_auth_mechanism),
        ("impala", "kerberos_service_name", settings.impala_kerberos_service_name),
        ("impala", "use_ssl", settings.impala_use_ssl),
        ("impala", "ca_cert", settings.impala_ca_cert),
        ("impala", "user", settings.impala_user),
        ("impala", "password", "***" if settings.impala_password else ""),
        ("greenplum", "dsn", mask_dsn(settings.greenplum_dsn)),
        ("greenplum", "copy_batch_size", settings.copy_batch_size),
        ("logging", "level", settings.log_level),
        ("logging", "dir", str(settings.log_dir)),
        ("logging", "rolling.backup_count", settings.log_rolling_backup_count),
    ]
    return [{"section": s, "key": k, "value": v} for s, k, v in rows]


DASHBOARD_HTML = """<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Query Executor 모니터링</title>
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
  .s-DONE{color:var(--ok);} .s-READING,.s-WRITING,.s-QUEUED{color:var(--acc);}
  .s-FAILED{color:var(--bad);} .s-CANCELLED{color:var(--warn);}
  .ok{color:var(--ok);} .bad{color:var(--bad);}
  .bar { background:#eaeef2; border-radius:6px; height:8px; width:120px; display:inline-block;
         vertical-align:middle; overflow:hidden; }
  .bar > i { display:block; height:100%; background:var(--acc); }
  .mut{color:var(--mut);} .err{color:var(--bad);} code{color:var(--acc);}
  .sec { color:var(--warn); font-weight:600; }
  .pager { display:flex; gap:10px; align-items:center; margin-top:10px; }
  .pager button { background:var(--panel); color:var(--fg); border:1px solid var(--line);
                  padding:6px 12px; border-radius:6px; cursor:pointer; }
  .pager button:disabled { color:var(--mut); cursor:default; opacity:.5; }
</style>
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
  <button data-tab="conf">환경설정</button>
  <button data-tab="info">그외 정보</button>
</div>
<main>
  <section class="panel active" id="p-tasks"></section>
  <section class="panel" id="p-hist"></section>
  <section class="panel" id="p-conf"></section>
  <section class="panel" id="p-info"></section>
</main>
<script>
const $ = s => document.querySelector(s);
let active = "tasks";
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

async function loadTasks(){
  const [d, m] = await Promise.all([
    getJSON("/tasks?status=active"),
    getJSON("/metrics"),
  ]);
  const cols = [
    {t:"작업 ID", f:r=>`<code>${fmt(r.job_id)}</code>`},
    {t:"Task ID", f:r=>`<code>${fmt(r.task_id)}</code>`},
    {t:"사용자", k:"username"},
    {t:"상태", f:r=>pill(r.status)},
    {t:"실행 방식", k:"exec_mode"},
    {t:"대상 테이블", k:"target_table"},
    {t:"적재 행수", f:r=>fmtNum(r.rows_written)},
    {t:"시작 시간", f:r=>`<span class="mut">${fmtDate(r.started_at)}</span>`},
    {t:"종료 시간", f:r=>`<span class="mut">${fmtDate(r.finished_at)}</span>`},
    {t:"소요 시간", f:r=>dur(r.started_at, r.finished_at)},
    {t:"에러", f:r=>r.error?`<span class="err">${r.error}</span>`:fmt(null)},
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
let histOffset = 0; const HIST_LIMIT = 50; let histTotal = 0;
async function loadHist(){
  const d = await getJSON(`/history?limit=${HIST_LIMIT}&offset=${histOffset}`);
  if(!d.enabled){
    $("#p-hist").innerHTML = `<p class="mut">이력 DB(history.db_dsn)가 설정되지 않았습니다. ` +
      `PostgreSQL 설정 시 과거 task 실행 이력이 표시됩니다.</p>`;
    return;
  }
  histTotal = d.total || 0;
  const cols = [
    {t:"기록 시간", f:r=>`<span class="mut">${fmtDate(r.recorded_at)}</span>`},
    {t:"작업 ID", f:r=>`<code>${fmt(r.job_id)}</code>`},
    {t:"Task ID", f:r=>`<code>${fmt(r.task_id)}</code>`},
    {t:"사용자", k:"username"},
    {t:"상태", f:r=>pill(r.status)},
    {t:"적재 행수", f:r=>fmtNum(r.rows_written)},
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

async function loadConf(){
  const d = await getJSON("/config");
  const cols = [
    {t:"section", f:r=>`<span class="sec">${r.section}</span>`},
    {t:"key", k:"key"},
    {t:"value", f:r=>fmt(String(r.value))},
  ];
  $("#p-conf").innerHTML = table(cols, d.config);
}
async function loadInfo(){
  const d = await getJSON("/info");
  const rows = Object.entries(d).filter(([k,v])=>typeof v!=="object")
    .map(([k,v])=>({key:k,value:v}));
  const byStatus = Object.entries(d.tasks_by_status||{}).map(([k,v])=>({key:"tasks."+k,value:v}));
  const cols = [{t:"key",k:"key"},
    {t:"value", f:r=> r.key.endsWith("_at") ? fmtDate(r.value) : fmt(String(r.value))}];
  $("#p-info").innerHTML = table(cols, rows.concat(byStatus));
}
const loaders = {tasks:loadTasks, hist:loadHist, conf:loadConf, info:loadInfo};
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
  `id=${d.executor_id} · max=${d.max_concurrent_tasks} · v${d.version}`; });
refresh();
setInterval(()=>{ if(!document.hidden) refresh(); }, 3000);
</script>
</body>
</html>"""
