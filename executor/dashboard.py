"""executor 모니터링 대시보드(인라인 HTML + vanilla JS, npm/빌드 불필요).

remote mode 에서 각 executor 프로세스가 자신의 / 에 단일 HTML 을 서빙하고,
브라우저가 /tasks·/metrics·/history·/config·/info 를 폴링해 탭을 갱신한다.
local mode 에서는 executor 프로세스가 따로 없으므로 coordinator 대시보드만 보인다.
"""

from __future__ import annotations

import re


def mask_dsn(dsn: str | None) -> str:
    """DSN 문자열의 비밀번호를 마스킹한다.

    ``scheme://user:pass@host`` 형태의 자격증명 중 비밀번호 부분만 ``***`` 로 치환해
    ``scheme://user:***@host`` 로 만든다. 대시보드 환경설정 탭처럼 DSN 을 화면에 그대로
    노출해야 할 때, 비밀번호가 새지 않도록 가린다. dsn 이 비어 있으면 빈 문자열을 반환한다.

    인자:
        dsn: 마스킹할 DSN(없을 수 있음).

    반환:
        비밀번호가 가려진 DSN 문자열(자격증명이 없으면 원본 그대로).
    """
    if not dsn:
        return ""
    return re.sub(r"://([^:/@]+):([^@]+)@", r"://\1:***@", dsn)


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
        ("history", "db_dsn", mask_dsn(settings.history_db_dsn),
         "task 이력/공유 상태 DB DSN(미설정 시 이력 비활성)"),
        ("history", "task_table", settings.task_history_table, "task 실행 이력 테이블"),
        ("monitor", "disk_path", settings.monitor_disk_path, "디스크 사용량 측정 경로"),
        ("impala", "host", settings.impala_host or "(미설정→Mock)",
         "Impala(소스) 호스트. 미설정 시 MockBackend"),
        ("impala", "port", settings.impala_port, "Impala 포트(HiveServer2)"),
        ("impala", "database", settings.impala_database, "Impala 기본 데이터베이스"),
        ("impala", "auth_mechanism", settings.impala_auth_mechanism,
         "Impala 인증 방식(LDAP 기본 | GSSAPI=Kerberos | PLAIN | NOSASL)"),
        ("impala", "kerberos_service_name", settings.impala_kerberos_service_name,
         "Kerberos 서비스 이름"),
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
        ("logging", "level", settings.log_level, "메인 로그 레벨(이 레벨 이상 기록)"),
        ("logging", "dir", str(settings.log_dir), "로그 디렉터리(일 단위 롤링)"),
        ("logging", "rolling.backup_count", settings.log_rolling_backup_count,
         "로그 보관 일수(초과분 자동 삭제)"),
    ]
    return [{"section": s, "key": k, "value": v, "desc": d} for s, k, v, d in rows]


DASHBOARD_HTML = """<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Query Executor 모니터링</title>
<link rel="stylesheet" href="/assets/fonts.css">
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
  .desc { text-align:left; color:var(--mut); white-space:normal; max-width:560px; }
  .sec { color:var(--warn); font-weight:600; }
  .pager { display:flex; gap:10px; align-items:center; margin-top:10px; }
  .pager button { background:var(--panel); color:var(--fg); border:1px solid var(--line);
                  padding:6px 12px; border-radius:6px; cursor:pointer; }
  .pager button:disabled { color:var(--mut); cursor:default; opacity:.5; }
  .btn { background:var(--panel); color:var(--fg); border:1px solid var(--line);
         padding:2px 10px; border-radius:6px; cursor:pointer; font-size:12px; }
  .btn:hover { background:#f6f8fa; }
  .btn.danger { color:var(--bad); border-color:var(--bad); }
  .lnk { color:var(--acc); cursor:pointer; text-decoration:none; }
  .lnk:hover { text-decoration:underline; }
  .modal { display:none; position:fixed; inset:0; background:rgba(0,0,0,.4);
           align-items:center; justify-content:center; z-index:50; }
  .modal-box { background:var(--panel); border:1px solid var(--line); border-radius:8px;
               width:min(900px,90%); max-height:80%; overflow:auto; padding:16px; }
  /* 타임라인(단계) 팝업만 기본 대비 20% 넓게(900→1080px). SQL 팝업(#modal)은 그대로. */
  #pmodal .modal-box { width:min(1080px,90%); }
  .modal-head { display:flex; justify-content:space-between; align-items:center;
                gap:20px; margin-bottom:10px; }
  .modal-head button { background:none; border:none; color:var(--mut); font-size:18px; cursor:pointer; }
  #modal-sql { white-space:pre-wrap; word-break:break-all; background:#f6f8fa;
               border:1px solid var(--line); border-radius:6px; padding:12px;
               font:13px/1.55 ui-monospace,Menlo,Consolas,monospace; color:var(--fg); }
  .tl td { text-align:left; white-space:nowrap; }
  .gtrack { background:#eaeef2; border-radius:4px; height:10px; width:220px;
            display:inline-block; vertical-align:middle; overflow:hidden; }
  .gtrack > i { display:block; height:100%; background:var(--acc); }
  .gtrack > i.run { background:var(--warn); }
  .phdot { font-size:11px; padding:1px 7px; border-radius:9px; background:#ddf4ff;
           color:var(--acc); font-weight:600; }
  /* 소요 시간 열과 에러 열의 폭 비율을 1:10 으로 고정한다(에러가 소요의 10배). */
  th.col-dur, td.col-dur { width:60px; }
  th.col-err, td.col-err { width:600px; max-width:600px; }
  td.col-err { text-align:left; white-space:normal; word-break:break-word;
               font-family:ui-monospace,Menlo,Consolas,monospace; }
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
// 밀리초를 사람이 읽는 소요시간으로. 1초 미만은 ms, 이상은 h/m/s.
function fmtDur(ms){
  if(ms===null||ms===undefined) return '<span class="mut">-</span>';
  if(ms<1000) return ms+'ms';
  const sec=Math.floor(ms/1000), h=Math.floor(sec/3600),
        m=Math.floor((sec%3600)/60), s=sec%60;
  return (h?h+'h ':'')+(m||h?m+'m ':'')+s+'s';
}
// task_id → phases 배열 매핑(처리중 + 이력에서 채운다). 단계 타임라인 모달이 참조한다.
const phaseMap = {};
// 진행 중(finished_at 없음)인 단계의 경과 ms 는 지금 시각 기준으로 계산한다.
function phaseMs(p){
  if(p.duration_ms!==null && p.duration_ms!==undefined) return p.duration_ms;
  if(!p.started_at) return null;
  const s=new Date(p.started_at); if(isNaN(s)) return null;
  const ms=new Date()-s; return (ms>=0?ms:null);
}
const phaseOf = (phases,name)=>(phases||[]).find(p=>p.name===name);
// 단계 타임라인을 간트형 표로 렌더링. 가장 긴 단계를 100%로 잡아 상대 막대를 그린다.
function renderPhases(phases){
  if(!phases || !phases.length) return '<p class="mut">단계 정보가 아직 없습니다.</p>';
  const durs = phases.map(phaseMs).filter(v=>v!==null&&v!==undefined);
  const max = durs.length ? Math.max(...durs, 1) : 1;
  let h = '<table class="tl"><thead><tr>'+
    ['단계','진행','시작','종료','소요','행수','비고'].map(t=>`<th>${t}</th>`).join('')+
    '</tr></thead><tbody>';
  for(const p of phases){
    const ms = phaseMs(p);
    const running = (p.finished_at===null||p.finished_at===undefined);
    const pct = ms!==null&&ms!==undefined ? Math.max(2, Math.round(ms/max*100)) : 0;
    const bar = `<span class="gtrack"><i class="${running?'run':''}" style="width:${pct}%"></i></span>`;
    let note = '';
    const e = p.extra||{};
    if(e.read_wait_ms!==undefined || e.write_wait_ms!==undefined){
      note = `읽기 ${fmtDur(e.read_wait_ms)} / 쓰기 ${fmtDur(e.write_wait_ms)}`;
      if(e.read_starve_ms) note += ` / Impala대기 ${fmtDur(e.read_starve_ms)}`;
      if(e.finalize_wait_ms!==undefined) note += ` / 서버 ${fmtDur(e.finalize_wait_ms)}`;
      if(e.rows_per_sec) note += ` · ${fmtNum(e.rows_per_sec)}행/s`;
    }
    h += '<tr>'+
      `<td><span class="phdot">${fmt(p.label||p.name)}</span>${running?' <span class="mut">(진행중)</span>':''}</td>`+
      `<td>${bar}</td>`+
      `<td><span class="mut">${fmtDate(p.started_at)}</span></td>`+
      `<td><span class="mut">${fmtDate(p.finished_at)}</span></td>`+
      `<td>${fmtDur(ms)}</td>`+
      `<td>${p.rows!==null&&p.rows!==undefined?fmtNum(p.rows):fmt(null)}</td>`+
      `<td class="mut">${note||'-'}</td>`+
    '</tr>';
  }
  return h+'</tbody></table>';
}
function showPhases(id){
  $("#pmodal-title").textContent = id + ' · 단계별 진행/소요';
  $("#pmodal-body").innerHTML = renderPhases(phaseMap[id]);
  $("#pmodal").style.display = 'flex';
}
function closePhases(){ $("#pmodal").style.display = 'none'; }
const phaseLink = (id,label)=>`<a class="lnk" onclick="showPhases('${id}');return false">${label}</a>`;
function table(cols, rows){
  const cls = c => c.cls ? ` class="${c.cls}"` : "";
  let h = "<table><thead><tr>" + cols.map(c=>`<th${cls(c)}>${c.t}</th>`).join("") + "</tr></thead><tbody>";
  if(!rows.length) h += `<tr><td colspan="${cols.length}" class="mut">데이터 없음</td></tr>`;
  for(const r of rows){ h += "<tr>" + cols.map(c=>`<td${cls(c)}>${c.f?c.f(r):fmt(r[c.k])}</td>`).join("") + "</tr>"; }
  return h + "</tbody></table>";
}
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
function showSql(id){
  $("#modal-title").textContent = id;
  $("#modal-sql").textContent = composeSql(sqlMap[id]);
  $("#modal").style.display = 'flex';
}
function closeModal(){ $("#modal").style.display = 'none'; }
const taskLink = id => `<a class="lnk" onclick="showSql('${id}');return false">${id}</a>`;
async function getJSON(u){ const r = await fetch(u); return r.json(); }
// POST 액션 공용: 실패 시 서버가 준 detail 메시지를 그대로 예외로 올린다.
async function postJSON(u){
  const r = await fetch(u, {method:'POST'});
  let body = null;
  try{ body = await r.json(); }catch(_e){ body = null; }
  if(!r.ok) throw new Error((body&&body.detail) || (r.status+' '+r.statusText));
  return body;
}
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
    {t:"Task ID", f:r=>`<code>${fmt(r.task_id)}</code>`},
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
    {t:"에러", cls:"col-err", f:r=>r.error?`<span class="err">${r.error}</span>`:fmt(null)},
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
let histOffset = 0; const HIST_LIMIT = 50; let histTotal = 0;
async function loadHist(){
  const d = await getJSON(`/history?limit=${HIST_LIMIT}&offset=${histOffset}`);
  if(!d.enabled){
    $("#p-hist").innerHTML = `<p class="mut">이력 DB(history.db_dsn)가 설정되지 않았습니다. ` +
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
    {t:"에러", cls:"col-err", f:r=>r.error?`<span class="err">${r.error}</span>`:fmt(null)},
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
    {t:"설명", f:r=>`<div class="desc">${fmt(r.desc)}</div>`},
  ];
  $("#p-conf").innerHTML = table(cols, d.config);
}
// 그외 정보 탭의 각 key 에 대한 한 줄 설명. tasks.<status> 키는 동적이라 별도 처리한다.
const infoDesc = {
  version: "애플리케이션 버전",
  executor_id: "이 executor 인스턴스 식별자(host:port)",
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
  `id=${d.executor_id} · max=${d.max_concurrent_tasks} · self_report=${d.self_report} · v${d.version}`; });
refresh();
setInterval(()=>{ if(!document.hidden) refresh(); }, 3000);
</script>
</body>
</html>"""
