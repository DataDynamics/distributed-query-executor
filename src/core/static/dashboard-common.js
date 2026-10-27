/* 대시보드 공용 스크립트 — coordinator·executor 두 앱의 인라인 HTML 이 공유한다.
   core/webassets.mount_static 이 /assets/dashboard-common.js 로 서빙한다(에어갭 내장).

   포맷터(fmt/fmtDate/…)·표 렌더러(table)·단계 타임라인(renderPhases)·모달·이력 페이저·
   탭/자동갱신(initDashboard)을 담는다. 페이지 쪽 스크립트는 이 파일이 먼저 로드된 뒤
   loaders(탭별 렌더 함수 맵)를 정의하고 initDashboard(loaders, 기본탭) 를 호출한다.
   페이지 계약(전제): 탭 버튼은 .tabs button[data-tab=X] + 패널 #p-X, 이력 탭 로더는
   loadHist 라는 전역 이름, 모달 DOM 은 #modal(#modal-title/#modal-sql)과
   #pmodal(#pmodal-title/#pmodal-body) 구조를 따른다. */

const $ = s => document.querySelector(s);

// HTML 이스케이프. 서버가 준 임의 문자열(에러 메시지/사용자명/SQL 조각 등)을 innerHTML 로
// 뿌릴 때 마크업으로 해석되거나 스크립트가 주입되지 않도록 반드시 이걸 거친다.
function esc(v){
  return String(v).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
                  .replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}
// 빈 값은 '-' 자리표시자, 값이 있으면 이스케이프해 반환(표 셀 기본 렌더러).
const fmt = v => (v===null||v===undefined||v==="") ? '<span class="mut">-</span>' : esc(v);
const fmtNum = v => (v===null||v===undefined||v==="") ? '<span class="mut">-</span>' : Number(v).toLocaleString('en-US');
const pill = s => `<span class="pill s-${esc(s)}">${esc(s)}</span>`;
const pad = n => String(n).padStart(2,'0');
function fmtDate(s){
  if(!s) return '<span class="mut">-</span>';
  const d = new Date(s); if(isNaN(d)) return esc(s);
  return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())} `+
         `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}
// 시작~종료 경과(종료 전이면 지금까지)를 h/m/s 로. 시작 전이면 '-'.
function dur(start, end){
  if(!start) return '<span class="mut">-</span>';
  const s = new Date(start), e = end ? new Date(end) : new Date();
  let ms = e - s; if(isNaN(ms)||ms<0) return '<span class="mut">-</span>';
  const sec = Math.floor(ms/1000), h=Math.floor(sec/3600),
        m=Math.floor((sec%3600)/60), ss=sec%60;
  return (h?h+'h ':'')+(m||h?m+'m ':'')+ss+'s';
}
// 밀리초를 사람이 읽는 소요시간으로. 1초 미만은 ms, 이상은 h/m/s.
function fmtDur(ms){
  if(ms===null||ms===undefined) return '<span class="mut">-</span>';
  if(ms<1000) return ms+'ms';
  const sec=Math.floor(ms/1000), h=Math.floor(sec/3600),
        m=Math.floor((sec%3600)/60), s=sec%60;
  return (h?h+'h ':'')+(m||h?m+'m ':'')+s+'s';
}
// 동시 처리 게이지: max<=0(무제한)이면 숫자만, 아니면 사용률 막대 + n/max.
function concBar(active, max){
  if(active===null||active===undefined) return '<span class="mut">-</span>';
  if(!max || max<=0) return `${active} <span class="mut">(무제한)</span>`;
  const pct = Math.min(100, Math.round(active/max*100));
  return `<span class="bar"><i style="width:${pct}%"></i></span> ${active}/${max}`;
}

// ---- 단계(phase) 타임라인 ----
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

// ---- 표 렌더러 ----
// cols: [{t:헤더, k:필드명 | f:행=>HTML, cls:셀클래스}]. f 가 없으면 fmt(r[k])(이스케이프됨).
function table(cols, rows){
  const cls = c => c.cls ? ` class="${c.cls}"` : "";
  let h = "<table><thead><tr>" + cols.map(c=>`<th${cls(c)}>${c.t}</th>`).join("") + "</tr></thead><tbody>";
  if(!rows.length) h += `<tr><td colspan="${cols.length}" class="mut">데이터 없음</td></tr>`;
  for(const r of rows){ h += "<tr>" + cols.map(c=>`<td${cls(c)}>${c.f?c.f(r):fmt(r[c.k])}</td>`).join("") + "</tr>"; }
  return h + "</tbody></table>";
}

// ---- HTTP ----
async function getJSON(u){ const r = await fetch(u); return r.json(); }
// POST 액션 공용: 실패 시 서버가 준 detail 메시지를 그대로 예외로 올린다.
async function postJSON(u, payload){
  const opts = {method:'POST'};
  if(payload!==undefined){
    opts.headers = {'Content-Type':'application/json'};
    opts.body = JSON.stringify(payload);
  }
  const r = await fetch(u, opts);
  let body = null;
  try{ body = await r.json(); }catch(_e){ body = null; }
  if(!r.ok) throw new Error((body&&(body.detail||body.message)) || (r.status+' '+r.statusText));
  return body;
}

// ---- 모달(#modal: 텍스트/SQL, #pmodal: 단계 타임라인) ----
function showTextModal(title, text){
  $("#modal-title").textContent = title;
  $("#modal-sql").textContent = text;
  $("#modal").style.display = 'flex';
}
function closeModal(){ $("#modal").style.display = 'none'; }
function showPhasesModal(title, html){
  $("#pmodal-title").textContent = title;
  $("#pmodal-body").innerHTML = html;
  $("#pmodal").style.display = 'flex';
}
function closePhases(){ $("#pmodal").style.display = 'none'; }

// ---- 실행 이력 페이저 ----
// 페이지 쪽 loadHist() 가 histTotal 을 갱신하고 pagerHtml(행수) 를 표 뒤에 붙인다.
let histOffset = 0, histTotal = 0;
const HIST_LIMIT = 50;
function pagerHtml(n){
  const from = histTotal ? histOffset + 1 : 0;
  const to = histOffset + n;
  return `<div class="pager">
      <button onclick="histPrev()" ${histOffset<=0?'disabled':''}>← 이전</button>
      <span class="mut">${from}–${to} / ${histTotal}</span>
      <button onclick="histNext()" ${to>=histTotal?'disabled':''}>다음 →</button>
    </div>`;
}
function histPrev(){ histOffset = Math.max(0, histOffset - HIST_LIMIT); loadHist(); }
function histNext(){ if(histOffset + HIST_LIMIT < histTotal){ histOffset += HIST_LIMIT; loadHist(); } }

// ---- 실행 이력 검색 필터 ----
// 페이지 HTML 의 필터 바(#hf-status/#hf-user/#hf-job)를 읽어 /history 쿼리스트링을 만든다.
// 필터 바는 자동 갱신(innerHTML 재렌더) 대상 밖의 정적 마크업이라 입력이 유지된다.
function histFilterQS(){
  const val = id => { const el = $(id); return el ? el.value.trim() : ''; };
  let qs = '';
  const st = val('#hf-status'), us = val('#hf-user'), jb = val('#hf-job');
  if(st) qs += `&status=${encodeURIComponent(st)}`;
  if(us) qs += `&username=${encodeURIComponent(us)}`;
  if(jb) qs += `&job_id=${encodeURIComponent(jb)}`;
  return qs;
}
function histSearch(){ histOffset = 0; loadHist(); }
function histReset(){
  for(const id of ['#hf-status','#hf-user','#hf-job']){ const el=$(id); if(el) el.value=''; }
  histOffset = 0; loadHist();
}

// ---- 데이터소스 미리보기(dbprobe) ----
// POST /datasources/{name}/query 를 호출해 결과를 #ds-out 에 표로 그린다.
// extra 는 요청 본문에 병합할 추가 필드(coordinator 의 executor_url 프록시 지정 등).
async function runDatasourceQuery(extra){
  const name = $("#ds-name").value;
  const sql = $("#ds-sql").value.trim();
  if(!name){ alert('데이터소스를 선택하세요'); return; }
  if(!sql){ alert('실행할 SELECT 문을 입력하세요'); return; }
  const limit = Math.max(1, parseInt($("#ds-limit").value, 10) || 100);
  $("#ds-meta").textContent = '';
  $("#ds-out").innerHTML = '<p class="mut">실행 중…</p>';
  try{
    const d = await postJSON(`/datasources/${encodeURIComponent(name)}/query`,
                             Object.assign({sql: sql, limit: limit}, extra||{}));
    renderProbeResult(d);
  }catch(e){ $("#ds-out").innerHTML = `<p class="err">${esc(e.message)}</p>`; }
}
// dbprobe/query-execute 응답({columns, rows(배열의 배열), row_count, truncated, elapsed_ms})을
// 표로 렌더링한다. metaSel/outSel 로 대상 요소를 지정(기본은 데이터소스 콘솔의 #ds-meta/#ds-out).
// 실제 실행한 executor 도 메타에 함께 보여준다 — query-execute 는 executed_by, 데이터소스
// 프록시는 proxied_to 로 오며, coordinator 직접 실행(둘 다 없음/null)이면 executor 표기를 생략한다.
function renderProbeResult(d, metaSel, outSel){
  metaSel = metaSel || "#ds-meta"; outSel = outSel || "#ds-out";
  const who = d.executed_by || d.proxied_to;
  $(metaSel).textContent =
    `${Number(d.row_count||0).toLocaleString('en-US')}행${d.truncated?' (잘림)':''} · ${d.elapsed_ms}ms`
    + (who ? ` · 실행 executor: ${who}` : '');
  const cols = (d.columns||[]).map((c,i)=>({t:esc(c), f:r=>fmt(r[i])}));
  $(outSel).innerHTML = cols.length ? table(cols, d.rows||[]) : '<p class="mut">결과 없음</p>';
}

// ---- 탭 전환 + 자동 갱신 ----
let active = null, loaders = {};
async function refresh(){
  try{ await loaders[active](); $("#upd").textContent = "갱신: " + new Date().toLocaleTimeString();}
  catch(e){ $("#upd").textContent = "갱신 실패: " + e; }
}
// 페이지 진입점: 탭 버튼 클릭 배선 + 첫 렌더 + 3초 주기 자동 갱신(탭 숨김 중엔 쉼).
function initDashboard(l, defaultTab){
  loaders = l; active = defaultTab;
  document.querySelectorAll(".tabs button").forEach(b=>b.onclick=()=>{
    document.querySelectorAll(".tabs button").forEach(x=>x.classList.remove("active"));
    document.querySelectorAll(".panel").forEach(x=>x.classList.remove("active"));
    b.classList.add("active"); $("#p-"+b.dataset.tab).classList.add("active");
    active = b.dataset.tab; refresh();
  });
  refresh();
  setInterval(()=>{ if(!document.hidden) refresh(); }, 3000);
}

// ---- API·링크 탭 ----
// 이 서버가 노출하는 URL 을 한곳에 모은다. 엔드포인트 목록은 /openapi.json 에서 읽으므로 라우트를
// 더하거나 빼도 이 화면은 손대지 않아도 되고, 꺼져 있어 등록되지 않은 기능(Prometheus 등)은 저절로
// 빠진다. 스키마는 기동 중에 바뀌지 않으므로 한 번만 받아 두고 3초 자동 갱신 때 다시 받지 않는다.
let _openapi = null;
async function loadOpenapi(){
  if(!_openapi){ _openapi = await getJSON('/openapi.json'); }
  return _openapi;
}
const absUrl = p => location.origin + p;
// 스키마에 없어도 항상 있는 문서 경로다(include_in_schema=False 로 등록된다).
const API_DOC_LINKS = [
  {path:'/docs', name:'Swagger UI', desc:'API 를 브라우저에서 바로 호출해 볼 수 있는 문서'},
  {path:'/redoc', name:'ReDoc', desc:'읽기 좋은 형태의 API 문서'},
  {path:'/openapi.json', name:'OpenAPI 스키마', desc:'API 명세(JSON). 클라이언트 생성기나 다른 도구에 넣는다'},
];
// 스키마에 경로가 있을 때만 보여 주는 운영용 바로가기다.
const API_OPS_LINKS = [
  {path:'/metrics/prometheus', name:'Prometheus 지표', desc:'Prometheus 가 긁어 가는 텍스트 형식 지표(scrape 대상 URL)'},
  {path:'/prometheus/targets', name:'Prometheus 대상 목록', desc:'executor 목록(http_sd_configs 의 url 로 지정)'},
  {path:'/metrics', name:'메트릭(JSON)', desc:'대시보드·헬스 모니터가 읽는 자원·동시 처리 지표'},
  {path:'/health', name:'헬스 체크', desc:'liveness 프로브용'},
  {path:'/info', name:'기타 정보', desc:'버전·기동 시각 등 런타임 요약'},
  {path:'/config', name:'설정', desc:'현재 설정(비밀값은 가려짐)'},
  {path:'/cluster', name:'클러스터 상태', desc:'coordinator 와 executor 들의 자원·상태'},
  {path:'/sync/status', name:'파일 동기화 상태', desc:'동기화 버전과 executor 별 상태'},
];
// 클립보드 복사. http(비보안 컨텍스트)에서는 navigator.clipboard 가 없으므로 textarea 로 대신한다.
function copyText(text, btn){
  const done = () => { if(btn){ const o = btn.textContent; btn.textContent = '복사됨'; setTimeout(()=>btn.textContent=o, 1200); } };
  if(navigator.clipboard && window.isSecureContext){
    navigator.clipboard.writeText(text).then(done, ()=>alert(text));
    return;
  }
  const ta = document.createElement('textarea');
  ta.value = text; ta.style.position = 'fixed'; ta.style.opacity = '0';
  document.body.appendChild(ta); ta.select();
  try{ document.execCommand('copy'); done(); }catch(_e){ alert(text); }
  document.body.removeChild(ta);
}
function copyBtn(text){
  return `<button class="btn" data-copy="${esc(text)}" onclick="copyText(this.dataset.copy,this)">복사</button>`;
}
function urlLink(u){ return `<a class="lnk" href="${esc(u)}" target="_blank" rel="noopener"><code>${esc(u)}</code></a>`; }
// 바로가기 행: 문서 링크 전부 + 스키마에 실제로 있는 운영 링크.
function apiQuickLinks(spec){
  const paths = (spec && spec.paths) || {};
  return API_DOC_LINKS.concat(API_OPS_LINKS.filter(l => paths[l.path] && paths[l.path].get));
}
// Swagger UI 의 해당 엔드포인트로 바로 가는 주소(deepLinking: #/<태그>/<operationId>).
function swaggerHref(tag, op){
  return op.operationId ? `/docs#/${encodeURIComponent(tag)}/${encodeURIComponent(op.operationId)}` : '/docs';
}
// 스키마의 모든 엔드포인트를 태그 순으로 펼친다. 경로 변수가 없는 GET 은 바로 열 수 있게 링크로 둔다.
function apiEndpoints(spec){
  const rows = [];
  for(const [path, item] of Object.entries((spec && spec.paths) || {})){
    for(const [method, op] of Object.entries(item)){
      if(!['get','post','put','patch','delete'].includes(method)) continue;
      const tag = (op.tags && op.tags[0]) || '기타';
      rows.push({tag, method: method.toUpperCase(), path, op});
    }
  }
  return rows.sort((a,b)=> a.tag.localeCompare(b.tag) || a.path.localeCompare(b.path) || a.method.localeCompare(b.method));
}
function renderApiLinks(spec){
  const quick = apiQuickLinks(spec);
  const qcols = [
    {t:'이름', f:r=>`<b>${esc(r.name)}</b>`},
    {t:'URL', cls:'col-url', f:r=>urlLink(absUrl(r.path))},
    {t:'', f:r=>copyBtn(absUrl(r.path))},
    {t:'설명', f:r=>`<div class="desc">${esc(r.desc)}</div>`},
  ];
  const eps = apiEndpoints(spec);
  const ecols = [
    {t:'분류', f:r=>`<span class="sec">${esc(r.tag)}</span>`},
    {t:'메서드', f:r=>`<span class="pill m-${esc(r.method)}">${esc(r.method)}</span>`},
    {t:'경로', cls:'col-url', f:r=> (r.method==='GET' && !r.path.includes('{'))
        ? urlLink(r.path) : `<code>${esc(r.path)}</code>`},
    {t:'설명', f:r=>`<div class="desc">${fmt(r.op.summary||'')}</div>`},
    {t:'문서', f:r=>`<a class="lnk" href="${swaggerHref(r.tag, r.op)}" target="_blank" rel="noopener">Swagger</a>`},
  ];
  const ver = spec && spec.info ? ` <span class="mut">${esc(spec.info.title||'')} v${esc(spec.info.version||'')}</span>` : '';
  return `<div class="subhd">바로가기${ver}</div>` + table(qcols, quick) +
         `<div class="subhd">전체 엔드포인트 <span class="mut">${eps.length}개 · /openapi.json 기준</span></div>` + table(ecols, eps);
}
// 헤더 오른쪽의 문서·지표 바로가기. Prometheus 는 켜져 있을 때만 붙는다.
async function initHeaderLinks(){
  const el = $('#hdr-links'); if(!el) return;
  let links = [['/docs','Swagger'],['/redoc','ReDoc']];
  try{
    const spec = await loadOpenapi();
    if(spec.paths && spec.paths['/metrics/prometheus']) links.push(['/metrics/prometheus','Prometheus']);
  }catch(_e){ /* 스키마를 못 읽어도 문서 링크는 둔다 */ }
  el.innerHTML = links.map(([p,t])=>`<a class="lnk" href="${p}" target="_blank" rel="noopener">${t}</a>`).join(' · ');
}
