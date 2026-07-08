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
  if(!r.ok) throw new Error((body&&body.detail) || (r.status+' '+r.statusText));
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
