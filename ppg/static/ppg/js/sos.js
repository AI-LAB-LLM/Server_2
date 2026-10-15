const POLL_MS = 3000;

const RULES = {
  'physical+psychological': '지속적인 물리 및 심리적 이상',
  'psychological':          '지속적인 심리적 이상',
};

let current     = null;   // 표시 중인 SOS (서버 응답 sos 객체)
let elapsedBase = 0;      // 서버 기준 경과 초
let fetchedAt   = 0;      // 응답 받은 시점 (performance.now)
let fetching    = false;
let closing     = false;

const $ = id => document.getElementById(id);

const hhmmss = iso => (typeof iso === 'string' && iso.length >= 19) ? iso.slice(11, 19) : '-';

const pad = n => String(n).padStart(2, '0');
function fmtElapsed(sec) {
  const s = Math.max(0, Math.floor(sec));
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60);
  return h ? `${h}:${pad(m)}:${pad(s % 60)}` : `${pad(m)}:${pad(s % 60)}`;
}

function levelNote(parts) {
  const txt = parts.filter(([, lv]) => lv != null).map(([name, lv]) => `${name} 레벨 ${lv}`);
  return txt.length ? ` (${txt.join(' · ')})` : '';
}

function basisText(sos) {
  const main = RULES[sos.rule];
  if (!main) return { main: '판단 근거 확인 불가', sub: '' };

  if (sos.rule === 'physical+psychological') {
    return {
      main,
      sub: `30초 내 강한 움직임 ${sos.imu_streak}회 연속 · 심리적 위협 ${sos.ppg_true}회`
         + levelNote([['물리', sos.imu_max_level], ['심리', sos.ppg_max_level]]),
    };
  }
  return {
    main,
    sub: `30초 내 심리적 위협 ${sos.ppg_true}회`
       + levelNote([['심리', sos.ppg_max_level]]),
  };
}

function tick() {
  if (!current) return;
  $('sosElapsed').textContent = fmtElapsed(elapsedBase + (performance.now() - fetchedAt) / 1000);
}

function show(sos) {
  const banner = $('sosBanner');
  if (!banner) return;

  if (!sos) {
    current = null;
    banner.hidden = true;
    return;
  }

  current     = sos;
  elapsedBase = Number(sos.elapsed_sec) || 0;
  fetchedAt   = performance.now();

  const { main, sub } = basisText(sos);
  $('sosBasisMain').textContent = main;
  $('sosBasisSub').textContent  = sub;
  $('sosOccurred').textContent  = hhmmss(sos.occurred_at);
  tick();
  banner.hidden = false;
}

async function poll(deviceId) {
  if (fetching || closing) return;
  fetching = true;
  try {
    const res = await fetch(
      `/ppg/api/sos/status/?device_id=${encodeURIComponent(deviceId)}&t=${Date.now()}`,
      { cache: 'no-store' }
    );
    if (!res.ok) return;
    const st = await res.json();
    if (st?.ok) show(st.active ? st.sos : null);
  } catch (e) {
    console.warn('[sos] poll failed', e);
  } finally {
    fetching = false;
  }
}

async function closeIncident(deviceId) {
  if (!current || closing) return;
  if (!confirm('사건을 종결하시겠습니까?\n종결하면 SOS 표시가 해제됩니다.')) return;

  const btn = $('sosCloseBtn');
  closing = true;
  btn.disabled = true;
  try {
    const res = await fetch('/ppg/api/sos/close/', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ device_id: deviceId, session_id: current.session_id }),
    });
    const data = await res.json().catch(() => null);
    if (!res.ok || !data?.ok) throw new Error(data?.error || `HTTP ${res.status}`);
    show(null);
  } catch (e) {
    alert(`사건 종결에 실패했습니다: ${e.message}`);
  } finally {
    btn.disabled = false;
    closing = false;
  }
}

export function startSosMonitor(deviceId) {
  if (!deviceId || window.__sosPollStarted) return;
  window.__sosPollStarted = true;

  $('sosCloseBtn')?.addEventListener('click', () => closeIncident(deviceId));
  poll(deviceId);
  setInterval(() => poll(deviceId), POLL_MS);
  setInterval(tick, 1000);
}
