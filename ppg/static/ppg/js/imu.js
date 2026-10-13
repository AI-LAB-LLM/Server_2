// ppg/static/ppg/js/imu.js
// IMU 위험 레벨 카드: 현재 레벨 타일 + 1~5 단계 스케일 + 최근 3분(6초 슬롯) 레벨 막대

const POLL_MS     = 3000;   // IMU 결과는 6초 간격 → 3초 폴링이면 최대 3초 지연
const SLOT_SEC    = 6;
const HISTORY_SEC = 180;
const SLOTS       = HISTORY_SEC / SLOT_SEC;   // 30칸

// 레벨 정의 (색상은 템플릿 CSS의 [data-level] 규칙)
export const IMU_LEVELS = {
  1: { name: '정적',      desc: '움직임이 거의 없는 상태 (앉거나 누워 있음)' },
  2: { name: '저강도',    desc: '가벼운 움직임 (천천히 걷기, 손 동작 등)' },
  3: { name: '보통',      desc: '일상 활동 수준의 활발한 움직임 (빠르게 걷기 등)' },
  4: { name: '고강도',    desc: '급격하고 강한 움직임 · 위협 감지 기준(4 이상)' },
  5: { name: '격렬·위험', desc: '매우 격렬한 움직임 (몸싸움·저항 의심) · 위협 감지' },
};

// 막대 높이(%) — 레벨별
const BAR_HEIGHT = { 1: 22, 2: 38, 3: 55, 4: 78, 5: 100 };

let lastSig  = null;
let fetching = false;

// "2026-10-12T21:45:12.123" → "21:45:12" (USE_TZ=False, 서버가 KST naive로 내려줌)
const hhmmss = iso => (typeof iso === 'string' && iso.length >= 19) ? iso.slice(11, 19) : '-';

const validLevel = lv => Number.isInteger(lv) && IMU_LEVELS[lv] ? lv : null;

function buildScale() {
  const list = document.getElementById('imuScale');
  if (!list || list.childElementCount) return;
  for (const [lv, meta] of Object.entries(IMU_LEVELS)) {
    const li = document.createElement('li');
    li.className = 'imu-scale__item';
    li.dataset.level = lv;
    li.title = `${lv} ${meta.name}: ${meta.desc}`;
    li.innerHTML = `<span class="imu-scale__chip"></span>
                    <span class="imu-scale__name">${lv} ${meta.name}</span>`;
    list.appendChild(li);
  }
}

function buildBars() {
  const bars = document.getElementById('imuBars');
  if (!bars || bars.childElementCount) return;
  for (let i = 0; i < SLOTS; i++) {
    const slot = document.createElement('div');
    slot.className = 'imu-bar is-empty';
    slot.innerHTML = '<span class="imu-bar__fill"></span>';
    bars.appendChild(slot);
  }
}

// history → 30칸 슬롯 (오른쪽 끝 = 최신 결과). 한 칸에 여러 건이면 높은 레벨 우선
function toSlots(history) {
  const slots = new Array(SLOTS).fill(null);
  for (const h of history) {
    const lv  = validLevel(h.level);
    const off = Number(h.offset_sec);
    if (lv == null || !Number.isFinite(off)) continue;
    const idx = SLOTS - 1 - Math.round(off / SLOT_SEC);
    if (idx < 0 || idx >= SLOTS) continue;
    if (!slots[idx] || lv > slots[idx].level) slots[idx] = { level: lv, ts: h.timestamp };
  }
  return slots;
}

function render(st) {
  const card = document.getElementById('imuCard');
  if (!card) return;
  buildScale();
  buildBars();

  const level   = st?.ok ? validLevel(st.imu_danger_level) : null;
  const history = st?.ok && Array.isArray(st.history) ? st.history : [];
  const meta    = level ? IMU_LEVELS[level] : null;

  // 현재 레벨 타일
  const tile = document.getElementById('imuTile');
  if (level) tile.dataset.level = level; else delete tile.dataset.level;
  document.getElementById('imuTileNum').textContent   = level ?? '–';
  document.getElementById('imuTileLabel').textContent = meta ? meta.name : '데이터 없음';
  card.classList.toggle('is-empty',  !level);
  card.classList.toggle('is-threat', level >= 4);

  // 단계 스케일
  document.querySelectorAll('#imuScale .imu-scale__item').forEach(li => {
    li.classList.toggle('is-active', Number(li.dataset.level) === level);
  });
  document.getElementById('imuDesc').textContent = meta
    ? meta.desc
    : '최근 3분간 IMU 결과가 없습니다.';
  document.getElementById('imuTs').textContent = level
    ? `측정 시각 ${hhmmss(st.timestamp)}`
    : '';

  // 6초 단위 막대
  const slots = toSlots(history);
  document.querySelectorAll('#imuBars .imu-bar').forEach((bar, i) => {
    const s    = slots[i];
    const fill = bar.firstElementChild;
    if (s) {
      bar.dataset.level = s.level;
      bar.classList.remove('is-empty');
      fill.style.height = `${BAR_HEIGHT[s.level]}%`;
      bar.title = `${hhmmss(s.ts)} · ${s.level} ${IMU_LEVELS[s.level].name}`;
    } else {
      delete bar.dataset.level;
      bar.classList.add('is-empty');
      fill.style.height = '0';
      bar.title = '데이터 없음';
    }
  });
}

async function poll(deviceId) {
  if (fetching) return;
  fetching = true;
  try {
    const res = await fetch(
      `/ppg/api/event_status/?device_id=${encodeURIComponent(deviceId)}&t=${Date.now()}`,
      { cache: 'no-store' }
    );
    if (!res.ok) return;
    const st = await res.json();

    // 새 결과가 들어왔을 때만 다시 그림
    const hist = Array.isArray(st?.history) ? st.history : [];
    const sig  = `${st?.ok}|${hist.map(h => `${h.id}:${h.level}`).join(',')}`;
    if (sig === lastSig) return;
    lastSig = sig;
    render(st);
  } catch (e) {
    console.warn('[imu] poll failed', e);
  } finally {
    fetching = false;
  }
}

export function startImuMonitor(deviceId) {
  render(null);
  if (!deviceId || window.__imuPollStarted) return;
  window.__imuPollStarted = true;
  poll(deviceId);
  setInterval(() => poll(deviceId), POLL_MS);
}
