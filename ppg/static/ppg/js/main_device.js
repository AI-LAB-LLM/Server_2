import { setItems, appendIrFromItems } from './state.js';
import { fetchRecordsWithPulses, fetchModelStatus } from './api.js';
import { renderIrHolding, renderWearStatus } from './charts.js';
import { startImuMonitor } from './imu.js';
import { startSosMonitor } from './sos.js';

let isFetching  = false;
const POLL_MS   = 4000;
const DEVICE_ID = window.DASH_DEVICE_ID || null;

// ── KPI ──────────────────────────────────────
function updateKpi(items) {
  const elBsl = document.getElementById('kpiBaseline');
  if (!elBsl) return;

  const hasInference = items.some(it => !it.is_baseline && it.predictions?.APNEA_RESULT);
  if (hasInference) {
    elBsl.textContent = '✅ Ready';
    return;
  }

  const baselineCount = items.filter(it => it.is_baseline).length;
  if (baselineCount === 0) {
    elBsl.textContent = '대기 중...';
  } else {
    elBsl.textContent = `⏳ Calibration 중 (${baselineCount}개 수집)`;
  }
}

// ── 딜레이 ────────────────────────────────────
function updateDelay(items) {
  const delayEl = document.getElementById('delayText');
  if (!delayEl || !items.length) return;

  const last      = items[items.length - 1];
  const endedAt   = last?.ended_at;
  const createdAt = last?.created_at;

  if (!endedAt || !createdAt) {
    delayEl.textContent = '-';
    return;
  }

  const diff = (new Date(createdAt).getTime() - new Date(endedAt).getTime()) / 1000;
  delayEl.textContent = `데이터 지연: ${diff.toFixed(1)}초`;
  delayEl.style.color = diff > 10 ? '#ef4444' : diff > 2 ? '#f97316' : '#10b981';
}

// ── 메인 폴링 ────────────────────────────────
async function fetchAndRender() {
  if (isFetching) return;
  isFetching = true;
  try {
    const items = await fetchRecordsWithPulses({ deviceId: DEVICE_ID, limit: 120 });
    setItems(items);
    appendIrFromItems(items);
    renderIrHolding();
    renderWearStatus();
    updateKpi(items);
    updateDelay(items);
  } catch (e) {
    console.error('fetchAndRender error:', e);
  } finally {
    isFetching = false;
  }
}

// ── 초기화 ───────────────────────────────────
(async () => {
  try {
    const status = await fetchModelStatus();
    const thr    = status?.model_config?.threshold;
    if (thr && Number.isFinite(thr)) window.__apneaThr = thr;
  } catch (e) {}

  const INIT = Array.isArray(window.ITEMS) ? window.ITEMS : [];
  setItems(INIT);
  appendIrFromItems(INIT);
  renderIrHolding();
  renderWearStatus();
  updateKpi(INIT);
  updateDelay(INIT);

  // IMU 카드는 자체 폴링(3초)으로 6초 단위 결과를 따라감
  startImuMonitor(DEVICE_ID);
  // SOS 배너: 미종결 SOS 표시 / 사건 종결
  startSosMonitor(DEVICE_ID);

  if (!window.__devicePollStarted) {
    window.__devicePollStarted = true;
    setInterval(fetchAndRender, POLL_MS);
    fetchAndRender();
  }
})();