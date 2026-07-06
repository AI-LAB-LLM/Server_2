import { setItems, appendIrFromItems } from './state.js';
import { fetchRecordsWithPulses, fetchModelStatus } from './api.js';
import { renderIrHolding, renderWearStatus } from './charts.js';

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

// ── IMU ──────────────────────────────────────
function updateImu(st) {
  const imuEl    = document.getElementById('imuLevelText');
  const imuLevel = document.getElementById('imuDangerLevel');
  const tsEl     = document.getElementById('imuTs');
  if (!st?.ok) {
    if (imuEl) imuEl.textContent = '데이터 없음';
    if (imuLevel) imuLevel.textContent = '';
    return;
  }

  if (imuEl)    imuEl.textContent    = st.imu_display ?? '안정';
  if (imuLevel) {
    const lv = st.imu_danger_level;
    imuLevel.textContent = lv != null ? `위험도: ${lv}` : '';
    imuLevel.style.color = lv >= 4 ? '#ef4444' : lv >= 2 ? '#f97316' : '#6b7280';
  }
  if (tsEl) tsEl.textContent = st.timestamp ? `(${st.timestamp})` : '';
}

// ── 딜레이 ────────────────────────────────────
function updateDelay(items) {
  const delayEl = document.getElementById('delayText');
  if (!delayEl || !items.length) return;

  const last     = items[items.length - 1];
  const windowTs = last?.timestamp;
  if (!windowTs) { delayEl.textContent = '-'; return; }

  const diff = (Date.now() - new Date(windowTs).getTime()) / 1000;
  delayEl.textContent = `데이터 지연: ${diff.toFixed(1)}초`;
  delayEl.style.color = diff > 30 ? '#ef4444' : diff > 15 ? '#f97316' : '#10b981';
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

    if (DEVICE_ID) {
      try {
        const res = await fetch(
          `/ppg/api/event_status/?device_id=${encodeURIComponent(DEVICE_ID)}&t=${Date.now()}`,
          { cache: 'no-store' }
        );
        if (res.ok) {
          const st = await res.json();
          updateImu(st);
        }
      } catch (e) {}
    }
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

  if (!window.__devicePollStarted) {
    window.__devicePollStarted = true;
    setInterval(fetchAndRender, POLL_MS);
    fetchAndRender();
  }
})();