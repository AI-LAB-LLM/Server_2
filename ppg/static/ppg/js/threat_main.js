import { fetchThreatRecords } from './threat_api.js';

const DEVICE_ID = window.THREAT_DEVICE_ID || null;
const POLL_MS   = 4000;
let isFetching   = false;
let chartPpg     = null;
let chartImu     = null;

function renderPpgChart(window) {
  const cont = document.getElementById('threatPpgChart');
  if (!cont || !window.CanvasJS) return;

  const pts = (window.ppg_green || []).map((y, i) => ({ x: i, y }));

  if (chartPpg) {
    chartPpg.options.data[0].dataPoints = pts;
    chartPpg.render();
    return;
  }

  chartPpg = new CanvasJS.Chart('threatPpgChart', {
    title: { text: 'PPG Green (Window)', fontSize: 16, fontFamily: 'Arial' },
    animationEnabled: false,
    axisX: { title: 'sample seq' },
    axisY: { title: 'ppg_green' },
    data: [{ type: 'line', markerSize: 0, dataPoints: pts }],
  });
  chartPpg.render();
}

function renderImuChart(win) {
  const cont = document.getElementById('threatImuChart');
  if (!cont || !window.CanvasJS) return;

  const ptsX = (win.ax || []).map((y, i) => ({ x: i, y }));
  const ptsY = (win.ay || []).map((y, i) => ({ x: i, y }));
  const ptsZ = (win.az || []).map((y, i) => ({ x: i, y }));

  if (chartImu) {
    chartImu.options.data[0].dataPoints = ptsX;
    chartImu.options.data[1].dataPoints = ptsY;
    chartImu.options.data[2].dataPoints = ptsZ;
    chartImu.render();
    return;
  }

  chartImu = new CanvasJS.Chart('threatImuChart', {
    title: { text: 'IMU (ax / ay / az)', fontSize: 16, fontFamily: 'Arial' },
    animationEnabled: false,
    axisX: { title: 'sample seq' },
    axisY: { title: 'acceleration' },
    legend: { verticalAlign: 'bottom' },
    data: [
      { type: 'line', markerSize: 0, name: 'ax', showInLegend: true, dataPoints: ptsX },
      { type: 'line', markerSize: 0, name: 'ay', showInLegend: true, dataPoints: ptsY },
      { type: 'line', markerSize: 0, name: 'az', showInLegend: true, dataPoints: ptsZ },
    ],
  });
  chartImu.render();
}

function updateMeta(win) {
  const elMeta = document.getElementById('threatMeta');
  if (!elMeta) return;
  if (!win) {
    elMeta.textContent = 'No threat window yet.';
    return;
  }
  elMeta.textContent =
    `window_id=${win.id} | sos_id=${win.sos_id ?? '-'} | ` +
    `${win.t_start} ~ ${win.t_end} | hz=${win.hz} | samples=${win.sample_count}`;
}

async function fetchAndRender() {
  if (isFetching) return;
  isFetching = true;
  try {
    const items = await fetchThreatRecords({ deviceId: DEVICE_ID, limit: 1 });
    const latest = items[items.length - 1] || null;
    renderPpgChart(latest || { ppg_green: [] });
    renderImuChart(latest || { ax: [], ay: [], az: [] });
    updateMeta(latest);
  } catch (e) {
    console.error('[threat] fetchAndRender error:', e);
  } finally {
    isFetching = false;
  }
}

document.addEventListener('DOMContentLoaded', () => {
  fetchAndRender();
  setInterval(fetchAndRender, POLL_MS);
});