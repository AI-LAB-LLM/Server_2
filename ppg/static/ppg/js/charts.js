// ppg/static/ppg/js/charts.js

import { charts, safelyGet, latestItem, getIrbufPoints } from './state.js';

const WINDOW_SEC = 600;
window.IR_MAX_CHUNKS ??= 120;

export function renderIrHolding() {
  const elChart = document.getElementById('irHoldingChart');
  const elMeta  = document.getElementById('irHoldingMeta');
  if (!elChart || !window.CanvasJS) return;

  const bufFull = getIrbufPoints();
  if (!Array.isArray(bufFull) || bufFull.length === 0) {
    if (elMeta) elMeta.textContent = 'No apnea predictions yet. Waiting for baseline...';
    return;
  }

  // x축: abs_ts 기반 Unix timestamp(초), 최신 600초 슬라이딩
  const lastX = bufFull[bufFull.length - 1].x;
  const X_MAX = lastX;
  const X_MIN = Math.max(bufFull[0].x, lastX - WINDOW_SEC);

  // const ptsProb = bufFull.map(p => {
  //   const y = Number(p.y);
  //   return { x: p.x, y: Number.isFinite(y) ? y : null };
  const ptsProb = [];

  for (let i = 0; i < bufFull.length; i++) {
    const p = bufFull[i];
    const y = Number(p.y);

    if (i > 0) {
      const prev = bufFull[i - 1];

      // MonitoringSession이 바뀌면 선만 끊음
      if (
        prev.session_id != null &&
        p.session_id != null &&
        prev.session_id !== p.session_id
      ) {
        ptsProb.push({
          x: p.x - 0.001,
          y: null,
          ts: p.ts,
          valid: p.valid,
          label: p.label,
        });
      }
    }

      ptsProb.push({
        x: p.x,
        y: Number.isFinite(y) ? y : null,
        ts: p.ts,
        valid: p.valid,
        label: p.label,
        session_id: p.session_id,
      });
    }

  const thrBase = (window.__apneaThr && Number.isFinite(window.__apneaThr))
    ? window.__apneaThr : 0.5;

  const hotIdx = [];
  const bands  = [];
  for (const pt of bufFull) {
    const y = Number(pt.y);
    if (pt.valid !== true || pt.wear_valid !== true) {
      bands.push(pt.x);
    } else if (Number.isFinite(y) && y > thrBase) {
      hotIdx.push(pt.x);
    }
  }

  const dataSeries = [
    { type: 'line', markerSize: 3, name: 'p(apnea)', dataPoints: ptsProb, connectNullData: false }
  ];
  if (hotIdx.length) {
    dataSeries.push({
      type: 'column', axisYType: 'secondary', name: 'Apnea',
      showInLegend: true,
      dataPoints: hotIdx.map(x => ({ x, y: 1 })),
      color: 'rgba(239,68,68,0.25)', markerSize: 0, dataPointWidth: 3
    });
  }
  if (bands.length) {
    dataSeries.push({
      type: 'column', axisYType: 'secondary', name: 'invalid',
      showInLegend: true,
      dataPoints: bands.map(x => ({ x, y: 1 })),
      color: 'rgba(59,130,246,0.25)', markerSize: 0, dataPointWidth: 3
    });
  }

  const axisY = {
    title: 'probability', minimum: 0, maximum: 1, interval: 0.1,
    stripLines: [{ value: thrBase, thickness: 2, color: '#ef4444',
                   label: `thr=${thrBase.toFixed(2)}` }]
  };
  const axisY2 = (hotIdx.length || bands.length)
    ? { minimum: 0, maximum: 1, gridThickness: 0, lineThickness: 0,
        tickLength: 0, labelFormatter: () => '' }
    : {};

  const axisX = {
    title: 'time',
    minimum: X_MIN,
    maximum: X_MAX,
    interval: 120,
    viewportMinimum: X_MIN,
    viewportMaximum: X_MAX,
    labelFormatter: function(e) {
      return new Date(e.value * 1000).toLocaleTimeString('ko-KR', {
        hour: '2-digit',
        minute: '2-digit',
      });
    },
  };

  
  if (!charts.irHolding) {
    charts.irHolding = new CanvasJS.Chart('irHoldingChart', {
      title: { text: 'Real-time Apnea Detection (Smoothed Probability)',
               fontSize: 18, fontWeight: 'normal', fontFamily: 'Arial' },
      animationEnabled: false,
      zoomEnabled: false,
      axisX,
      axisY,
      axisY2,
      data: dataSeries,
      toolTip: {
        shared: false,
        content: function(e) {
          const dp = e.entries?.[0]?.dataPoint;

          const p = dp?.y != null
            ? Number(dp.y).toFixed(3)
            : '-';

          const t = thrBase.toFixed(2);

          const v = dp?.valid === true
            ? 'ok'
            : 'baseline/warming-up';

          const lb = dp?.label != null
            ? Number(dp.label)
            : '-';

          const ts = dp?.ts
            ? new Date(dp.ts).toLocaleTimeString('ko-KR')
            : '-';

          return `<b>${ts}</b><br>p(apnea)=${p} / thr=${t} / label=${lb} / ${v}`;
        }
      },
      legend: { verticalAlign: 'bottom' }
    });
  } else {
    charts.irHolding.options.axisX  = axisX;
    charts.irHolding.options.axisY  = axisY;
    charts.irHolding.options.axisY2 = axisY2;
    charts.irHolding.options.data   = dataSeries;
  }

  if (elChart.offsetWidth === 0 || elChart.offsetHeight === 0) return;
  charts.irHolding.render();

  if (elMeta) {
    const last = bufFull[bufFull.length - 1];
    const status = last?.valid ? 'inference' : 'baseline/warming-up';
    elMeta.textContent = `Latest: p=${last?.y != null ? Number(last.y).toFixed(3) : '-'} | ${status}`;
  }
}

export function renderWearStatus() {
  const card  = document.getElementById('wearCard');
  const elTxt = document.getElementById('wearStatusText');
  const elMeta= document.getElementById('wearStatusMeta');
  const elImg = document.getElementById('wearStateImage');
  if (!card || !elTxt || !elMeta || !elImg) return;

  const it   = latestItem();
  const wear = it ? safelyGet(it, 'predictions.WEAR_GREEN', null) : null;
  const ts   = it?.timestamp || '-';

  if (!wear || !wear.valid) {
    card.className     = 'card wear-card is-unk';
    elTxt.textContent  = 'Still checking...';
    elMeta.textContent = `invalid / ${ts}`;
    elImg.src = '/static/ppg/image/loading.png';
    return;
  }

  if (wear.label === 1) {
    card.className     = 'card wear-card is-wear';
    elTxt.textContent  = 'Wearing';
    elMeta.textContent = `valid / ${ts}`;
    elImg.src = '/static/ppg/image/wear_on.png';
  } else {
    card.className     = 'card wear-card is-off';
    elTxt.textContent  = 'Not Wearing';
    elMeta.textContent = `valid / ${ts}`;
    elImg.src = '/static/ppg/image/wear_off.png';
  }
}