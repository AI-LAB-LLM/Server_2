// geo/static/geo/js/geo_map.js
// 여정 이상탐지 결과에 따른 카카오맵 경로 시각화 모듈

let _overlays = [];

// ── 라벨별 색상/스타일 설정 ───────────────────────────
const LABEL_CONFIG = {
  known_normal: {
    icon: '✅',
    title: '정상 경로',
    currentColor: '#00e085',   // 선명한 초록
    baselineColor: '#b57bff',  // 보라 (하늘색 위치기록과 구분)
    baselineOpacity: 0.55,
    currentOpacity: 1.0,
    currentWeight: 8,
    anchorColor: '#7c6fff',    // 보라
    badgeBg: '#ecfdf5',
    badgeBorder: '#6ee7b7',
    badgeText: '#059669',
  },
  unseen_path_same_od: {
    icon: '⚠️',
    title: '새로운 경로',
    currentColor: '#ff8c00',   // 선명한 주황
    baselineColor: '#b57bff',  // 보라
    baselineOpacity: 0.55,
    currentOpacity: 1.0,
    currentWeight: 8,
    anchorColor: '#7c6fff',
    badgeBg: '#fff7ed',
    badgeBorder: '#fdba74',
    badgeText: '#c2410c',
  },
  anomaly: {
    icon: '🚨',
    title: '이상 경로',
    currentColor: '#ff2020',   // 선명한 빨강
    baselineColor: '#94a3b8',  // 회색 (의도적으로 흐림)
    baselineOpacity: 0.4,
    currentOpacity: 1.0,
    currentWeight: 9,
    anchorColor: '#ff2020',
    badgeBg: '#fef2f2',
    badgeBorder: '#fca5a5',
    badgeText: '#b91c1c',
  },
};

// ── 오버레이 관리 ─────────────────────────────────────
function clearOverlays() {
  _overlays.forEach(o => o.setMap(null));
  _overlays = [];
}

// ── KST 시간 포맷 ─────────────────────────────────────
function fmtKstFull(iso) {
  const kst = new Date(new Date(iso).getTime() + 9 * 3600 * 1000);
  const p   = n => String(n).padStart(2, '0');
  return `${p(kst.getUTCMonth()+1)}.${p(kst.getUTCDate())} ${p(kst.getUTCHours())}:${p(kst.getUTCMinutes())}:${p(kst.getUTCSeconds())}`;
}

// ── 현재 경로 GPS 점 (dot + hover 툴팁) ──────────────
function drawTripPoint(kmap, point, color) {
  const wrapper = document.createElement('div');
  wrapper.style.cssText = 'position:relative;cursor:pointer';

  const tip = document.createElement('div');
  tip.style.cssText = [
    'position:absolute','bottom:14px','left:50%','transform:translateX(-50%)',
    'background:rgba(15,23,42,.88)','color:#fff',
    'font-size:11px','padding:3px 8px','border-radius:5px',
    'white-space:nowrap','pointer-events:none','display:none',
    'font-family:monospace','letter-spacing:.02em',
    'box-shadow:0 2px 8px rgba(0,0,0,.25)',
  ].join(';');
  tip.textContent = fmtKstFull(point.timestamp);

  const dot = document.createElement('div');
  dot.style.cssText = [
    'width:8px','height:8px','border-radius:50%',
    `background:${color}`,
    'border:2px solid rgba(255,255,255,0.9)',
    'box-shadow:0 1px 4px rgba(0,0,0,.35)',
    'transition:transform .12s',
  ].join(';');

  wrapper.addEventListener('mouseenter', () => {
    tip.style.display   = 'block';
    dot.style.transform = 'scale(1.5)';
  });
  wrapper.addEventListener('mouseleave', () => {
    tip.style.display   = 'none';
    dot.style.transform = '';
  });

  wrapper.appendChild(tip);
  wrapper.appendChild(dot);

  const overlay = new kakao.maps.CustomOverlay({
    position: new kakao.maps.LatLng(point.lat, point.lon),
    content: wrapper,
    yAnchor: 0.5, xAnchor: 0.5,
    zIndex: 3,
  });
  overlay.setMap(kmap);
  _overlays.push(overlay);
}

// ── 폴리라인 그리기 ───────────────────────────────────
function drawPolyline(kmap, points, color, opacity, weight) {
  if (!points || points.length < 2) return;
  const path = points.map(p => new kakao.maps.LatLng(p.lat, p.lon));
  const line = new kakao.maps.Polyline({
    path,
    strokeColor: color,
    strokeOpacity: opacity,
    strokeWeight: weight,
    strokeStyle: 'solid',
  });
  line.setMap(kmap);
  _overlays.push(line);
}

// ── 앵커존 원 그리기 ──────────────────────────────────
function drawAnchorCircle(kmap, anchor, color) {
  const circle = new kakao.maps.Circle({
    center: new kakao.maps.LatLng(anchor.lat, anchor.lon),
    radius: anchor.radius_m,
    strokeWeight: 2,
    strokeColor: color,
    strokeOpacity: 0.7,
    strokeStyle: 'dashed',
    fillColor: color,
    fillOpacity: 0.07,
  });
  circle.setMap(kmap);
  _overlays.push(circle);
}

// ── 출발/도착 마커 그리기 ─────────────────────────────
function drawEndpointMarker(kmap, lat, lon, label, bgColor) {
  const content = `<div style="
    background:${bgColor};color:#fff;
    border-radius:50%;width:22px;height:22px;
    display:flex;align-items:center;justify-content:center;
    font-size:10px;font-weight:800;
    border:2px solid rgba(255,255,255,0.9);
    box-shadow:0 2px 6px rgba(0,0,0,0.35);
  ">${label}</div>`;
  const overlay = new kakao.maps.CustomOverlay({
    position: new kakao.maps.LatLng(lat, lon),
    content,
    yAnchor: 0.5,
    xAnchor: 0.5,
    zIndex: 10,  // dot(3)보다 위
  });
  overlay.setMap(kmap);
  _overlays.push(overlay);
}

// ── 지도 범위 맞추기 ──────────────────────────────────
function fitBounds(kmap, pointSets) {
  const bounds = new kakao.maps.LatLngBounds();
  let count = 0;
  pointSets.forEach(pts => {
    (pts || []).forEach(p => {
      if (p && p.lat && p.lon) {
        bounds.extend(new kakao.maps.LatLng(p.lat, p.lon));
        count++;
      }
    });
  });
  if (count > 0) kmap.setBounds(bounds, 60, 60, 60, 60);
}

// ── 상태 배지 업데이트 ────────────────────────────────
// pill=true  → 지도 위 오버레이 pill 스타일 (배경색 + 흰 텍스트, 모양은 CSS 클래스)
// pill=false → 카드 스타일 (배경+테두리 박스)
function updateStatusBadge(statusElId, label, anomalyData, pill = false) {
  const el = document.getElementById(statusElId);
  if (!el) return;

  const cfg = LABEL_CONFIG[label];
  if (!cfg) { el.style.display = 'none'; return; }

  const od = anomalyData.od_key
    ? anomalyData.od_key.split('_').slice(-2).join(' → ')
    : '-';

  let scoreText = '';
  if (anomalyData.dtw_score != null && anomalyData.threshold != null) {
    const ratio = (anomalyData.dtw_score / anomalyData.threshold * 100).toFixed(0);
    scoreText = `DTW ${Math.round(anomalyData.dtw_score)} / ${Math.round(anomalyData.threshold)} (${ratio}%)`;
  } else if (anomalyData.message) {
    scoreText = anomalyData.message;
  }

  if (pill) {
    el.style.background = cfg.currentColor;
    el.style.color = '#fff';
    el.style.border = '2px solid rgba(255,255,255,0.35)';
    el.innerHTML = `
      <span style="font-size:15px;line-height:1">${cfg.icon}</span>
      <span>${cfg.title}</span>
      <span class="od-tag">${od}</span>
      ${scoreText ? `<span class="score-tag">${scoreText}</span>` : ''}
    `;
  } else {
    el.style.cssText = `
      display:flex; align-items:center; flex-wrap:wrap; gap:6px;
      padding:7px 10px; border-radius:8px; margin-bottom:8px;
      background:${cfg.badgeBg}; border:1px solid ${cfg.badgeBorder};
    `;
    el.innerHTML = `
      <span style="font-size:15px;line-height:1">${cfg.icon}</span>
      <span style="font-weight:700;color:${cfg.badgeText};font-size:13px">${cfg.title}</span>
      <span style="font-size:11px;color:#6b7280;background:#f3f4f6;
        padding:2px 7px;border-radius:4px;font-family:monospace">${od}</span>
      ${scoreText ? `<span style="font-size:11px;color:#6b7280">${scoreText}</span>` : ''}
    `;
  }
}

// ── 범례 업데이트 ─────────────────────────────────────
function updateLegend(legendElId, label, hasBaseline, hasCurrent) {
  const el = document.getElementById(legendElId);
  if (!el) return;

  const cfg = LABEL_CONFIG[label];
  if (!cfg || (!hasBaseline && !hasCurrent)) {
    el.style.display = 'none';
    return;
  }

  const items = [];
  if (hasBaseline) {
    items.push(`
      <span style="display:inline-flex;align-items:center;gap:4px;font-size:11px;color:#374151">
        <span style="display:inline-block;width:20px;height:3px;
          background:${cfg.baselineColor};border-radius:2px;opacity:0.6"></span>
        베이스라인
      </span>`);
  }
  if (hasCurrent) {
    items.push(`
      <span style="display:inline-flex;align-items:center;gap:4px;font-size:11px;color:#374151">
        <span style="display:inline-flex;align-items:center;gap:2px">
          <span style="display:inline-block;width:16px;height:4px;background:${cfg.currentColor};border-radius:2px"></span>
          <span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${cfg.currentColor};border:1.5px solid #fff;box-shadow:0 0 0 1px ${cfg.currentColor}"></span>
        </span>
        현재 경로
      </span>`);
  }
  items.push(`
    <span style="display:inline-flex;align-items:center;gap:4px;font-size:11px;color:#374151">
      <span style="display:inline-block;width:12px;height:12px;border-radius:50%;
        border:2px dashed ${cfg.anchorColor};opacity:0.8"></span>
      앵커존
    </span>`);

  el.style.cssText = `display:flex; gap:12px; flex-wrap:wrap; padding:6px 2px 0; margin-top:4px;`;
  el.innerHTML = items.join('');
}

// ── 메인 렌더 함수 ────────────────────────────────────
export function renderTripMap(
  data,
  kmap,
  {
    statusElId    = 'geoTripStatus',
    legendElId    = 'geoMapLegend',
    tripActiveKey = '__tripMapActive',
    pillBadge     = false,
    skipFitBounds = false,  // true: 지도 뷰를 건드리지 않음 (같은 trip 재조회 시)
  } = {}
) {
  if (!kmap || !window.kakao?.maps) return;

  clearOverlays();

  const { anomaly_result, current_route, baseline_routes, anchors } = data;

  if (!anomaly_result) {
    window[tripActiveKey] = false;
    if (anchors && anchors.length) {
      anchors.forEach(a => drawAnchorCircle(kmap, a, '#6366f1'));
    }
    const statusEl = document.getElementById(statusElId);
    if (statusEl) statusEl.style.display = 'none';
    const legendEl = document.getElementById(legendElId);
    if (legendEl) legendEl.style.display = 'none';
    return;
  }

  window[tripActiveKey] = true;

  const label = anomaly_result.final_route_label;
  const cfg   = LABEL_CONFIG[label] || LABEL_CONFIG.anomaly;

  // 1. 베이스라인 경로
  const hasBaseline = baseline_routes && baseline_routes.length > 0;
  if (hasBaseline) {
    baseline_routes.forEach(trip => {
      drawPolyline(kmap, trip.points, cfg.baselineColor, cfg.baselineOpacity, 2);
    });
  }

  // 2. 현재 경로
  const hasCurrent = current_route && current_route.length >= 2;
  if (hasCurrent) {
    drawPolyline(kmap, current_route, cfg.currentColor, cfg.currentOpacity, cfg.currentWeight);
    // 각 GPS 점 dot + hover 툴팁 (S/E 마커보다 먼저 그려 z-index 아래에 위치)
    current_route.forEach(p => drawTripPoint(kmap, p, cfg.currentColor));
    // S/E 마커는 dot 위에 표시 (zIndex:10)
    const s = current_route[0];
    drawEndpointMarker(kmap, s.lat, s.lon, 'S', cfg.currentColor);
    const e = current_route[current_route.length - 1];
    drawEndpointMarker(kmap, e.lat, e.lon, 'E', cfg.currentColor);
  }

  // 3. 앵커존
  if (anchors && anchors.length) {
    anchors.forEach(a => drawAnchorCircle(kmap, a, cfg.anchorColor));
  }

  // 4. 지도 범위 자동 조정 (새 trip 로드 시에만)
  if (!skipFitBounds) {
    const allPts = [];
    if (hasCurrent) allPts.push(current_route);
    if (hasBaseline) allPts.push(baseline_routes[0].points);
    if (anchors?.length) allPts.push(anchors.map(a => ({ lat: a.lat, lon: a.lon })));
    fitBounds(kmap, allPts);
  }

  // 5. UI 업데이트
  updateStatusBadge(statusElId, label, anomaly_result, pillBadge);
  updateLegend(legendElId, label, hasBaseline, hasCurrent);
}
