const DEFAULT_API_BASE = "/api/geo/track";
const DEFAULT_POLL_MS = 5000;

// API가 반환하는 timestamp는 오프셋 없는 KST 벽시계 문자열이므로 Date 파싱 없이 그대로 표기한다.
function formatKst(isoString) {
  if (!isoString) return "";
  const [datePart, timePart = ""] = isoString.split("T");
  return `${datePart} ${timePart.slice(0, 8)} KST`;
}

// datetime-local 입력값(KST 벽시계 기준 "YYYY-MM-DDTHH:mm")을 API가 기대하는 naive KST ISO 문자열로 변환
function kstLocalInputToIso(value) {
  return `${value}:00`;
}

// 실제 현재 시각(Date)을 datetime-local 입력에 채울 KST 벽시계 문자열로 변환
function toDatetimeLocalValue(date) {
  const kstMs = date.getTime() + 9 * 3600 * 1000;
  return new Date(kstMs).toISOString().slice(0, 16);
}

// 오늘 00:00(KST)에 해당하는 naive KST ISO 문자열을 반환 (API에 그대로 전달)
function kstTodayStartIso() {
  const now = new Date();
  const kst = new Date(now.getTime() + 9 * 3600 * 1000);
  kst.setUTCHours(0, 0, 0, 0);
  return kst.toISOString().slice(0, 19);
}

function createPointOverlay(kakao, map, point, isLatest) {
  const wrapper = document.createElement("div");
  wrapper.className = "geo-point";

  const tooltip = document.createElement("div");
  tooltip.className = "geo-tooltip" + (isLatest ? " is-visible" : "");
  tooltip.textContent = formatKst(point.timestamp);

  const dot = document.createElement("div");
  dot.className = "geo-dot" + (isLatest ? " is-latest" : "");

  dot.addEventListener("mouseenter", () => tooltip.classList.add("is-visible"));
  dot.addEventListener("mouseleave", () => {
    if (!isLatest) tooltip.classList.remove("is-visible");
  });

  wrapper.appendChild(tooltip);
  wrapper.appendChild(dot);

  const overlay = new kakao.maps.CustomOverlay({
    position: new kakao.maps.LatLng(point.latitude, point.longitude),
    content: wrapper,
    yAnchor: 1,
    zIndex: isLatest ? 10 : 1,
  });
  overlay.setMap(map);
  return overlay;
}


export function mountGeoMapWidget(container, deviceId, options = {}) {
  const apiBase = options.apiBase || DEFAULT_API_BASE;
  const pollMs = options.pollMs || DEFAULT_POLL_MS;

  container.innerHTML = "";
  container.classList.add("geo-map-widget");

  const filterBar = document.createElement("div");
  filterBar.className = "geo-filter-bar";
  filterBar.innerHTML = `
    <select class="geo-preset-select">
      <option value="60">최근 1시간</option>
      <option value="180" selected>최근 3시간</option>
      <option value="360">최근 6시간</option>
      <option value="1440">최근 24시간</option>
      <option value="today">오늘</option>
      <option value="custom">사용자 지정</option>
    </select>
    <span class="geo-custom-range" style="display:none;">
      <input type="datetime-local" class="geo-start-input">
      <span>~</span>
      <input type="datetime-local" class="geo-end-input">
      <span>(KST)</span>
      <button type="button" class="geo-apply-btn">적용</button>
    </span>
    <span class="geo-latest-badge">불러오는 중...</span>
  `;

  const mapWrap = document.createElement("div");
  mapWrap.className = "geo-map-wrap";

  const mapEl = document.createElement("div");
  mapEl.className = "geo-map-canvas";

  const zoomControl = document.createElement("div");
  zoomControl.className = "geo-zoom-control";
  zoomControl.innerHTML = `
    <button type="button" class="geo-zoom-btn geo-zoom-in" aria-label="확대">+</button>
    <button type="button" class="geo-zoom-btn geo-zoom-out" aria-label="축소">&minus;</button>
  `;

  mapWrap.appendChild(mapEl);
  mapWrap.appendChild(zoomControl);

  container.appendChild(filterBar);
  container.appendChild(mapWrap);

  const presetSelect = filterBar.querySelector(".geo-preset-select");
  const customRange = filterBar.querySelector(".geo-custom-range");
  const startInput = filterBar.querySelector(".geo-start-input");
  const endInput = filterBar.querySelector(".geo-end-input");
  const applyBtn = filterBar.querySelector(".geo-apply-btn");
  const latestBadge = filterBar.querySelector(".geo-latest-badge");

  let map = null;
  let polyline = null;
  let overlays = [];
  let pollTimer = null;
  let lastSignature = null;
  let destroyed = false;

  function clearOverlays() {
    overlays.forEach((o) => o.setMap(null));
    overlays = [];
    if (polyline) {
      polyline.setMap(null);
      polyline = null;
    }
  }

  function renderPoints(points) {
    if (!map || destroyed) return;

    const signature = `${points.length}:${points[points.length - 1]?.id ?? ""}`;
    if (signature === lastSignature) return;
    lastSignature = signature;

    clearOverlays();

    if (!points.length) {
      latestBadge.textContent = "데이터 없음";
      return;
    }

    const kakao = window.kakao;
    const path = points.map((p) => new kakao.maps.LatLng(p.latitude, p.longitude));

    polyline = new kakao.maps.Polyline({
      path,
      strokeWeight: 3,
      strokeColor: "#38bdf8",
      strokeOpacity: 0.9,
      strokeStyle: "solid",
    });
    polyline.setMap(map);

    points.forEach((p, idx) => {
      overlays.push(createPointOverlay(kakao, map, p, idx === points.length - 1));
    });

    const bounds = new kakao.maps.LatLngBounds();
    path.forEach((p) => bounds.extend(p));
    map.setBounds(bounds);

    latestBadge.textContent = `마지막 수집: ${formatKst(points[points.length - 1].timestamp)}`;
  }

  function buildQuery() {
    const preset = presetSelect.value;
    const params = new URLSearchParams({ device_id: deviceId });

    if (preset === "custom") {
      if (startInput.value) params.set("start", kstLocalInputToIso(startInput.value));
      if (endInput.value) params.set("end", kstLocalInputToIso(endInput.value));
    } else if (preset === "today") {
      params.set("start", kstTodayStartIso());
    } else {
      params.set("window_minutes", preset);
    }
    return params;
  }

  async function fetchAndRender() {
    if (destroyed) return;
    try {
      const params = buildQuery();
      const res = await fetch(`${apiBase}?${params.toString()}`, { cache: "no-store" });
      if (!res.ok) return;
      const data = await res.json();
      renderPoints(data.points || []);
    } catch (e) {
      console.error("[geo-map-widget] fetch error", e);
    }
  }

  function stopPolling() {
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }

  function startPollingIfNeeded() {
    stopPolling();
    if (presetSelect.value !== "custom") {
      pollTimer = setInterval(fetchAndRender, pollMs);
    }
  }

  function restartCycle() {
    lastSignature = null;
    stopPolling();
    fetchAndRender();
    startPollingIfNeeded();
  }

  presetSelect.addEventListener("change", () => {
    const isCustom = presetSelect.value === "custom";
    customRange.style.display = isCustom ? "inline-flex" : "none";

    if (isCustom && !startInput.value) {
      const now = new Date();
      const start = new Date(now.getTime() - 3 * 3600 * 1000);
      startInput.value = toDatetimeLocalValue(start);
      endInput.value = toDatetimeLocalValue(now);
    }

    if (!isCustom) restartCycle();
  });

  applyBtn.addEventListener("click", restartCycle);

  zoomControl.querySelector(".geo-zoom-in").addEventListener("click", () => {
    if (map) map.setLevel(map.getLevel() - 1);
  });
  zoomControl.querySelector(".geo-zoom-out").addEventListener("click", () => {
    if (map) map.setLevel(map.getLevel() + 1);
  });

  function ensureMap() {
    return new Promise((resolve) => {
      if (map || !window.kakao || !window.kakao.maps) {
        resolve(map);
        return;
      }
      window.kakao.maps.load(() => {
        map = new window.kakao.maps.Map(mapEl, {
          center: new window.kakao.maps.LatLng(37.494, 126.997),  // 두 앵커 중간
          level: 9,
        });
        resolve(map);
      });
    });
  }

  (async () => {
    await ensureMap();
    if (destroyed) return;
    await fetchAndRender();
    startPollingIfNeeded();
  })();

  return {
    getMap() { return ensureMap(); },
    clearGpsOverlays() { clearOverlays(); lastSignature = null; },
    setFilterBarVisible(v) { filterBar.style.display = v ? '' : 'none'; },
    pausePolling() { stopPolling(); },
    resumePolling() { fetchAndRender(); startPollingIfNeeded(); },
    destroy() {
      destroyed = true;
      stopPolling();
      clearOverlays();
    },
  };
}
