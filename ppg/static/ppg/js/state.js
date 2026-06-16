// apnea/static/apnea/js/state.js

export const POPUP_ENABLED = true;

export let ITEMS = (typeof window !== 'undefined' && Array.isArray(window.ITEMS)) ? window.ITEMS : [];

export const charts = {
  irHolding: null,
};

const MAX_ITEMS = 120;

export const IRBUF_CAP = 900;
let IRBUF = [];
let IRBUF_SEQ = 0;
let IRBUF_MAX_ID = Number.NEGATIVE_INFINITY;
let IRBUF_LAST_TS = null;
let IRBUF_LAST_SIG = null;

const toNum = v => {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};

export function setItems(newItems) {
  const arr = Array.isArray(newItems) ? newItems : [];
  ITEMS = (arr.length > MAX_ITEMS) ? arr.slice(-MAX_ITEMS) : arr;
}

export function safelyGet(obj, path, def = null) {
  if (!obj || !path) return def;
  try {
    return path.split('.').reduce((acc, k) => (acc != null ? acc[k] : undefined), obj) ?? def;
  } catch {
    return def;
  }
}

export function latestItem() {
  return (Array.isArray(ITEMS) && ITEMS.length) ? ITEMS[ITEMS.length - 1] : null;
}


export function getIrbufPoints() { return IRBUF.slice(); }

export function resetIrbuf() {
  IRBUF = [];
  IRBUF_SEQ = 0;
  IRBUF_MAX_ID = Number.NEGATIVE_INFINITY;
  IRBUF_LAST_TS = null;
  IRBUF_LAST_SIG = null;
}

export function appendIrFromItems(items) {
  const rows = Array.isArray(items) ? items : [];
  if (!rows.length) return;

  const newRows = [...rows]
    .map(r => ({ r, idNum: toNum(r.id ?? r.pk) }))
    .filter(o => o.idNum != null && o.idNum > IRBUF_MAX_ID)
    .sort((a, b) => a.idNum - b.idNum);

  for (const { r, idNum } of newRows) {
    const beatResults = r?.beat_results;
    const pred = r?.predictions?.APNEA_RESULT;

    if (Array.isArray(beatResults) && beatResults.length > 0 && pred) {
      for (const beat of beatResults) {
        const timeSec = beat?.time_sec;
        const prob    = beat?.p_apnea_smooth;
        const valid   = beat?.status === 'ok';
        const label   = beat?.pred_label;

        if (timeSec == null) continue;

        const x = Number(timeSec);
        if (!Number.isFinite(x)) continue;

        // time_sec가 이전보다 작아지면 새 세션으로 판단하고 기존 그래프 제거
        if (IRBUF.length > 0 && x < IRBUF[IRBUF.length - 1].x) {
          IRBUF.length = 0;
        }

        IRBUF.push({
          x,
          y: (prob != null && Number.isFinite(Number(prob))) ? Number(prob) : null,
          valid,
          ts: r?.timestamp ?? null,
          thr: null,
          label,
          wear_valid: r?.predictions?.WEAR_GREEN?.valid,
        });
      }
    }

    if (IRBUF.length > IRBUF_CAP) {
      IRBUF.splice(0, IRBUF.length - IRBUF_CAP);
    }

    if (idNum > IRBUF_MAX_ID) {
      IRBUF_MAX_ID = idNum;
    }
  }
}