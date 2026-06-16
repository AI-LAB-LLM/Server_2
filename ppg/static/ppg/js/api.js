const BASE = '/ppg/api/apnea/records/';

export async function fetchRecordsWithPulses({ deviceId = null, limit = 120 } = {}) {
  const params = new URLSearchParams();
  if (deviceId) params.set('device_id', deviceId);
  if (limit)    params.set('limit', String(limit));
  params.set('t', Date.now().toString());

  const res = await fetch(`${BASE}?${params.toString()}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  return (data?.ok && Array.isArray(data.items)) ? data.items : [];
}

export async function startBaselineSession(deviceId, startedAtMs = Date.now()) {
  const res = await fetch('/ppg/api/apnea/baseline/', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      device_id:  deviceId || '_default_',
      started_at: new Date(startedAtMs).toISOString(),
    }),
  });
  if (!res.ok) throw new Error(`baseline start failed: ${res.status}`);
  return res.json();
}

export async function fetchModelStatus() {
  const res = await fetch('/ppg/api/apnea/status/', { cache: 'no-store' });
  if (!res.ok) throw new Error(`status HTTP ${res.status}`);
  return res.json();
}