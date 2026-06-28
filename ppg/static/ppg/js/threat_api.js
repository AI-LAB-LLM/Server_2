export async function fetchThreatRecords({ deviceId = null, limit = 5 } = {}) {
  const params = new URLSearchParams();
  if (deviceId) params.set('device_id', deviceId);
  params.set('limit', String(limit));
  params.set('t', Date.now().toString());

  const res = await fetch(`/ppg/api/threat/records/?${params.toString()}`, { cache: 'no-store' });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  return (data?.ok && Array.isArray(data.items)) ? data.items : [];
}