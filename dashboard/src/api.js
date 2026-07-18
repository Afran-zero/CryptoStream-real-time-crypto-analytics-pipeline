// Lightweight API client. `VITE_API_BASE` is read at build time; when
// empty, we hit a relative path so the Vite dev-server proxy (see
// vite.config.js) takes over.

const BASE = (import.meta.env.VITE_API_BASE || '').replace(/\/$/, '')

function buildQuery(params) {
  const sp = new URLSearchParams()
  for (const [k, v] of Object.entries(params || {})) {
    if (v !== undefined && v !== null) sp.set(k, v)
  }
  const qs = sp.toString()
  return qs ? `?${qs}` : ''
}

async function get(path, params) {
  const target = `${BASE}${path}${buildQuery(params)}`
  const res = await fetch(target)
  if (!res.ok) throw new Error(`${path} → ${res.status}`)
  return res.json()
}

export const fetchHealth = () => get('/health')
export const fetchLatest = (symbols) =>
  get('/prices/latest', { symbols: symbols.join(',') })
export const fetchCandles = (symbol, limit = 100) =>
  get(`/candles/${encodeURIComponent(symbol)}`, { interval: '1m', limit })
export const fetchMa = (symbol, limit = 200) =>
  get(`/indicators/${encodeURIComponent(symbol)}/ma`, { limit })
