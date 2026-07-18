import { useEffect, useMemo, useState } from 'react'
import {
  fetchHealth,
  fetchLatest,
  fetchCandles,
  fetchMa,
} from './api.js'
import HealthBadge from './components/HealthBadge.jsx'
import LatestPrices from './components/LatestPrices.jsx'
import CandleChart from './components/CandleChart.jsx'

const POLL_MS = 5000
const FRESHNESS_GREEN_S = 120  // <2 min is green
const FRESHNESS_AMBER_S = 600  // <10 min is amber
const CHART_LIMIT = 60

// In a real deployment this list comes from a `/symbols` endpoint or
// the WATCHLIST env (passed at build time). Hardcoding the Module 0
// default keeps the demo dependency-free.
const SYMBOLS = (import.meta.env.VITE_WATCHLIST || 'BTCUSD,ETHUSD,SOLUSD')
  .split(',')
  .map((s) => s.trim().toUpperCase())
  .filter(Boolean)

export default function App() {
  const [health, setHealth] = useState(null)
  const [latest, setLatest] = useState([])
  const [symbol, setSymbol] = useState(SYMBOLS[0])
  const [candles, setCandles] = useState([])
  const [ma, setMa] = useState([])
  const [error, setError] = useState(null)

  useEffect(() => {
    const ctrl = new AbortController()
    let alive = true

    const tick = async () => {
      try {
        const [h, l, c, m] = await Promise.all([
          fetchHealth(),
          fetchLatest(SYMBOLS),
          fetchCandles(symbol, CHART_LIMIT),
          fetchMa(symbol, CHART_LIMIT),
        ])
        if (!alive || ctrl.signal.aborted) return
        setHealth(h)
        setLatest(l.prices || [])
        setCandles(c.candles || [])
        setMa(m.points || [])
        setError(null)
      } catch (e) {
        if (!alive || ctrl.signal.aborted) return
        // AbortError is the expected outcome when the symbol changes
        // mid-flight; skip the noisy error message in that case.
        if (e.name !== 'AbortError') setError(e.message || String(e))
      }
    }
    tick()
    const id = setInterval(tick, POLL_MS)
    return () => {
      alive = false
      ctrl.abort()
      clearInterval(id)
    }
  }, [symbol])

  const badgeLevel = useMemo(() => {
    if (!health) return 'amber'
    if (health.db !== 'ok') return 'red'
    const s = health.gold_freshness_seconds
    if (s === null || s === undefined) return 'amber'
    if (s < FRESHNESS_GREEN_S) return 'green'
    if (s < FRESHNESS_AMBER_S) return 'amber'
    return 'red'
  }, [health])

  return (
    <div className="container">
      <div className="row">
        <h1>CryptoStream</h1>
        <HealthBadge level={badgeLevel} health={health} />
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 8 }}>
          <label htmlFor="sym">Symbol:</label>
          <select
            id="sym"
            value={symbol}
            onChange={(e) => setSymbol(e.target.value)}
          >
            {SYMBOLS.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </div>
      </div>

      {error && <div className="error">API error: {error}</div>}

      <div className="panel">
        <h2>Latest prices</h2>
        <LatestPrices prices={latest} />
      </div>

      <div className="panel">
        <h2>{symbol} · 1m candles</h2>
        <CandleChart candles={candles} ma={ma} />
      </div>
    </div>
  )
}
