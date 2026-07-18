function formatPrice(v) {
  if (v == null) return '—'
  const n = Number(v)
  if (Number.isNaN(n)) return String(v)
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 })
}

export default function LatestPrices({ prices }) {
  if (!prices.length) {
    return <div className="error">No latest prices yet — pipeline warming up.</div>
  }
  return (
    <div className="kv">
      {prices.map((p) => (
        <div key={`${p.symbol}/${p.exchange}`} className="item">
          <div className="symbol">
            {p.symbol} · {p.exchange}
          </div>
          <div className="price">${formatPrice(p.close)}</div>
        </div>
      ))}
    </div>
  )
}
