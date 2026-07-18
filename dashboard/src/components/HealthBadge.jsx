export default function HealthBadge({ level, health }) {
  const labels = {
    green: 'healthy',
    amber: 'stale',
    red: 'down',
  }
  const subtitle = (() => {
    if (!health) return 'connecting…'
    if (health.db !== 'ok') return 'database unreachable'
    const s = health.gold_freshness_seconds
    if (s == null) return 'no data yet'
    if (s < 60) return `fresh (${s}s)`
    if (s < 3600) return `stale (${Math.round(s / 60)}m)`
    return `stale (${Math.round(s / 3600)}h)`
  })()

  return (
    <span className={`badge ${level}`} title={JSON.stringify(health)}>
      <span className="dot" />
      {labels[level]} · {subtitle}
    </span>
  )
}
