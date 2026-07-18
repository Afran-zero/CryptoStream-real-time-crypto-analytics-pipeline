import { useMemo } from 'react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts'

// Merge candles with MA points keyed on `(bucket, exchange)` so multiple
// exchanges for the same symbol don't overwrite each other's MA line.
function mergeSeries(candles, ma) {
  const maByKey = new Map(
    ma.map((p) => [`${p.bucket}|${p.exchange ?? ''}`, p.ma_20])
  )
  return candles.map((c) => {
    const maVal = maByKey.get(`${c.bucket}|${c.exchange ?? ''}`)
    return {
      bucket: c.bucket,
      close: Number(c.close),
      ma_20: maVal != null ? Number(maVal) : null,
    }
  })
}

export default function CandleChart({ candles, ma }) {
  const data = useMemo(() => mergeSeries(candles || [], ma || []), [candles, ma])

  if (!data.length) {
    return <div className="error">No candles yet.</div>
  }

  return (
    <div style={{ width: '100%', height: 360 }}>
      <ResponsiveContainer>
        <LineChart data={data} margin={{ top: 10, right: 24, left: 0, bottom: 0 }}>
          <CartesianGrid stroke="#30363d" strokeDasharray="3 3" />
          <XAxis
            dataKey="bucket"
            tickFormatter={(v) => new Date(v).toLocaleTimeString()}
            stroke="#9da7b3"
            fontSize={11}
          />
          <YAxis
            domain={['auto', 'auto']}
            stroke="#9da7b3"
            fontSize={11}
            tickFormatter={(v) => Number(v).toLocaleString()}
          />
          <Tooltip
            contentStyle={{
              background: '#161b22',
              border: '1px solid #30363d',
              fontSize: 12,
            }}
            labelFormatter={(v) => new Date(v).toLocaleString()}
          />
          <Legend />
          <Line
            type="monotone"
            dataKey="close"
            stroke="#56d364"
            dot={false}
            name="close"
            strokeWidth={2}
          />
          <Line
            type="monotone"
            dataKey="ma_20"
            stroke="#e3b341"
            dot={false}
            name="MA(20)"
            strokeDasharray="4 4"
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
