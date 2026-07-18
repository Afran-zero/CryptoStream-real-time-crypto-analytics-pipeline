# 10 — React + Vite fundamentals

CryptoStream's dashboard is a **React** app served by nginx. This
page explains what React is, what Vite does, and how the dashboard
manages state, fetches data, and renders charts.

---

## What is React?

**React** is a JavaScript library for building user interfaces
(especially web pages). You describe your UI as a tree of
**components**; React figures out how to update the browser when
data changes.

The pitch: **"UI as a function of state."** Given the current
data, render the page. When data changes, re-render.

```jsx
function Greeting({ name }) {
  return <h1>Hello, {name}!</h1>;
}
```

That's a React component. It takes a `name` prop and returns HTML.
When `name` changes, React updates the page.

---

## Components

A component is a function that returns JSX (a JavaScript-flavored
HTML-like syntax). Components can be composed:

```jsx
function App() {
  return (
    <div>
      <Header title="CryptoStream" />
      <PriceList prices={prices} />
      <CandleChart candles={candles} />
    </div>
  );
}
```

CryptoStream's components:

| Component | Purpose |
|-----------|---------|
| `App` | Top-level; manages state and polling |
| `HealthBadge` | Coloured dot: green/amber/red |
| `LatestPrices` | Table of latest prices per symbol |
| `CandleChart` | Line chart of close price + MA(20) |

---

## State — the `useState` hook

A component can have **state** — values that change over time and
trigger re-renders when they do.

```jsx
import { useState } from 'react';

function Counter() {
  const [count, setCount] = useState(0);
  return (
    <button onClick={() => setCount(count + 1)}>
      Clicked {count} times
    </button>
  );
}
```

`useState(0)` creates a state variable initialised to 0.
`setCount` updates it. Calling `setCount` triggers React to
re-render the component.

CryptoStream's `App` component holds several pieces of state:

```jsx
const [health, setHealth] = useState(null);
const [latest, setLatest] = useState([]);
const [candles, setCandles] = useState([]);
const [ma, setMa] = useState([]);
const [error, setError] = useState(null);
```

Each is updated as fresh data arrives.

---

## Effects — the `useEffect` hook

**Effects** are how you run side effects (network requests,
timers, subscriptions) in React. `useEffect` schedules a function
to run after render:

```jsx
import { useEffect, useState } from 'react';

function PriceTicker() {
  const [price, setPrice] = useState(null);

  useEffect(() => {
    const id = setInterval(async () => {
      const res = await fetch('/api/prices/latest?symbols=BTCUSD');
      const data = await res.json();
      setPrice(data.prices[0]);
    }, 5000);
    return () => clearInterval(id);    // cleanup
  }, []);                              // [] = run once on mount

  return <div>{price?.price ?? 'loading...'}</div>;
}
```

Things to know:

- **The dependency array `[]`** tells React when to re-run. `[]`
  means "only on mount". `[symbol]` means "when `symbol`
  changes".
- **Cleanup function.** The function you return from `useEffect`
  runs when the component unmounts or before the next effect. Use
  it to clear timers, cancel requests, unsubscribe.
- **Strict Mode** in development calls effects twice to surface
  bugs.

CryptoStream's polling effect:

```jsx
useEffect(() => {
  const ctrl = new AbortController();
  let alive = true;

  const tick = async () => {
    try {
      const [h, l, c, m] = await Promise.all([
        fetchHealth(),
        fetchLatest(SYMBOLS),
        fetchCandles(symbol, CHART_LIMIT),
        fetchMa(symbol, CHART_LIMIT),
      ]);
      if (!alive || ctrl.signal.aborted) return;
      setHealth(h);
      setLatest(l.prices || []);
      setCandles(c.candles || []);
      setMa(m.points || []);
      setError(null);
    } catch (e) {
      if (!alive || ctrl.signal.aborted) return;
      if (e.name !== 'AbortError') setError(e.message || String(e));
    }
  };

  tick();
  const id = setInterval(tick, POLL_MS);

  return () => {
    alive = false;
    ctrl.abort();
    clearInterval(id);
  };
}, [symbol]);
```

Why the `AbortController`? When the user changes the symbol
selector, the effect cleans up:

1. Sets `alive = false` so any in-flight `setX` calls don't
   update state.
2. Calls `ctrl.abort()` so the `fetch` is cancelled (no waiting
   for a slow response).
3. Clears the interval.

Then the effect runs again with the new `symbol`.

---

## Why React (vs plain JS or Vue)?

| Alternative | Why not |
|-------------|---------|
| Plain JS + DOM API | Tedious for complex UIs; you reinvent state management |
| Vue | Similar; React has the larger ecosystem and more jobs |
| Svelte | Newer; smaller ecosystem; we picked React for familiarity |
| Server-rendered HTML | Doesn't update live without page reloads |

For a polling dashboard with state, React is a clean fit. The
component model matches the UI structure, and the hooks
(`useState`, `useEffect`) handle the dynamic parts naturally.

---

## Vite — the build tool

**Vite** is the tool that turns `.jsx` files into a working
website. It handles:

- **Transpilation** — JSX → plain JavaScript that browsers
  understand.
- **Bundling** — combining hundreds of files into a few.
- **Dev server** — local server with hot-reload (edit a file,
  browser updates instantly).
- **Build** — production-ready static files.

Why Vite over Create React App, Next.js, or Webpack?

- **CRA is deprecated.** Meta stopped maintaining it.
- **Next.js adds SSR/routing.** Overkill for a single-page
  dashboard.
- **Webpack is slow.** Vite uses native ESM in dev for instant
  startup.

CryptoStream's `dashboard/vite.config.js`:

```js
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': process.env.VITE_API_BASE || 'http://localhost:8000',
    },
  },
});
```

In dev, requests to `/api/*` get proxied to the FastAPI server
(`VITE_API_BASE` or `http://localhost:8000` by default). This
avoids CORS hassles during development.

---

## Build-time env vars (VITE_*)

Vite reads `import.meta.env.VITE_*` at **build time** and bakes
the values into the JavaScript bundle:

```js
const BASE = (import.meta.env.VITE_API_BASE || '').replace(/\/$/, '');
```

This means:

- If you change `VITE_API_BASE` in `.env`, you must rebuild:
  `docker compose build dashboard`.
- The dashboard's JS bundle contains the URL of the API, hardcoded.
- No runtime configuration for the API URL.

For prod, that's fine — you build once, deploy the bundle.

The `VITE_*` values are passed to the Docker build via `args:` in
`docker-compose.yml`:

```yaml
dashboard:
  build:
    context: ./dashboard
    args:
      VITE_API_BASE: ${VITE_API_BASE:-http://localhost:8000}
      VITE_WATCHLIST: ${VITE_WATCHLIST:-BTCUSD,ETHUSD,SOLUSD}
```

---

## Recharts — the chart library

CryptoStream uses **recharts** for the candle chart. Recharts
gives you React components for charts:

```jsx
import { LineChart, Line, XAxis, YAxis, Tooltip, CartesianGrid } from 'recharts';

<LineChart data={data} width={600} height={300}>
  <CartesianGrid />
  <XAxis dataKey="bucket" />
  <YAxis />
  <Tooltip />
  <Line dataKey="close" stroke="#56d364" />
  <Line dataKey="ma_20" stroke="#e3b341" strokeDasharray="4 4" />
</LineChart>
```

You compose the chart from components. The data is an array of
plain objects; recharts handles axes, tooltips, hover effects.

For CryptoStream, the data is:

```js
const data = candles.map(c => ({
  bucket: c.bucket,         // ISO timestamp
  close: Number(c.close),   // close price
  ma_20: maLookup[c.bucket + '|' + c.exchange],  // MA(20) for this (bucket, exchange)
}));
```

The composite key (`bucket|exchange`) ensures that when multiple
exchanges are present, their MA lines don't collide.

---

## nginx — the production server

In production, Vite's dev server is replaced by **nginx**, which
serves the static built files:

```nginx
server {
  listen 80;
  server_name _;

  root /usr/share/nginx/html;
  index index.html;

  location / {
    try_files $uri $uri/ /index.html;   # SPA fallback
  }

  location /assets/ {
    expires 1y;
    add_header Cache-Control "public, immutable";
  }
}
```

The `try_files` line is the **SPA fallback**: if the user navigates
to `/some/deep/path`, nginx doesn't have a file for it, so it
serves `index.html` and React Router (or in our case, plain React
state) takes over.

The `/assets/` cache header means hashed asset files
(`main-abc123.js`) are cached for a year — they have unique hashes
so they can never go stale.

The `Dockerfile` is a multi-stage build:

```dockerfile
FROM node:20-alpine AS builder
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
ARG VITE_API_BASE=http://localhost:8000
ARG VITE_WATCHLIST=BTCUSD,ETHUSD,SOLUSD
ENV VITE_API_BASE=$VITE_API_BASE
ENV VITE_WATCHLIST=$VITE_WATCHLIST
RUN npm run build

FROM nginx:alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=builder /app/dist /usr/share/nginx/html
```

Stage 1 builds the React app. Stage 2 ships only the built files
plus nginx. No node_modules in the final image — small, fast,
secure.

---

## A complete React component

Here's the `HealthBadge` component in full:

```jsx
export default function HealthBadge({ level, health }) {
  const label = level === 'green' ? 'OK' :
                level === 'amber' ? 'Warming' : 'Stale';
  return (
    <span className={`badge badge-${level}`}>
      <span className="dot" />
      {label}
      {health?.gold_freshness_seconds != null &&
        ` · ${health.gold_freshness_seconds}s ago`}
    </span>
  );
}
```

Takes a `level` ("green" / "amber" / "red") and an optional
`health` object. Renders a coloured dot + a label + the
freshness in seconds. Done.

---

## Try it yourself

```bash
# Open the dashboard
open http://localhost:5173

# Open the browser dev tools (F12)
# Go to the Network tab; you'll see /api/* requests every 5s.
# Go to the React tab (if installed) to inspect the component tree.

# Tail the dashboard's nginx logs
make dashboard-logs
```

---

## Vocabulary

| Term | Meaning |
|------|---------|
| React | JS library for component-based UIs |
| Component | A function that returns JSX |
| JSX | HTML-like syntax in JavaScript |
| Props | Inputs to a component |
| State | Component-local data that changes over time |
| Hook | A function that adds React features (`useState`, `useEffect`) |
| `useState` | Declare a state variable |
| `useEffect` | Schedule a side effect after render |
| Effect cleanup | Function returned from `useEffect` to clean up |
| Dependency array | Tells React when to re-run an effect |
| Vite | Build tool / dev server for modern JS apps |
| `import.meta.env` | Build-time env vars (Vite) |
| `VITE_*` | Convention for build-time env vars |
| recharts | React chart library |
| nginx | Production web server; serves the built React bundle |
| SPA fallback | Always serve index.html so client-side routing works |

---

## What's next?

- [11_HOW_DATA_FLOWS.md](11_HOW_DATA_FLOWS.md) — trace the full
  data flow including the dashboard.
- [12_DESIGN_DECISIONS.md](12_DESIGN_DECISIONS.md) — why this
  frontend stack specifically.