import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// `VITE_API_BASE` is read by `src/api.js` at build time. The compose
// service sets it via an env_file or `environment`.
export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    proxy: {
      // During `npm run dev`, proxy `/api/*` to the FastAPI service so
      // we avoid CORS entirely. In production the static bundle is
      // served from the dashboard image and the browser calls the API
      // directly via `VITE_API_BASE`.
      '/api': {
        target: process.env.VITE_API_BASE || 'http://localhost:8000',
        changeOrigin: true,
        rewrite: (p) => p.replace(/^\/api/, ''),
      },
    },
  },
})
