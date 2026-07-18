# CryptoStream — dashboard image (Module 7).
# Multi-stage: Node 20 for the Vite build, nginx:alpine for serving the
# static bundle. Build context is the dashboard/ directory.

FROM node:20-alpine AS build

WORKDIR /app

# Build-time env: API base URL injected into the JS bundle, plus the
# watchlist used to populate the symbol selector.
ARG VITE_API_BASE=http://localhost:8000
ARG VITE_WATCHLIST=BTCUSD,ETHUSD,SOLUSD
ENV VITE_API_BASE=${VITE_API_BASE} \
    VITE_WATCHLIST=${VITE_WATCHLIST}

COPY package.json package-lock.json* ./
RUN npm install --no-audit --no-fund

COPY . .
RUN npm run build


FROM nginx:alpine

COPY --from=build /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
