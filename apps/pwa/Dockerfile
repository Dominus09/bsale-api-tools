# PWA operativa Quillotana — Coolify
# Base Directory / contexto: apps/pwa
# Build Pack: Dockerfile (no Nixpacks)
# Puerto: 3000 · Dominio futuro: app.quillotana.cl
# NO usar este Dockerfile para el ERP (frontend/).

FROM node:22-alpine AS builder
RUN apk add --no-cache libc6-compat
WORKDIR /app

ARG BUILD_NODE_MEMORY_MB=4096
ENV NODE_OPTIONS="--max-old-space-size=${BUILD_NODE_MEMORY_MB}"
ENV npm_config_audit=false
ENV npm_config_fund=false

COPY package.json package-lock.json* ./
RUN npm ci --no-audit --no-fund \
  || npm install --no-audit --no-fund

COPY . .
ARG NEXT_PUBLIC_API_URL=https://api.quillotana.cl
ENV NEXT_PUBLIC_API_URL=${NEXT_PUBLIC_API_URL}
ENV NEXT_TELEMETRY_DISABLED=1
ENV NODE_ENV=production
RUN npm run build

FROM node:22-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
ENV HOSTNAME=0.0.0.0
ENV PORT=3000

RUN apk add --no-cache curl \
  && addgroup --system --gid 1001 nodejs \
  && adduser --system --uid 1001 nextjs

COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./

USER nextjs
EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD curl -fsS http://127.0.0.1:3000/ > /dev/null || exit 1

CMD ["node", "server.js"]
