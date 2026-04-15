# Imagen de producción Next.js para Coolify / Docker.
#
# Coolify (importante):
# - Si el deploy falla en Nixpacks paso "nix-env / unpacking nixpkgs" (exit 255): en el recurso,
#   Build Pack = "Dockerfile" (no Nixpacks), Base Directory = "frontend", Dockerfile = Dockerfile.
# - Base Directory / contexto de build: carpeta "frontend" (este Dockerfile).
# - Puerto interno del contenedor: 3000 (o el PORT que inyecte Coolify; standalone lo respeta).
# - Healthcheck: wget de busybox (incluido en node:alpine); evita apk add curl si el mirror Alpine falla.
# - NO uses el Dockerfile de la raíz del repo (Python/API en 8000) para work.quillotana.cl.
#
# "Cannot GET /login" en el navegador suele ser Express u otro servicio, no Next: revisa que el
# dominio apunte al recurso del frontend y al puerto correcto.

FROM node:22-alpine AS builder
# Evita fallos raros de binarios precompilados en musl (Next / Tailwind oxide, etc.).
RUN apk add --no-cache libc6-compat
WORKDIR /app

# Builders con poca RAM (Coolify/VPS): menos OOM en install + build de Next.
ENV NODE_OPTIONS="--max-old-space-size=4096"
ENV npm_config_audit=false
ENV npm_config_fund=false
ENV npm_config_fetch_retries=5
ENV npm_config_fetch_retry_mintimeout=20000

COPY package.json package-lock.json ./
# Caché de npm acelera redeploys y reduce timeouts al registry.
RUN --mount=type=cache,target=/root/.npm \
    npm ci --no-audit --no-fund

COPY . .
ENV NEXT_TELEMETRY_DISABLED=1
ENV NODE_ENV=production
# Fallo explícito si Next no emitió standalone (evita COPY vacío en runner y exit 255 opacos en Coolify).
RUN npm run build \
  && test -f .next/standalone/server.js \
  && test -f .next/standalone/package.json

FROM node:22-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
ENV HOSTNAME=0.0.0.0
ENV PORT=3000

RUN addgroup --system --gid 1001 nodejs \
  && adduser --system --uid 1001 nextjs

# npm run build ya ejecuta copy-standalone-assets (public + static dentro de standalone)
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./
RUN test -f server.js && test -d node_modules

USER nextjs
EXPOSE 3000

# 127.0.0.1 evita localhost → ::1 en Alpine. Coincide con ENV PORT=3000 (Coolify suele usar 3000).
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD wget -qO- http://127.0.0.1:3000/ > /dev/null || exit 1

CMD ["node", "server.js"]
