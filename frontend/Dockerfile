# Imagen de producción Next.js para Coolify / Docker.
#
# Coolify (importante):
# - Base Directory / contexto de build: carpeta "frontend" (este Dockerfile).
# - Puerto interno del contenedor: 3000 (o el PORT que inyecte Coolify; standalone lo respeta).
# - NO uses el Dockerfile de la raíz del repo (Python/API en 8000) para work.quillotana.cl.
#
# "Cannot GET /login" en el navegador suele ser Express u otro servicio, no Next: revisa que el
# dominio apunte al recurso del frontend y al puerto correcto.

FROM node:22-alpine AS builder
WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install

COPY . .
ENV NEXT_TELEMETRY_DISABLED=1
ENV NODE_ENV=production
RUN npm run build

FROM node:22-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
ENV HOSTNAME=0.0.0.0
ENV PORT=3000

RUN addgroup --system --gid 1001 nodejs && adduser --system --uid 1001 nextjs

# npm run build ya ejecuta copy-standalone-assets (public + static dentro de standalone)
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./

USER nextjs
EXPOSE 3000

CMD ["node", "server.js"]
