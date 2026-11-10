# Quillotana Operativa (`apps/pwa`)

PWA móvil para choferes. Consume `api.quillotana.cl`. Dominio futuro: `app.quillotana.cl`.

## Stack

- Next.js 16 (App Router) · React 19 · standalone Docker
- Auth: JWT Bearer + `user_apps` / `user_operational_profiles` (perfil `chofer`)
- API P1: `GET /distribuidora/pwa/driver/despachos` (+ detalle)

## Desarrollo local

```bash
cd apps/pwa
npm install
# opcional: NEXT_PUBLIC_API_URL=http://127.0.0.1:8000
npm run dev   # http://localhost:3001
```

## Coolify

| Campo | Valor |
|---|---|
| Base Directory | `apps/pwa` |
| Dockerfile | `Dockerfile` |
| Build Pack | Dockerfile |
| Puerto | `3000` |
| Dominio | `app.quillotana.cl` |
| Build arg | `NEXT_PUBLIC_API_URL=https://api.quillotana.cl` |
| Build arg | `NEXT_PUBLIC_OPS_WHATSAPP_INVITE_URL` (invite del grupo operativo; vacío → `wa.me`) |
| Runtime env | `API_PROXY_TARGET=http://<hostname-interno-api>:8000` |

`NEXT_PUBLIC_OPS_WHATSAPP_INVITE_URL` es **build-arg** (se incrusta en el bundle). Cambiar el grupo requiere rebuild/redeploy de la PWA; no hardcodear el enlace en el repo.

`API_PROXY_TARGET` es **runtime** (no build arg). Debe ser el hostname Docker/Coolify del API en la misma red, puerto interno 8000. Sin ese valor el proxy usa DNS público (`api.quillotana.cl`) y puede fallar con `EAI_AGAIN`.

Verificar desde el contenedor PWA: `getent hosts <hostname>` y `node scripts/probe-api-upstream.mjs`.

Redeploy manual. No tocar el recurso del ERP (`frontend/`).

## Seguridad P1

- Sin cache de API en service worker
- Sin documentos offline
- Ownership solo por `dispatch_plan.driver_user_id`
- Logout limpia `localStorage` de sesión PWA
