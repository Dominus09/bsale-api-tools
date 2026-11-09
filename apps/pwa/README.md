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

## Coolify (propuesta — no configurar aún)

| Campo | Valor |
|---|---|
| Base Directory | `apps/pwa` |
| Dockerfile | `Dockerfile` |
| Build Pack | Dockerfile |
| Puerto | `3000` |
| Dominio | `app.quillotana.cl` |
| Build arg | `NEXT_PUBLIC_API_URL=https://api.quillotana.cl` |

Redeploy manual. No tocar el recurso del ERP (`frontend/`).

## Seguridad P1

- Sin cache de API en service worker
- Sin documentos offline
- Ownership solo por `dispatch_plan.driver_user_id`
- Logout limpia `localStorage` de sesión PWA
