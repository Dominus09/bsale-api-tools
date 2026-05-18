# Panel operacional Quillotana

Monitoreo de vendedores, rutas e incidencias para el equipo de operaciones. **Solo lectura** sobre las mismas tablas que usa la app móvil (`/app_distribuidora`); no modifica contratos ni sync offline.

## Arquitectura

```
App móvil  →  POST /app_distribuidora/*  →  bsale.rutas_dia, bsale.visitas
Panel web  →  GET  /operaciones/*        →  (mismas tablas, JWT staff)
```

| Capa | Ubicación |
|------|-----------|
| API | `backend/routers/operaciones.py` |
| Lógica | `backend/services/operaciones_service.py` |
| Schemas | `backend/schemas/operaciones.py` |
| Auth staff | `backend/utils/auth_staff.py` (Bearer JWT de `/login`) |
| Cliente web | `frontend/services/operaciones.ts` |
| UI | `frontend/app/(dashboard)/operaciones/**` |
| Componentes | `frontend/components/operaciones/**` |

## Endpoints REST

Prefijo: `/operaciones`. Todos requieren header `Authorization: Bearer <token>` (mismo token del login ERP).

| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/operaciones/dashboard?fecha=YYYY-MM-DD` | KPIs + resumen vendedores |
| GET | `/operaciones/vendedores?fecha=` | Tabla operacional |
| GET | `/operaciones/vendedor/{codigo}?fecha=` | Timeline, incidencias, km, cumplimiento |
| GET | `/operaciones/ruta/{ruta_id}` | Marcadores mapa (visitado / pendiente / incidencia) |
| GET | `/operaciones/incidencias?fecha=&vendedor=&limit=` | Listado filtrable |
| GET | `/operaciones/metricas?fecha=` | KPIs + desglose por vendedor |

Documentación interactiva: `http://localhost:8000/docs` (tag **Operaciones Quillotana**).

## Datos y reglas de negocio

- **Incidencias**: filas en `bsale.visitas` con `estado = 'incidencia'` (no hay tabla separada).
- **GPS actual**: última visita del día con `lat`/`lon` no nulos (no hay tracking en tiempo real en BD).
- **Batería**: `null` hasta que la app envíe ese campo.
- **Estado conexión** (`activo` / `atrasado` / `offline`):
  - `offline`: vendedor inactivo, sin ruta, o `updated_at` de ruta > `OPERACIONES_OFFLINE_MINUTES` (default 15).
  - `atrasado`: cumplimiento &lt; `OPERACIONES_ATRASADO_PCT` (default 50%) con ruta activa.
  - `activo`: resto.

## Variables de entorno

### Backend (`.env` en raíz o donde cargue `backend`)

| Variable | Default | Uso |
|----------|---------|-----|
| `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD` | — | PostgreSQL (obligatorio) |
| `JWT_SECRET_KEY` o el `SECRET` usado en `backend/routers/auth.py` | — | Validar JWT staff |
| `OPERACIONES_OFFLINE_MINUTES` | `15` | Umbral offline |
| `OPERACIONES_ATRASADO_PCT` | `50` | Umbral atrasado (%) |

### Frontend (`frontend/.env.local`)

| Variable | Default | Uso |
|----------|---------|-----|
| `NEXT_PUBLIC_API_URL` | proxy dev | Base API (ej. `http://localhost:8000`) |
| `NEXT_PUBLIC_OPERACIONES_POLL_MS` | `30000` | Intervalo polling dashboard/vendedores |

## Ejecución local

### Backend

```bash
cd "C:\Users\user\OneDrive\Proyectos cursor\bsale-api-tools"
# venv activado si aplica
uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

### Frontend

```bash
cd frontend
npm install
npm run dev
```

Abrir `http://localhost:3000`, iniciar sesión en el panel ERP, luego **Operaciones → Panel operaciones** (`/operaciones/dashboard`).

## Prueba del dashboard completo

1. **Login**: credenciales ERP → token en `localStorage` (el cliente usa `getAuthHeaders()`).
2. **Datos**: debe existir al menos una fila en `bsale.rutas_dia` para la fecha y visitas asociadas (generadas por la app móvil o seeds de prueba).
3. **Dashboard**: KPIs y tabla resumen; polling cada 30 s (configurable).
4. **Vendedores**: estados verde/amarillo/rojo; clic en nombre → detalle.
5. **Mapa**: elegir vendedor con ruta o URL `/operaciones/mapa?ruta=<id>`.
6. **Incidencias**: tabla con filtros por fecha.
7. **API directa** (opcional):

```bash
curl -H "Authorization: Bearer TOKEN" "http://localhost:8000/operaciones/dashboard?fecha=2026-05-18"
```

## Rutas frontend

| Ruta | Vista |
|------|--------|
| `/operaciones/dashboard` | KPIs + resumen |
| `/operaciones/vendedores` | Tabla vendedores |
| `/operaciones/vendedor/[codigo]` | Detalle día |
| `/operaciones/mapa?ruta=` | Leaflet |
| `/operaciones/incidencias` | Incidencias |

## Árbol de archivos creados

```
backend/
  utils/auth_staff.py
  schemas/operaciones.py
  services/operaciones_service.py
  routers/operaciones.py
  main.py                          # include_router(operaciones_router)

frontend/
  services/operaciones.ts
  hooks/use-operaciones-poll.ts
  components/operaciones/
    estado-badge.tsx
    kpi-cards.tsx
    vendedores-table.tsx
    mapa-operacional-client.tsx
  app/(dashboard)/operaciones/
    page.tsx                       # redirect → dashboard
    dashboard/page.tsx
    vendedores/page.tsx
    vendedor/[codigo]/page.tsx
    mapa/page.tsx
    incidencias/page.tsx
  components/layout/sidebar.tsx    # enlaces Operaciones

docs/backend/OPERACIONES_PANEL.md   # este archivo
```

## Deploy futuro

- Montar el mismo backend con variables `PG_*` y `JWT_SECRET_KEY`.
- Build Next: `cd frontend && npm run build`; servir con `NEXT_PUBLIC_API_URL` apuntando al API en producción.
- CORS: ya configurado en `backend/main.py` para orígenes del panel.
- No requiere cambios en la app móvil ni en `/app_distribuidora`.

## Limitaciones conocidas

- Mapa: una ruta por query (`ruta_id`); no hay capa multi-ruta global en una sola vista.
- Kilómetros: estimación Haversine entre visitas con coordenadas del día.
- Tiempo real: polling HTTP (no WebSocket).
