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
| GET | `/operaciones/foto/{visita_id}` | Imagen evidencia (JWT, archivo o redirect URL) |
| POST | `/operaciones/heartbeat` | Telemetría app móvil → `{ "ack": true, "server_timestamp": "..." }` |
| POST | `/app_distribuidora/heartbeat` | **Alias** (misma lógica; si la app usa esa base URL) |
| POST | `/app_distribuidora/operaciones/heartbeat` | **Alias** (path relativo `operaciones/heartbeat`) |
| POST | `/operaciones/gps_track` | Punto GPS tracking → `{ "ack": true, "server_timestamp": "..." }` |
| POST | `/app_distribuidora/gps_track` | **Alias** GPS track |
| POST | `/app_distribuidora/operaciones/gps_track` | **Alias** path relativo |

Documentación interactiva: `http://localhost:8000/docs` (tag **Operaciones Quillotana**).

## Datos y reglas de negocio

- **Visitas realizadas** (KPIs, tabla vendedores, avance): `COUNT` en `bsale.visitas` con `estado IN ('visitado', 'incidencia')`. Las incidencias **sí cuentan** como cierre operacional. **Pendientes** = `total_clientes - visitas_realizadas` (no se usa `rutas_dia.clientes_visitados`, que la app móvil no recalcula en sync).
- **Incidencias** (contador aparte): filas con `estado = 'incidencia'`.
- **GPS actual**: última visita del día con `lat`/`lon` no nulos (no hay tracking en tiempo real en BD).
- **Batería**: `null` hasta que la app envíe ese campo.
- **Fotos incidencias**: al sincronizar, si `foto_url` es `data:image/...;base64,...` se guarda en disco (`VISITA_FOTOS_DIR`, default `data/uploads/visitas/`) y en BD queda clave `visitas/{id}.jpg`. El panel las sirve en `GET /operaciones/foto/{visita_id}` (JWT). Legacy: `data:` en BD sigue mostrándose; sin imagen → placeholder en UI.
- **Estado conexión** (prioridad heartbeat, fallback legacy):
  - Con filas en `bsale.operaciones_heartbeat` del día: último pulso &lt; **2 min** → `activo` (Online); **2–10 min** → `atrasado`; **&gt; 10 min** → `offline`.
  - Sin heartbeat: lógica anterior (`rutas_dia.updated_at`, cumplimiento, `OPERACIONES_OFFLINE_MINUTES`).
- **Última sync / GPS / km / batería**: heartbeat + **`operaciones_gps_track`**; km preferente desde trazas GPS; posición = punto más reciente entre ambas fuentes.

## Variables de entorno

### Backend (`.env` en raíz o donde cargue `backend`)

| Variable | Default | Uso |
|----------|---------|-----|
| `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USER`, `PG_PASSWORD` | — | PostgreSQL (obligatorio) |
| `JWT_SECRET_KEY` o el `SECRET` usado en `backend/routers/auth.py` | — | Validar JWT staff |
| `OPERACIONES_OFFLINE_MINUTES` | `15` | Umbral offline |
| `OPERACIONES_ATRASADO_PCT` | `50` | Umbral atrasado (%) |
| `VISITA_FOTOS_DIR` | `data/uploads/visitas` | Directorio fotos incidencias (filesystem) |
| `OPERACIONES_HEARTBEAT_ONLINE_MINUTES` | `2` | Umbral Online |
| `OPERACIONES_HEARTBEAT_ATRASADO_MINUTES` | `10` | Umbral Offline (mayor a esto) |
| `OPERACIONES_HEARTBEAT_API_KEY` | — | Si se define, la app debe enviar `X-Heartbeat-Key` |

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

## Migración heartbeat

```bash
psql -f sql/bsale_operaciones_heartbeat.sql
```

## Prueba heartbeat (app / curl)

```bash
curl -X POST http://localhost:8000/operaciones/heartbeat \
  -H "Content-Type: application/json" \
  -H "X-Heartbeat-Key: TU_CLAVE_OPCIONAL" \
  -d '{
    "vendedor_id": "vendedor_1",
    "timestamp": "2026-05-18T15:00:00Z",
    "lat": -33.45,
    "lng": -71.23,
    "bateria": 87,
    "conexion": "wifi",
    "pendientes": 2,
    "app_version": "1.2.0",
    "dispositivo": "Android 14"
  }'

# Respuesta esperada:
# {"ack":true,"server_timestamp":"2026-05-18T15:00:01.123456+00:00"}

# Si la app usa base /app_distribuidora:
curl -X POST http://localhost:8000/app_distribuidora/heartbeat ...

## Prueba gps_track (vacía cola móvil)

```bash
curl -X POST http://localhost:8000/operaciones/gps_track \
  -H "Content-Type: application/json" \
  -d '{
    "vendedor_id": "vendedor_1",
    "timestamp": "2026-05-18T20:05:00Z",
    "lat": -33.451,
    "lng": -71.231,
    "accuracy": 12.5,
    "speed": 4.2,
    "battery": 78,
    "app_version": "1.0.0"
  }'
# {"ack":true,"server_timestamp":"..."}
```
```

Repetir cada 30–60 s y abrir el dashboard (hoy): badge **Online** si el último pulso &lt; 2 min.

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
  services/heartbeat_service.py
  services/operaciones_visitas.py
  services/visita_foto_service.py
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
    incidencia-foto.tsx
  app/(dashboard)/operaciones/
    page.tsx                       # redirect → dashboard
    dashboard/page.tsx
    vendedores/page.tsx
    vendedor/[codigo]/page.tsx
    mapa/page.tsx
    incidencias/page.tsx
  components/layout/sidebar.tsx    # enlaces Operaciones

sql/bsale_operaciones_heartbeat.sql
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
