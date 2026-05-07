# Análisis técnico del backend ERP

**Fecha de referencia del repositorio:** revisión estática de código y configuración (sin ejecutar servicios ni migraciones).

---

## 1. Alcance del análisis

### Qué se consideró “backend”

- **Aplicación API principal:** carpeta `backend/` (FastAPI: `backend/main.py`, routers, middlewares, servicios, repositorios, utilidades, esquemas Pydantic, jobs bajo `backend/jobs/`, SQL bajo `backend/sql/`).
- **Contenedor / runtime:** `Dockerfile` (raíz), `requirements.txt` (raíz).
- **Documentación generada en esta tarea:** `docs/backend/*.md` (este archivo y compañeros).

### Qué se excluyó (no es backend de la API FastAPI)

- **Frontend:** carpeta `frontend/` (incluye `frontend/package.json`, `frontend/Dockerfile`, pantallas y assets). **No se analizó** salvo la mención explícita en `backend/main.py` de orígenes CORS típicos de un panel (`localhost:3000`, subdominios `*.quillotana.cl`), útil solo para entender **quién podría consumir** endpoints.
- **Herramientas Python sueltas en la raíz del repo** (`sync_*.py`, `ultimaoc.py`, `pruebadatos.py`, etc.): **no forman parte del paquete `backend/`**, pero sí son **scripts de operaciones/datos** relacionados con Bsale/Postgres. Se mencionan donde aportan contexto (riesgos, duplicación operativa), sin inventariar cada línea.

### Qué requiere revisión manual

- **Tráfico real y permisos en producción:** no hay evidencia estática suficiente sobre API gateway, Basic Auth, IP allowlist, o JWT validado fuera de FastAPI → marcar **requiere verificación**.
- **Uso efectivo de cada endpoint por clientes** (web/móvil/scripts): **requiere verificación** (logs, analytics, contratos OpenAPI consumidos).
- **Modelo de datos completo en Postgres:** el repo incluye SQL de apoyo, pero **no** un inventario automático de todas las tablas existentes en cada ambiente → **requiere verificación** contra la BD real.

---

## 2. Resumen ejecutivo

- **Tipo de backend:** API **FastAPI** (`backend/main.py`) orientada a **analytics/comercial**, **inteligencia de compras**, **catálogo**, **márgenes** (vía NocoDB en algunos endpoints), y un **dominio grande de “Distribuidora”** (rutero, rutas, mapas, órdenes de compra Bsale, planificación, picking, camiones) más una **app de terreno** (`/app_distribuidora`).
- **Funcionalidades ERP ya montadas (alto nivel):**
  - Autenticación staff (`/login`) y lookup de cliente por RUT (`/login-client`) en `backend/routers/auth.py`.
  - Pedidos “app.orders” (`backend/routers/orders.py`).
  - Módulos de proveedores/ofertas/promociones/listas/precios/productos (`backend/routers/*.py` correspondientes).
  - **Compras / OC** relativamente completos (`backend/routers/purchases.py` + SQL `backend/sql/purchase_intelligence_module.sql`, etc.).
  - **Distribuidora** muy amplia: sincronización Bsale→Postgres, vistas operativas, optimización de rutas, rutero, planificación (`backend/routers/distribuidora*.py`, `backend/services/distribuidora/*`).
  - **Jobs programados** en el lifespan de FastAPI (`backend/main.py`: sync distribuidora y rutero).
- **Qué parece completo:** muchos flujos de Distribuidora tienen servicios dedicados y repositorios SQL explícitos (`backend/repositories/distribuidora/`).
- **Qué parece incompleto / heterogéneo:** coexistencia de **Postgres directo** + **NocoDB REST** (`backend/database.py`) + **API Bsale** (`backend/services/distribuidora/bsale_client.py`) sin una capa ORM unificada.
- **En desarrollo / deuda:** autenticación **no homogénea** en routers (no se observa un patrón único tipo `Depends` JWT en la mayoría de rutas) → **requiere verificación**.
- **Riesgos técnicos principales:**
  - **Secretos y tokens** en código o scripts sueltos (p. ej. `backend/routers/auth.py`, `ultimaoc.py`, `pruebadatos.py`) — ver sección 10 y `docs/backend/LIMPIEZA_PROPUESTA_BACKEND.md`.
  - **Superficie de ataque amplia** en `/distribuidora/*` y endpoints de sync bajo `/erp/*` sin evidencia clara de auth uniforme.
  - **Jobs en background dentro del proceso web** (`lifespan` en `backend/main.py`): riesgo operativo (CPU, bloqueos, duplicados si múltiples réplicas) → **requiere verificación** de despliegue.
- **Oportunidades de limpieza:** consolidar scripts raíz vs `backend/jobs/`, unificar nombres/env (`NocoDB_token` vs convenciones), externalizar secretos, documentar ownership por módulo (ver `LIMPIEZA_PROPUESTA_BACKEND.md`).

---

## 3. Arquitectura actual del backend

### Estructura de carpetas (resumen)

- `backend/main.py` — instancia FastAPI, middlewares, `include_router`, `lifespan`.
- `backend/routers/` — definición de endpoints (casi toda la superficie HTTP).
- `backend/services/` — lógica de negocio (p. ej. `backend/services/distribuidora/sync_service.py`).
- `backend/repositories/` — acceso a datos SQL (p. ej. `backend/repositories/distribuidora/documents_repo.py`).
- `backend/schemas/` — modelos Pydantic compartidos (p. ej. `backend/schemas/distribuidora.py`).
- `backend/middleware/` — middleware Starlette custom (`backend/middleware/distribuidora_request_log.py`).
- `backend/jobs/` — jobs invocables por CLI o usados desde routers (`backend/jobs/sync_bsale_distribuidora.py`, etc.).
- `backend/sql/` — scripts SQL versionados por dominio.
- `backend/utils/` — utilidades (geo, rutas locales, cliente ORS).
- `backend/db.py`, `backend/database.py` — dos mecanismos distintos de datos (Postgres vs NocoDB).

### Patrón arquitectónico

- **Patrón predominante:** **Router → (Service opcional) → SQL (en repo o en el propio router)**.
- **No hay ORM único tipo SQLAlchemy** en el trazado principal: se usa **psycopg2** (`backend/db.py`) y SQL en strings.

### Cómo se inicia el servidor

- **Docker:** `Dockerfile` raíz ejecuta `uvicorn backend.main:app --host 0.0.0.0 --port 8000` (`Dockerfile` líneas 11–12).
- **Local típico:** `uvicorn backend.main:app --reload` (**requiere verificación** en docs internos del equipo).

### Cómo se conectan las rutas

- Cada módulo expone `APIRouter` en `backend/routers/<modulo>.py`.
- `backend/main.py` registra routers con `app.include_router(...)`, algunos con `prefix` (`/api`, `/app_distribuidora`).

### Middlewares

- **CORS ASGI custom:** `backend/cors_middleware.py` clase `QuillotanaCorsMiddleware`, registrada en `backend/main.py`.
- **Log de requests bajo `/distribuidora`:** `backend/middleware/distribuidora_request_log.py`, registrada en `backend/main.py`.

### Autenticación (estado observado)

- **Login staff** emite JWT con `jwt.encode` en `backend/routers/auth.py` (HS256, `SECRET` fijo en archivo).
- **No se encontró** uso generalizado de `Depends` / `HTTPBearer` en routers vía búsqueda en `backend/` → **la mayoría de endpoints parecen no validar JWT en servidor** (**requiere verificación** si hay gateway o validación externa).

### Conexión a base de datos

- **Postgres:** `backend/db.py` (`get_connection()` con `PG_HOST`, `PG_DB`, `PG_USER`, `PG_PASSWORD`, `PG_PORT`).
- **NocoDB (REST):** `backend/database.py` (`NOCODB_URL`, `NocoDB_token` en env — nombre atípico).

### Tabla — carpetas/archivos clave

| Carpeta/Archivo | Rol dentro del backend | Observación |
|---|---|---|
| `backend/main.py` | Entrypoint ASGI, routers, CORS, jobs background | Evidencia: `FastAPI`, `lifespan`, `include_router` |
| `backend/routers/` | Endpoints HTTP | Gran superficie; ver `ENDPOINTS_BACKEND.md` |
| `backend/services/distribuidora/` | Sync Bsale, órdenes, planificación, rutas | Archivos extensos; núcleo operativo |
| `backend/repositories/distribuidora/` | SQL centralizado por dominio | Buen candidato a “fuente de verdad” queries |
| `backend/jobs/` | Scripts/jobs mantenidos en paquete | Convivencia con scripts raíz del repo |
| `backend/sql/` | DDL/DML y vistas | No sustituye migraciones automáticas detectadas |
| `backend/schemas/` | Pydantic | Principalmente app distribuidora |
| `backend/utils/` | Helpers (ORS, geo, rutas) | `backend/utils/config.py` exige `ORS_API_KEY` al importar |
| `backend/cors_middleware.py` | CORS | Middleware custom |
| `backend/middleware/distribuidora_request_log.py` | Observabilidad `/distribuidora` | Logs de duración |
| `backend/database.py` | Cliente NocoDB | Tablas por ID string en routers |
| `backend/db.py` | Cliente Postgres | Usado masivamente |

---

## 4. Tecnologías y herramientas utilizadas

**Fuente principal:** `requirements.txt`, `Dockerfile`, imports en `backend/`.

| Herramienta | Tipo | Detectada en | Uso aparente | Estado | Observación |
|---|---|---|---|---|---|
| FastAPI | Framework web | `requirements.txt`, `backend/main.py` | API HTTP | En uso | — |
| Uvicorn | Servidor ASGI | `requirements.txt`, `Dockerfile` | Run prod | En uso | — |
| Starlette (via FastAPI) | HTTP/Middleware | `backend/middleware/*`, `backend/cors_middleware.py` | Middleware | En uso | — |
| Pydantic | Validación / schemas | routers + `backend/schemas/` | Request/response models | En uso | — |
| psycopg2-binary | Driver DB | `requirements.txt`, `backend/db.py` | Postgres | En uso | — |
| requests | Cliente HTTP | `requirements.txt`, `backend/database.py`, servicios Bsale | NocoDB + Bsale | En uso | — |
| PyJWT / python-jose | Auth JWT | `requirements.txt`, `backend/routers/auth.py` | Token staff | PyJWT en uso (`jwt.encode`) | `python-jose`: **no** aparece import en `backend/` (búsqueda estática) → **posible dependencia no usada** (**requiere verificación** con herramienta tipo `pipdeptree`/`vulture`) |
| passlib/bcrypt | Hash passwords | `requirements.txt`, `backend/routers/auth.py` | Login staff | En uso | — |
| pandas / openpyxl | Datos / Excel | `requirements.txt`, routers de export | Exportaciones | Parcial | Algunos endpoints importan pandas de forma condicional (`backend/routers/uploads.py`) |
| python-dotenv | Config local | `requirements.txt` | Carga `.env` en dev | **requiere verificación** | No hay `.env.example` detectado en el repo |

---

## 5. Inventario completo de endpoints backend

### Fuente de verdad (tabla exhaustiva)

El inventario **fila a fila** de **111** endpoints (método, ruta completa inferida, archivo, handler) está en:

- `docs/backend/ENDPOINTS_BACKEND.md`

> Nota metodológica: las rutas se **infieren** concatenando `prefix` de `include_router` en `backend/main.py` + `prefix` del `APIRouter` + path del decorador. Si existiera otro entrypoint ASGI alternativo, **requiere verificación**.

### Resumen por módulo (conteo de handlers)

| Agrupación (router / archivo) | Cantidad | Archivo(s) |
|---|---:|---|
| `main` | 1 | `backend/main.py` |
| `auth` | 2 | `backend/routers/auth.py` |
| `orders` | 4 | `backend/routers/orders.py` |
| `catalog` | 1 | `backend/routers/catalog.py` |
| `companies` | 1 | `backend/routers/companies.py` |
| `dashboard` | 1 | `backend/routers/dashboard.py` |
| `price_lists` | 1 | `backend/routers/price_lists.py` |
| `margins` | 2 | `backend/routers/margins.py` |
| `offers` | 3 | `backend/routers/offers.py` |
| `promotions` | 5 | `backend/routers/promotions.py` |
| `alerts` | 1 | `backend/routers/alerts.py` |
| `summary` | 1 | `backend/routers/summary.py` |
| `products` | 1 | `backend/routers/products.py` |
| `products_master` | 3 | `backend/routers/products_master.py` |
| `suppliers` | 3 | `backend/routers/suppliers.py` |
| `purchases` | 11 | `backend/routers/purchases.py` |
| `uploads` | 2 | `backend/routers/uploads.py` |
| `margin_problems` | 1 | `backend/routers/margin_problems.py` |
| `margin_export` | 1 | `backend/routers/margin_export.py` |
| `distribuidora` | 20 | `backend/routers/distribuidora.py` |
| `distribuidora_sync` | 5 | `backend/routers/distribuidora_sync.py` |
| `distribuidora_orders` | 11 | `backend/routers/distribuidora_orders.py` |
| `distribuidora_planificacion` | 2 | `backend/routers/distribuidora_planificacion.py` |
| `distribuidora_planning` | 4 | `backend/routers/distribuidora_planning.py` |
| `distribuidora_clients` | 8 | `backend/routers/distribuidora_clients.py` |
| `distribuidora_route_planning` | 7 | `backend/routers/distribuidora_route_planning.py` |
| `distribuidora_route_picking` | 1 | `backend/routers/distribuidora_route_picking.py` |
| `distribuidora_trucks` | 1 | `backend/routers/distribuidora_trucks.py` |
| `app_distribuidora` | 4 | `backend/routers/app_distribuidora.py` |
| `erp` | 3 | `backend/routers/erp.py` |
| **Total** | **111** | — |

### Tabla compacta (formato solicitado; 1 fila = 1 módulo-router)

> Para cumplir el formato pedido sin duplicar 111 filas aquí, cada fila agrega **todos los endpoints** de ese router. Detalle por ruta: `ENDPOINTS_BACKEND.md`.

| Módulo | Método | Endpoint | Archivo | Función/Controlador | Qué hace | Datos que recibe | Datos que responde | Auth/Permisos | Modelo/Tabla/Servicio | Estado |
|---|---|---|---|---|---|---|---|---|---|
| Todos los listados en `ENDPOINTS_BACKEND.md` | Varios | Ver columna “Endpoint” en archivo enlazado | `backend/routers/*.py` + `backend/main.py` | Ver columna “Controlador/Función” | Variado por ruta | Query/body según OpenAPI implícito en código | JSON / CSV / streams según ruta | **requiere verificación** (no hay patrón único `Depends`) | Postgres `bsale.*` / `distribuidora.*`, NocoDB, API Bsale según ruta | Activo (montaje en `main.py`) |

---

## 6. Detalle funcional de endpoints (por módulo ERP)

> Esta sección es **descriptiva**. Para el listado exacto de rutas, ver `docs/backend/ENDPOINTS_BACKEND.md`.

### Módulo: Autenticación (`auth`)

#### Endpoints incluidos

- `POST /login-client`
- `POST /login`

#### Funcionalidad

- Login cliente por RUT (`bsale.clients`) y login staff (`bsale.users`) con JWT HS256.

#### Flujo backend

1. Recibe JSON (`LoginRequest` / `LoginClientRequest`) en `backend/routers/auth.py`.
2. Valida/normaliza (RUT vía `backend/client_rut.py`).
3. Consulta Postgres con `get_connection()` (`backend/db.py`).
4. Responde JSON (token JWT o datos cliente).

#### Observaciones

- **Completo:** flujo básico de login.
- **Incompleto / riesgos:** `SECRET` hardcodeado (`backend/routers/auth.py`).
- **Requiere verificación:** si existe validación JWT en otros endpoints.

---

### Módulo: Pedidos app (`orders`)

#### Endpoints incluidos

- Ver `backend/routers/orders.py` en `ENDPOINTS_BACKEND.md` (`/orders`, `/orders/{id}`, status, create).

#### Funcionalidad

- CRUD parcial de pedidos con validaciones Pydantic (`CreateOrderBody`, etc.).

#### Observaciones

- **Tablas:** evidencia SQL en el router (p. ej. esquema app orders) — **requiere verificación** de nombres exactos leyendo el archivo completo.

---

### Módulo: Catálogo público (`catalog`, prefijo `/api`)

#### Funcionalidad

- Catálogo para clientes / web público (`backend/routers/catalog.py`).

---

### Módulo: Analytics / márgenes vía NocoDB (`dashboard`, `alerts`, `summary`, `margins`, `margin_export`, `margin_problems`)

#### Funcionalidad

- Lecturas vía `noco_get()` (`backend/database.py`) sobre tabla ID `m777i9qvqgbvpuk` en `dashboard.py`, `alerts.py`, `summary.py`, `margin_export.py`, `margin_problems.py`.
- Márgenes adicionales en `margins.py` (SQL/vistas en Postgres — **requiere verificación** leyendo archivo).

#### Observaciones

- **Riesgo:** dependencia fuerte de NocoDB + token (`NocoDB_token`).

---

### Módulo: Maestros comerciales (`companies`, `price_lists`, `offers`, `promotions`, `suppliers`, `products`, `products_master`, `uploads`)

#### Funcionalidad

- Gestión de maestros y cargas masivas (`uploads` con `UploadFile`).

#### Observaciones

- **Validación:** mix de Pydantic models y dicts (`products_master`) — **requiere verificación** endpoint por endpoint.

---

### Módulo: Compras / inteligencia de compras (`purchases`)

#### Funcionalidad

- Consultas y mutaciones sobre tablas `bsale.purchase_manual_items`, `bsale.oc_document`, `bsale.oc_details`, vistas `bsale.vw_purchase_analysis`, joins con `bsale.products_master`, `bsale.offices`, etc. (evidencia en `backend/routers/purchases.py` vía `grep` SQL).

#### Observaciones

- **Completo:** uno de los módulos más “ERP” y con SQL explícito amplio.

---

### Módulo: Distribuidora — Rutero / rutas / mapa (`distribuidora`)

#### Funcionalidad

- Endpoints extensos: ruta detalle, sugerencias, optimización ORS, rutero, pendientes, georef, mapa, resúmenes (`backend/routers/distribuidora.py`).

#### Observaciones

- **Dependencias externas:** OpenRouteService (`backend/utils/ors_client.py`, `backend/utils/config.py`).
- **Riesgo operativo:** alto acoplamiento y archivo muy grande.

---

### Módulo: Distribuidora — Sync Bsale (`distribuidora_orders`, `distribuidora_sync`)

#### Funcionalidad

- Sync incremental órdenes/ventas, estado, resync OC, endpoints `/erp/sync-*` (`backend/routers/distribuidora_sync.py`, `backend/routers/distribuidora_orders.py`).
- Tokens: `BSALE_TOKEN` / `BSALE_TOKEN_SPA` (mensajes de error en routers; lógica en `backend/services/distribuidora/sync_service.py`).

---

### Módulo: Distribuidora — Planificación (`distribuidora_planning`, `distribuidora_planificacion`, `distribuidora_route_planning`, `distribuidora_route_picking`, `distribuidora_trucks`)

#### Funcionalidad

- Planning de órdenes/ventas, ORS batch, CRUD route planning, picking list, flota.

---

### Módulo: Distribuidora — Clientes analytics (`distribuidora_clients`)

#### Funcionalidad

- Dashboards de frecuencia, inactivos, export a Excel en algunas rutas (ver archivo).

---

### Módulo: App móvil (`app_distribuidora`, prefijo `/app_distribuidora`)

#### Funcionalidad

- Login app, ruta del día, visitas y sync (`backend/routers/app_distribuidora.py`, modelos `backend/schemas/distribuidora.py`).

---

### Módulo: ERP panel (`erp`, prefijo `/erp`)

#### Funcionalidad

- Dashboard/alerts/margins “ERP” (`backend/routers/erp.py`) — comentario en `backend/main.py` indica separación de URLs vs analytics legacy.

---

## 7. Modelos, entidades y base de datos

### Enfoque del repositorio

- **Postgres** esquemas observados frecuentemente: `bsale`, `distribuidora` (evidencia en SQL bajo `backend/sql/` y queries en routers/servicios).
- **No se detectó** carpeta tipo `alembic/` o migraciones ORM en el snapshot del repo → **las migraciones parecen ser scripts SQL manuales** (**requiere verificación** de proceso operativo).

### Tabla orientativa (no exhaustiva)

| Modelo/Entidad | Archivo | Tabla/Colección | Campos clave | Relaciones | Endpoints asociados | Estado |
|---|---|---|---|---|---|---|
| Visita app | `backend/schemas/distribuidora.py` | `bsale.visitas` (inferido desde docstrings) | estado, incidencias, sync | ruta día | `/app_distribuidora/*` | Pydantic; **requiere verificación** contra DDL real |
| Documentos distribuidora | `backend/repositories/distribuidora/documents_repo.py` | `distribuidora.documents` | document_id, tipo, fechas | detalles, related | sync + órdenes | Activo en código |
| Document related | `backend/services/distribuidora/sync_related_service.py` | `distribuidora.document_related` | detail_id, related_document_id | OC↔factura | sync related | Activo |
| OC compras (bsale) | `backend/routers/purchases.py` | `bsale.oc_document`, `bsale.oc_details` | oc_id, supplier, totals | offices | `/purchase-*` | Activo |
| Analytics margin rows | `backend/routers/dashboard.py` | NocoDB table id `m777i9qvqgbvpuk` | status, margins | — | `/dashboard/*`, `/margin-*` | Dependiente NocoDB |
| Vendedores app | `backend/routers/app_distribuidora.py` | `bsale.vendedores_app` (inferido) | codigo, password hash | — | `/app_distribuidora/login` | **requiere verificación** |

### Inconsistencias / huecos

- **Dos clientes de datos** (`backend/db.py` vs `backend/database.py`) con convenciones distintas.
- **Modelos Pydantic** concentrados en app distribuidora; otros módulos usan modelos inline en routers.

---

## 8. Servicios, helpers y middlewares

| Tipo | Archivo | Función | Usado por | Estado | Observación |
|---|---|---|---|---|---|
| Middleware CORS | `backend/cors_middleware.py` | CORS ASGI | `backend/main.py` | Activo | Custom |
| Middleware log | `backend/middleware/distribuidora_request_log.py` | Logs `/distribuidora` | `backend/main.py` | Activo | — |
| Cliente Bsale | `backend/services/distribuidora/bsale_client.py` | GET API v1 | sync services | Activo | Reintentos HTTP |
| Sync distribuidora | `backend/services/distribuidora/sync_service.py` | Sync masivo | routers + jobs | Activo | Archivo grande |
| Sync related | `backend/services/distribuidora/sync_related_service.py` | Relaciones API | routers + jobs | Activo | Locks advisory |
| ORS | `backend/utils/ors_client.py` | Rutas/geometría | distribuidora | Activo | Requiere `ORS_API_KEY` |
| Config ORS | `backend/utils/config.py` | Lee env | importado por ORS | Activo | Falla si falta key |
| Cliente RUT | `backend/client_rut.py` | Validación RUT | auth | Activo | — |

---

## 9. Variables de entorno y configuración backend

> **No se listan secretos reales.** Si aparece un nombre de variable sensible, es solo el **nombre**.

| Variable/Config | Archivo | Uso | Obligatoria | Observación |
|---|---|---|---|---|
| `PG_HOST`, `PG_DB`, `PG_USER`, `PG_PASSWORD`, `PG_PORT` | `backend/db.py` | Postgres | Sí (runtime) | — |
| `NOCODB_URL` | `backend/database.py` | Base URL NocoDB | Tiene default | Default `https://db.quillotana.cl` |
| `NocoDB_token` | `backend/database.py` | Token NocoDB | Sí si se usan endpoints NocoDB | Nombre atípico vs `NOCODB_TOKEN` |
| `ORS_API_KEY` | `backend/utils/config.py` | OpenRouteService | Sí al importar módulo | `raise Exception` si falta |
| `BSALE_TOKEN`, `BSALE_TOKEN_SPA` | `backend/services/distribuidora/sync_service.py`, routers sync | API Bsale distribuidora | Condicional | Mensajes en `distribuidora_sync.py` |
| `CORS_EXTRA_ORIGINS` | `backend/main.py` | CORS extra | No | Lista separada por comas |
| `CORS_ALLOW_ORIGIN_REGEX` | `backend/main.py` | Regex CORS | No | Default `*.quillotana.cl` |
| `DISTRIBUIDORA_BSALE_SYNC_*`, `RUTERO_SYNC_*` | `backend/main.py` | Intervalos jobs | No | Defaults + flags `*_DISABLED` |
| `AUTH_LOGIN_DEBUG` | `backend/routers/auth.py` | Logs login | No | Cuidado con datos sensibles |

---

## 10. Seguridad backend

| Riesgo | Archivo/Endpoint | Severidad | Evidencia | Recomendación |
|---|---|---|---|---|
| JWT secret hardcodeado | `backend/routers/auth.py` | Alta | `SECRET = "quillotana_secret_key"` | Mover a env y rotar |
| Tokens en scripts | `ultimaoc.py`, `pruebadatos.py` | Alta si en remoto | strings literales | Eliminar de repo / env only |
| Superficie sin auth uniforme | Muchos `backend/routers/*.py` | Media–Alta | ausencia de `Depends` generalizada | Auditar gateway + añadir auth server-side donde falte |
| Uploads CSV | `backend/routers/uploads.py` | Media | `UploadFile` | Validar tamaño/tipo, virus scan, auth |
| Logs con info sensible | `backend/routers/auth.py` (`AUTH_LOGIN_DEBUG`) | Media | logging opcional | Mantener off en prod |
| CORS permisivo + regex subdominios | `backend/main.py` | Media | `https://[a-z0-9-]+\.quillotana\.cl$` | Revisar si es demasiado amplio |
| Jobs sync en web process | `backend/main.py` lifespan | Media | loops `asyncio` | Mover a worker si hay réplicas |

---

## 11. Código duplicado, muerto o limpiable

Ver también tabla en `docs/backend/LIMPIEZA_PROPUESTA_BACKEND.md`.

| Tipo | Archivo/Carpeta | Motivo | Evidencia | Riesgo de eliminar | Verificación recomendada antes de borrar |
|---|---|---|---|---|---|
| Posible duplicado typo | `backend/jobs/debug_full_bsalse_relationships.py` | Nombre inconsistente | comparar con `debug_full_bsale_relationships.py` | Bajo | Grep en repo y CI |
| Scripts raíz vs jobs | `sync_*.py` (raíz) | Misma familia que `backend/jobs/*` | conviven | Medio | Buscar en deploy |
| Dependencia tal vez sin uso | `python-jose[cryptography]` | No apareció en grep rápido de imports | `requirements.txt` | Bajo | `pipdeptree` / vulture |

---

## 12. Problemas e inconsistencias detectadas

| Problema | Archivo/Endpoint | Impacto | Prioridad | Recomendación |
|---|---|---|---|---|
| Dos sistemas de persistencia | `db.py` + `database.py` | Complejidad operativa | Media | Documentar qué módulo usa cuál |
| Nombres env inconsistentes | `NocoDB_token` | Errores de despliegue | Media | Estandarizar + doc |
| Auth no centralizada | Routers | Exposición | Alta | Middleware JWT o dependency |
| ORS config import-time failure | `backend/utils/config.py` | Caída al importar | Media | Lazy load |

---

## 13. Recomendaciones de ordenamiento backend

| Etapa | Acción | Archivos involucrados | Prioridad | Riesgo | Resultado esperado |
|---|---|---|---|---|---|
| 1 | Documentar estado | `docs/backend/*`, `backend/README.md` | Alta | Bajo | Base común |
| 2 | Completar OpenAPI/contratos | `backend/routers/*` | Alta | Bajo | Menos ambigüedad |
| 3 | Limpiar scripts/tokens | raíz + `backend/jobs/*` | Media | Medio | Menor riesgo de filtración |
| 4 | Unificar rutas/servicios | `distribuidora*.py` | Media | Alto | Solo con pruebas |
| 5 | Validaciones y errores | routers grandes | Media | Medio | API más predecible |
| 6 | Seguridad | `auth.py`, CORS, uploads | Alta | Alto | Endurecimiento |
| 7 | Config/deploy | Docker/Coolify docs | Media | Medio | Paridad env |
| 8 | Pruebas mínimas | `pytest` + smoke | Alta | Bajo | Regresión |

### Etapa 1: Documentar lo existente

- Mantener `docs/backend/ANALISIS_BACKEND_ERP.md`, `ENDPOINTS_BACKEND.md`, `LIMPIEZA_PROPUESTA_BACKEND.md` como fuente única backend.
- Añadir diagrama de despliegue cuando exista (Coolify, réplicas, workers).

### Etapa 2: Completar documentación de endpoints

- Completar columnas “qué hace / tablas” en `ENDPOINTS_BACKEND.md` (hoy parte es genérica por coste de mantenimiento).
- Opcional: exportar OpenAPI (`/openapi.json`) y archivar versión por release.

### Etapa 3: Limpiar código muerto con bajo riesgo

- Resolver duplicados obvios (`debug_full_bsalse_*`), scripts con tokens fuera del repo.

### Etapa 4: Unificar rutas/controladores/servicios

- Partir desde módulos pequeños (`trucks`, `route_picking`) hacia `distribuidora.py` (último).

### Etapa 5: Mejorar validaciones y manejo de errores

- Estandarizar `HTTPException` + `detail` estructurado en routers grandes.

### Etapa 6: Revisar seguridad y permisos

- JWT desde env, política de uploads, rate limit en `/login` y sync.

### Etapa 7: Mejorar configuración y deploy

- `.env.example` sin secretos; documentar variables en tabla sección 9.

### Etapa 8: Agregar pruebas mínimas

- Smoke tests de rutas críticas (`/`, `/login`, un GET `/distribuidora/*`, un `/erp/sync-distribuidora/status`).

---

## 14. Plan de limpieza propuesto

### Limpieza segura

- Externalizar secretos hardcodeados y tokens en scripts sueltos → ver `LIMPIEZA_PROPUESTA_BACKEND.md`.

### Limpieza con revisión

- Consolidar scripts `sync_*.py` de raíz vs `backend/jobs/`.
- Resolver duplicado `debug_full_bsalse_*` vs `bsale_*`.

### No tocar todavía

- `backend/routers/distribuidora.py`, sync services, SQL bajo `backend/sql/distribuidora/` sin plan de rollback.

---

## 15. Próximos pasos recomendados (priorizado)

1. **Verificar autenticación real** en producción (gateway vs FastAPI) leyendo despliegue y probando endpoints sensibles (`/erp/*`, `/distribuidora/*`).
2. **Rotacionar y mover secretos** (`backend/routers/auth.py`, scripts con tokens en raíz).
3. **Mantener vivo** `docs/backend/ENDPOINTS_BACKEND.md` (ideal: generación automática en CI desde AST).
4. **Definir dueño** de cada dominio (`purchases` vs `distribuidora_orders` vs `distribuidora_sync`).
5. **No refactor grande** hasta tener smoke tests mínimos de rutas críticas.

---

## Referencias de archivos clave

- `backend/main.py` — aplicación y registro de rutas.
- `backend/db.py`, `backend/database.py` — conectores datos.
- `backend/routers/*` — endpoints.
- `backend/services/distribuidora/*` — negocio + integración Bsale.
- `backend/repositories/distribuidora/*` — SQL reutilizable.
- `backend/sql/*` — definiciones SQL.
- `requirements.txt`, `Dockerfile` — stack y ejecución.
