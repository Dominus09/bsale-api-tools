# Fase 1A.1 — Matriz de seguridad de endpoints (hardening)

Rama: `auth-multiapp-foundation`. Base commit 1A: `f3b734f`.  
Guard reutilizable: `require_staff_user` → `require_erp_app` (JWT + legacy ERP / deniega PWA-only).  
`assert_company` / `assert_office` disponibles; aplicados donde el contrato tiene `company_id` claro.

## Leyenda

| Estado | Significado |
|---|---|
| **OK** | Protegido en 1A.1 (sesión ERP) |
| **PREV** | Ya tenía auth antes de 1A.1 |
| **EXC** | Excepción documentada — no endurecido aún |
| **PEND** | Pendiente de hardening (menor riesgo relativo o alcance amplio) |

Consumidor: **ERP** = frontend staff; **MOB** = app móvil vendedores; **CRON** = jobs Python; **INT** = integración/API key.

---

## A. Protegidos en 1A.1 (antes abiertos o incompletos)

| Método | Ruta | Auth | Scope | Consumidor | Riesgo residual | Protección |
|---|---|---|---|---|---|---|
| GET | `/companies` | OK `require_staff_user` | — | ERP | Bajo | Sesión ERP |
| GET | `/dashboard/{company_id}` | OK + `assert_company` | company | ERP | Medio si emit off | ERP + company si `company_ids` emitidos |
| * | `/distribuidora/*` (rutero, mapa, sync-rutero, orden-manual, …) | OK router deps | — | ERP | Escritura de rutas | ERP |
| * | `/distribuidora/planificacion/*` | OK | — | ERP | Escritura planificación | ERP |
| * | `/distribuidora/dispatch-plans/*` | OK (antes parcial) | — | ERP | Predespacho | ERP (contratos API intactos) |
| * | `/distribuidora` route-planning / trucks / route-picking / cuadraturas | OK | — | ERP | Ops logística | ERP |
| * | `/distribuidora` orders/purchase, dispatch-prep, resync-oc, clients, planning views | OK | — | ERP | Datos OC / clientes | ERP |
| POST | `/upload/suppliers`, `/upload/offers` | OK | — | ERP | Carga masiva | ERP |
| * | `/erp/sync-distribuidora*`, `/sync/*` | OK | — | ERP UI | Disparo sync | ERP; **crons usan servicios Python, no HTTP** |

## B. Ya protegidos (PREV) — sin cambio de contrato

| Área | Rutas | Auth |
|---|---|---|
| Cargas | `/cargas/*` | PREV `require_staff_user` |
| Cost analytics / analytics-v2 / returns / commercial (admin) | varios | PREV staff / management / cost_control |
| Logistics, promotions, margin-rules (writes), gondolas | varios | PREV |
| Admin users | `/admin/users*` | PREV user_admin_* + ERP |
| Operaciones Quillotana (staff) | mayoría `/operaciones/*` | PREV; algunos dual staff/móvil |
| Auth | `/auth/me` | PREV (+ campos multiapp) |

## C. Excepciones documentadas (NO endurecer sin transición)

| Método | Ruta | Protección actual | Por qué EXC | Plan |
|---|---|---|---|---|
| GET/POST/PUT | `/orders`, `/orders/{id}`, `/orders/{id}/status` | **Ninguna JWT staff** | Pedidos catálogo/cliente público o semi-público legacy | Diseñar transición; no tocar en 1A.1 |
| POST | `/operaciones/heartbeat`, `/gps_track`, `/gps-track` | `X-Heartbeat-Key` **o** Bearer vía `verify_operaciones_mobile_auth` | App vendedores móvil | Mantener; no exigir ERP |
| POST | `/operaciones/visita_sync`, `/visita-sync` | Delegado a `app_distribuidora` (auth móvil legacy) | Cola móvil | Transición PWA aparte |
| GET | `/operaciones/telemetry/health` | Público | Healthcheck montaje | OK público |
| Health / readiness | rutas health de app | Público / deploy | Liveness | OK |

**Importante:** Si `OPERACIONES_HEARTBEAT_KEY` no está configurada, revisar `verify_operaciones_mobile_auth` en despliegue (puede quedar abierta). Eso es riesgo operativo previo, no introducido por 1A.1.

## D. Pendientes (siguiente endurecimiento, no bloquean 1A.1)

| Router | Ejemplos | Notas |
|---|---|---|
| `products.py`, `products_master.py`, `price_lists.py`, `offers.py`, `suppliers.py`, `summary.py`, `margins*`, `labels.py`, `catalog.py` | Lecturas/escrituras ERP sin Depends uniforme | Priorizar writes |
| `erp.py` (no sync) | Catálogo ERP residual | Auditar |
| `app_distribuidora.py` | API móvil visitas | Transición PWA |
| Office-level en dispatch/planificación | Recursos por oficina | Requiere join recurso→office + `assert_office` |

## E. Compatibilidad multiapp (C)

| Caso | Comportamiento |
|---|---|
| Tablas 064–066 **ausentes** | `tables_ready=False` → ERP legacy OK; PWA deny |
| Tablas presentes, **0 filas** user_apps | Legacy ERP OK (salvo `AUTH_REQUIRE_USER_APPS=1`) |
| Filas solo `pwa` | ERP **403** |
| Error BD al cargar alcance | `load_failed=True` → **503**, nunca allow |
| Flags active / token_version | Siguen **OFF** por defecto |

### Orden seguro despliegue

1. Desplegar backend con código 1A.1 (flags OFF). Pre-064: ERP intacto.
2. Aplicar SQL `064` → `065` → `066` (vacías). ERP legacy sigue.
3. Fase 1B: UI + backfill selectivo `user_apps(erp)` / perfiles.
4. Solo entonces considerar `AUTH_REQUIRE_USER_APPS` / `AUTH_CHECK_ACTIVE_ON_REQUEST` / `AUTH_ENFORCE_TOKEN_VERSION` en staging.

### Revocación JWT gradual

1. Mantener `AUTH_CHECK_ACTIVE_ON_REQUEST` y `AUTH_ENFORCE_TOKEN_VERSION` en OFF.
2. Activar primero en entorno de prueba con usuarios piloto.
3. Al desactivar usuario o rotar `token_version`, invalidación inmediata solo con flags ON.
4. No activar global sin inventario de sesiones ERP activas.

## F. Fase 1B — cambios exactos necesarios (sin implementar)

### Backend

| Superficie | Cambio |
|---|---|
| `GET /admin/users`, `GET /admin/users/{id}` | Incluir `apps[]`, `profiles[]`, `offices[{company_id,office_id}]` además de `companies` |
| `POST /admin/users` | Aceptar `apps`, `profiles`, `office_scopes`; default apps=`["erp"]` para trabajadores ERP; PWA-only sin `erp` |
| `PATCH /admin/users/{id}` | Upsert/delete en `user_apps`, `user_operational_profiles`, `user_offices`; validar office∈company |
| `GET /admin/users/meta/offices?company_id=` | Nuevo catálogo oficinas (`bsale.offices`) filtrado por empresas del actor |
| `GET /admin/users/meta/companies` | Ya existe; reutilizar |
| Servicio `user_admin` | Transacciones atómicas; no escribir apps si tablas ausentes → 503 claro |
| `/auth/me` | Ya expone multiapp (1A); UI usará flags |

### Frontend

| Archivo | Cambio |
|---|---|
| `usuarios-accesos/page.tsx` | Columnas Apps / Perfiles; pasar catálogo oficinas al sheet |
| `user-admin-sheet.tsx` | Checks ERP/PWA; multi-select oficinas filtradas por empresas marcadas; perfiles solo si PWA; create/update payloads |
| `lib/admin-users-api.ts` | Tipos + fetch meta offices |

Regla UX: trabajador puede ser ERP, PWA o ambos; OPERATIVO no recibe ERP automático al marcar solo PWA.
