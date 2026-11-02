# Runbook de publicación — Usuarios Multiapp (Fase 1C)

**Rama de integración:** `auth-multiapp-publish`  
**Commit:** `adff44d58cb04070adfc7f82f2834b64ffa64e3c`  
**Base:** `origin/main` @ `70ee381` (+ 3 commits: 1A, 1A.1, 1B)  
**Hosts:** FE `test.quillotana.cl` · API `api.quillotana.cl` · BD compartida `analytics`  
**NO incluye:** app.quillotana.cl, backfill masivo, cutover flags.

---

## Orden recomendado (preferente)

**Migraciones SQL 064–066 ANTES del redeploy de backend/frontend.**

### Justificación

- Scripts aditivos (`IF NOT EXISTS`), tablas vacías, sin INSERT de permisos.
- El backend **actualmente desplegado** no consulta `user_apps` / perfiles / oficinas → crearlas no cambia auth ERP.
- Al aterrizar el código nuevo, `GET /admin/users/meta/apps` puede responder `ready: true` de inmediato.
- Alternativa (SQL después del deploy) también es segura: código nuevo tolerante a tablas ausentes (ERP legacy; escrituras multiapp → 503). Preferimos SQL primero para no dejar la UI en “migraciones pendientes”.

### No preferible / evitar

- Activar `AUTH_REQUIRE_USER_APPS` / `AUTH_CHECK_ACTIVE_ON_REQUEST` / `AUTH_ENFORCE_TOKEN_VERSION` en este go-live.
- Asumir que un rollback de imagen Coolify revierte SQL (no lo hace).

---

## 1. Respaldo previo

1. Snapshot o dump lógico de `analytics` (mínimo esquemas `app` + `bsale.users`).
2. Anotar SHA desplegado actual de API y FE en Coolify.
3. Confirmar que hay al menos un administrador con acceso al panel Usuarios.

```sql
-- READ-ONLY
SELECT current_database(), now();
SELECT count(*) FROM bsale.users WHERE active IS TRUE;
SELECT to_regclass('app.user_companies'),
       to_regclass('app.user_apps'),
       to_regclass('app.user_operational_profiles'),
       to_regclass('app.user_offices');
```

Esperado hoy: `user_companies` presente; `user_apps` / profiles / offices **NULL**.

---

## 2. Verificaciones SQL previas

```sql
SELECT to_regclass('bsale.users'), to_regclass('bsale.companies'), to_regclass('bsale.offices');
-- PK offices debe permitir FK (company_id, bsale_id):
SELECT conname, pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = 'bsale.offices'::regclass AND contype IN ('p','u');
```

Confirmado en inspección: `offices_pk PRIMARY KEY (company_id, bsale_id)`.

---

## 3. Migraciones (ventana corta, misma BD que TEST/PROD)

Ejecutar **en este orden**, con `ON_ERROR_STOP`:

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f backend/sql/064_auth_user_apps.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f backend/sql/065_auth_user_operational_profiles.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f backend/sql/066_auth_user_offices.sql
```

Post-check:

```sql
SELECT to_regclass('app.user_apps'),
       to_regclass('app.user_operational_profiles'),
       to_regclass('app.user_offices');
SELECT count(*) FROM app.user_apps;                 -- 0
SELECT count(*) FROM app.user_operational_profiles; -- 0
SELECT count(*) FROM app.user_offices;              -- 0
```

Si falla a mitad: re-ejecutar es seguro (`IF NOT EXISTS`). Completar las pendientes antes del deploy FE que use multiapp.

---

## 4. Integración en main (después de revisión)

1. Push de `auth-multiapp-publish` (o PR → merge a `main`). **No forzar** sobre trabajos locales sucios del árbol original (OC/costos).
2. Coolify debe desplegar el SHA de integración (`adff44d…` o el merge commit resultante).

---

## 5. Redeploy Coolify (manual)

1. **Backend** `api.quillotana.cl` → deploy del commit de integración.
2. Esperar healthy / logs sin traceback de import.
3. **Frontend** `test.quillotana.cl` → deploy del mismo commit.
4. No tocar variables de cutover (`AUTH_REQUIRE_USER_APPS`, etc.).

---

## 6. Salud API

- Health/readiness habitual del servicio.
- Login ERP con usuario existente.
- `GET /auth/me` → `has_erp_access: true` para cuentas legacy sin filas `user_apps`.

---

## 7. Meta multiapp

Con sesión admin:

```http
GET /admin/users/meta/apps
```

Esperado: `"ready": true` y items `erp` / `pwa`.

También: `GET /admin/users/meta/profiles`, `GET /admin/users/meta/offices?company_id=<id>`.

---

## 8. Pruebas ERP post-deploy

- Abrir Distribuidora (predespacho, planificación, mapa, rutero).
- Cargas / sync live-now desde UI (Bearer).
- Selector de empresas (`GET /companies`).
- Confirmar **sin 401** con sesión válida.

---

## 9. Usuarios controlados (solo piloto, no masivo)

En `/admin/usuarios-accesos` (cuenta con `user_admin`):

1. Editar usuario ERP existente → guardar sin perder empresas.
2. Crear usuario **ERP-only** de prueba (invitar).
3. Crear usuario **PWA-only** de prueba → verificar que **no** abre módulos ERP (403 en `/companies` si intenta).
4. Usuario ambas apps + 2 perfiles + 2 oficinas de la misma empresa.
5. Rechazo esperado: oficina de otra empresa.
6. Desactivar usuario de prueba → cuenta inactiva en listado.

**No** crear choferes/bodega productivos hasta cerrar revisión de endpoints aún abiertos (catálogo, orders públicos, etc.).

---

## 10. Recuperación

| Fallo | Acción |
|---|---|
| Deploy FE/API malo | Redeploy SHA anterior en Coolify |
| SQL aplicada, código viejo | **OK**: tablas vacías no afectan backend viejo |
| Código nuevo, SQL no aplicada | UI muestra aviso; CRUD ERP básico OK; multiapp writes → 503 |
| Cuenta admin bloqueada | OWNER / allowlist / `OWNER_RECOVERY.md` — no rollback SQL |
| Querer “deshacer” tablas | **No** DROP en caliente; planificar migración inversa aparte |

Rollback de imagen **≠** revertir 064–066.

---

## Clientes que pueden ver 401/403 tras 1A.1

| Cliente | Riesgo | Mitigación |
|---|---|---|
| Frontend ERP (`lib/api.ts`, cargas, delivery-map) | Bajo | Ya envía `getAuthHeaders()` |
| Crons / jobs Python | Ninguno HTTP | Llaman servicios, no estos endpoints |
| Scripts curl / Postman sin Bearer a `/distribuidora/*`, `/companies`, `/upload/*`, `/erp/sync*` | Alto | Añadir JWT staff o dejar de usar HTTP abierto |
| App móvil telemetría / visitas | Fuera de alcance | No endurecido en 1A.1 |
| `/orders` catálogo | Sin cambio | Sigue excepción documentada |

---

## Checklist manual UI (post Coolify)

- [ ] Listado muestra Apps / Perfiles / Oficinas
- [ ] Filtros app / perfil / estado
- [ ] Crear ERP-only
- [ ] Crear PWA-only
- [ ] Crear ambas + perfiles
- [ ] Empresas + oficinas filtradas
- [ ] Confirmación antes de guardar
- [ ] Editar sin borrar relaciones no tocadas
- [ ] Desactivar
- [ ] Error 403 sin permiso admin
- [ ] Usuarios legacy visibles como ERP (legacy) hasta backfill
