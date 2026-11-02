# RUNBOOK — Migraciones auth multiapp 064–066 (`analytics`)

**NO ejecutar automáticamente.** Requiere autorización explícita + respaldo.

## Hallazgo de inspección (2026-11)

| Objeto | Estado en BD accesible |
|---|---|
| `bsale.users.token_version` (060) | **Aplicado** |
| `app.user_companies` (061) | **Aplicado** |
| `app.auth_events` (062) | **Aplicado** |
| `app.auth_password_tokens` (063) | **Aplicado** |
| `app.user_apps` (064) | Pendiente |
| `app.user_operational_profiles` (065) | Pendiente |
| `app.user_offices` (066) | Pendiente |

`test.quillotana.cl` / `api.quillotana.cl` usan la **misma** BD `analytics` (sin staging aislado).

## Orden exacto

Si 061–063 ya existen (como ahora), aplicar solo:

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f backend/sql/064_auth_user_apps.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f backend/sql/065_auth_user_operational_profiles.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f backend/sql/066_auth_user_offices.sql
```

Si en otro entorno faltaran 061–063, el orden completo es:

`060` → `061` → `062` → `063` → `064` → `065` → `066`

Todos son `IF NOT EXISTS` / aditivos (seguros de re-ejecutar).

## Verificación previa (READ-ONLY)

```sql
SELECT current_database();
SELECT to_regclass('app.user_companies'), to_regclass('app.user_apps'),
       to_regclass('app.user_operational_profiles'), to_regclass('app.user_offices');
SELECT count(*) FROM bsale.users;
```

## Verificación posterior

```sql
SELECT to_regclass('app.user_apps'), to_regclass('app.user_operational_profiles'),
       to_regclass('app.user_offices');
SELECT count(*) FROM app.user_apps;  -- debe ser 0 hasta backfill 1B
```

## Backfill (NO en Fase 1A)

No poblar `user_apps(erp)` hasta Fase 1B + inventario. Mientras no haya filas,
el código trata cuentas legacy como ERP (compatibilidad).

## Flags (permanecen OFF)

- `AUTH_REQUIRE_USER_APPS` — cutover apps
- `AUTH_REQUIRE_USER_COMPANIES` — cutover empresas
- `AUTH_ENFORCE_TOKEN_VERSION` / `AUTH_CHECK_ACTIVE_ON_REQUEST` — no activar globalmente en esta fase

## Orden seguro con hardening 1A.1 + admin 1B

`test.quillotana.cl` y `api.quillotana.cl` comparten **la misma** BD `analytics`.

1. **Desplegar backend** 1A.1 + 1B (flags OFF).
   - Sin tablas 064–066: ERP legacy OK; PWA deny; escritura multiapp → **503** claro (UI muestra aviso).
   - Admin CRUD sin campos `apps`/`profiles`/`office_scopes` sigue compatible.
2. **Aplicar SQL** `064` → `065` → `066` (vacías) en ventana controlada.
3. Verificar `GET /admin/users/meta/apps` → `ready: true`.
4. Probar en UI Usuarios y Accesos: crear ERP, PWA-only, ambas; editar oficinas.
5. **No** backfill masivo de `user_apps(erp)` ni activar `AUTH_REQUIRE_USER_APPS` en esta fase.
6. Revocación JWT (`active` / `token_version`): flags solo en piloto.

Matriz: `docs/auth/PHASE_1A1_SECURITY_MATRIX.md`.
