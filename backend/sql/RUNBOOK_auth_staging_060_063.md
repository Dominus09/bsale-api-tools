# RUNBOOK — Migraciones auth 060–063 (solo staging aislado)

**NO ejecutar en producción ni en una BD compartida con producción.**

## 0. Condición de corte (obligatoria)

Antes de cualquier `psql`:

1. Confirmar que el host/DB de staging **no** es la misma instancia ni el mismo `database` que producción.
2. Confirmar credenciales distintas y sin réplica de escritura productiva.
3. Si `test.quillotana.cl` apunta a la misma BD `analytics` (o equivalente productivo): **DETENER** y crear PostgreSQL aislado + dump sanitizado.

Inventario MCP actual (solo lectura): base `analytics`, rol `ai_reader` — **tratar como datos reales hasta demostrar lo contrario**.

## 1. Respaldo

```bash
# En el servidor staging, con variables de staging (no pegar secretos en tickets)
pg_dump --format=custom --file=auth_pre_060_063_$(date +%Y%m%d).dump "$STAGING_DATABASE_URL"
```

## 2. Orden (dependencias)

| # | Archivo | Depende de |
|---|---------|------------|
| 1 | `060_auth_token_version.sql` | `bsale.users` |
| 2 | `061_auth_user_companies.sql` | `bsale.users`, `bsale.companies`, schema `app` |
| 3 | `062_auth_audit_events.sql` | `bsale.users`, schema `app` |
| 4 | `063_auth_password_tokens.sql` | `bsale.users`, schema `app` |

Idempotencia: usan `IF NOT EXISTS` / `ADD COLUMN IF NOT EXISTS`. Reintento seguro si falla a mitad (reaplicar el mismo archivo).

## 3. Aplicación (una transacción lógica por archivo)

```bash
psql "$STAGING_DATABASE_URL" -v ON_ERROR_STOP=1 -f backend/sql/060_auth_token_version.sql
psql "$STAGING_DATABASE_URL" -v ON_ERROR_STOP=1 -f backend/sql/061_auth_user_companies.sql
psql "$STAGING_DATABASE_URL" -v ON_ERROR_STOP=1 -f backend/sql/062_auth_audit_events.sql
psql "$STAGING_DATABASE_URL" -v ON_ERROR_STOP=1 -f backend/sql/063_auth_password_tokens.sql
```

## 4. Verificación post-migración

```sql
SELECT column_name FROM information_schema.columns
 WHERE table_schema='bsale' AND table_name='users' AND column_name='token_version';

SELECT to_regclass('app.user_companies'),
       to_regclass('app.auth_events'),
       to_regclass('app.auth_password_tokens');

-- Compatibilidad: usuarios existentes intactos (solo conteos)
SELECT role, active, COUNT(*) FROM bsale.users GROUP BY 1, 2;
```

## 5. Datos de prueba (ficticios)

- Crear usuarios `staging-*@example.invalid` vía API admin.
- No reutilizar contraseñas ni sesiones de producción.
- Deshabilitar jobs Bsale / correos externos en el stack de staging.

## 6. Reversión

| Objeto | Rollback |
|--------|----------|
| `token_version` | `ALTER TABLE bsale.users DROP COLUMN IF EXISTS token_version;` (tras apagar `AUTH_ENFORCE_TOKEN_VERSION`) |
| `app.user_companies` | `DROP TABLE IF EXISTS app.user_companies;` |
| `app.auth_events` | `DROP TABLE IF EXISTS app.auth_events;` |
| `app.auth_password_tokens` | `DROP TABLE IF EXISTS app.auth_password_tokens;` |
| Total | restaurar `pg_dump` del paso 1 |

## 7. Después de migrar (aún sin prod)

Seguir secuencia de flags del informe Etapa 2.1. No activar `AUTH_REQUIRE_USER_COMPANIES` hasta asignar empresas a **todas** las cuentas de staging.
