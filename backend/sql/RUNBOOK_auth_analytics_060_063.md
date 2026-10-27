# RUNBOOK — Migraciones auth 060–063 sobre BD única `analytics`

**Decisión gerencia:** no hay PostgreSQL staging separado. Las migraciones se
aplican sobre la misma base `analytics` que usa la ERP.

**NO ejecutar automáticamente.** Requiere autorización explícita + respaldo
verificado.

## 0. Precondiciones

1. Código Etapas 1–2.1 desplegado (Fase A) con flags disruptivos **OFF**.
2. `AUTH_USER_ADMIN_EMAILS` / `AUTH_ASSIGN_ADM_EMAILS` **aún vacías o sin
   publicar el panel** hasta Fase E (mutaciones denegadas en production).
3. Respaldo recuperable confirmado (sección 1).
4. Ventana de mantenimiento acordada (ALTER ADD COLUMN es breve; CREATE TABLE
   casi sin bloqueo).

## 1. Respaldo obligatorio

### 1.1 Dump recuperable (ejemplo Coolify / servidor)

```bash
# Usar credenciales del servicio PostgreSQL de analytics (no pegar secretos)
pg_dump --format=custom --file=analytics_pre_auth_$(date -u +%Y%m%dT%H%M%SZ).dump \
  --dbname="$DATABASE_URL"
```

Verificar que el dump **incluye** al menos:

- schema `bsale` (tabla `users`, `companies`)
- schema `app` (si ya existe)

```bash
pg_restore --list analytics_pre_auth_*.dump | grep -E 'bsale\.(users|companies)|SCHEMA - app'
```

### 1.2 Inventario estructural previo (solo lectura)

```sql
SELECT current_database();  -- debe ser analytics
SELECT COUNT(*) FROM bsale.users;  -- esperado: 13
SELECT EXISTS (
  SELECT 1 FROM information_schema.columns
  WHERE table_schema='bsale' AND table_name='users' AND column_name='token_version'
) AS has_token_version;
SELECT to_regclass('app.user_companies'),
       to_regclass('app.auth_events'),
       to_regclass('app.auth_password_tokens');
```

Registrar resultados en el ticket de cambio (conteos, no correos).

## 2. Orden de aplicación (aditivo, idempotente)

| Orden | Archivo | Efecto | Bloqueo esperado |
|------:|---------|--------|------------------|
| 1 | `060_auth_token_version.sql` | `ADD COLUMN token_version … DEFAULT 0` | Breve ACCESS EXCLUSIVE en `bsale.users` (tabla pequeña, 13 filas) |
| 2 | `061_auth_user_companies.sql` | schema `app` + tabla vacía | Mínimo |
| 3 | `062_auth_audit_events.sql` | tabla vacía | Mínimo |
| 4 | `063_auth_password_tokens.sql` | tabla vacía | Mínimo |

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f backend/sql/060_auth_token_version.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f backend/sql/061_auth_user_companies.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f backend/sql/062_auth_audit_events.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f backend/sql/063_auth_password_tokens.sql
```

Reintento: los scripts usan `IF NOT EXISTS` / `ADD COLUMN IF NOT EXISTS`.

## 3. Validación post-migración (obligatoria)

```sql
-- Estructura
SELECT column_name, data_type, column_default
FROM information_schema.columns
WHERE table_schema='bsale' AND table_name='users' AND column_name='token_version';

SELECT to_regclass('app.user_companies') IS NOT NULL AS ok_uc,
       to_regclass('app.auth_events') IS NOT NULL AS ok_ae,
       to_regclass('app.auth_password_tokens') IS NOT NULL AS ok_apt;

-- Datos staff intactos (conteos)
SELECT role, active, COUNT(*) FROM bsale.users GROUP BY 1, 2 ORDER BY 1, 2;

-- Tablas nuevas vacías
SELECT
  (SELECT COUNT(*) FROM app.user_companies) AS uc,
  (SELECT COUNT(*) FROM app.auth_events) AS ae,
  (SELECT COUNT(*) FROM app.auth_password_tokens) AS apt;
```

**Criterio de éxito:** mismos conteos de roles/activos que el inventario previo;
`token_version` presente; tablas `app.*` existen y vacías; login ERP sigue
funcionando (Fase C).

## 4. Recuperación

| Escenario | Acción |
|-----------|--------|
| Fallo a mitad | Reaplicar el archivo fallido (`IF NOT EXISTS`) |
| Rollback lógico | Apagar flags; `DROP TABLE` de `app.auth_*` / `user_companies` si vacío; `DROP COLUMN token_version` solo tras confirmar que no hay enforce |
| Catástrofe | `pg_restore` del dump del paso 1 sobre instancia recuperada |

Un rollback SQL **no** invalida JWT ya emitidos ni deshace acciones humanas.

## 5. Lo que estas migraciones NO hacen

- No modifican emails, roles, passwords ni `active` de los 13 usuarios.
- No poblan `user_companies`.
- No activan flags de aplicación.
- No tocan tablas Bsale RAW / réplica.
