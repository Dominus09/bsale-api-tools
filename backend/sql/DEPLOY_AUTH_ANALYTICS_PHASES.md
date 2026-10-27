# Despliegue controlado — Usuarios y accesos sobre `analytics`

No ejecutar automáticamente. Autorización explícita por fase.

## Fase A — Código compatible (flags OFF)

**Acción Coolify:** desplegar backend + frontend con variables:

- `ENVIRONMENT=production`
- `AUTH_USER_ADMIN_EMAILS=` (vacío)
- `AUTH_ASSIGN_ADM_EMAILS=` (vacío)
- Todos los flags JWT/audit/company **sin definir** o en 0

**Efecto:** login actual intacto; panel visible en lectura para gerencia/adm;
mutaciones **denegadas** (allowlist vacía en production).

**Verificar:** login de una cuenta existente; GET `/admin/users` 200 para gerencia;
POST `/admin/users` 403.

**Revertir:** redeploy del release anterior.

## Fase B — Migraciones aditivas

Seguir `RUNBOOK_auth_analytics_060_063.md` (respaldo → 060→063 → validar conteos).

**Verificar:** `token_version` existe; tablas `app.*` vacías; 13 usuarios sin cambio
de roles/activos.

**Revertir:** restore dump / DROP tablas vacías / DROP COLUMN tras apagar enforce.

## Fase C — Login de cuentas existentes

Sin flags nuevos. Probar login bcrypt de roles adm / gerencia / operativo (cuentas
reales, **sin** reset ni desactivar).

**Revertir:** no aplica si B no tocó datos de usuarios.

## Fase D — Seguridad gradual

Orden sugerido (uno a uno, con smoke login entre cada uno):

1. `AUTH_AUDIT_EVENTS=1`
2. `JWT_EXPIRE_MINUTES=480` (ejemplo)
3. Convivencia → `JWT_REQUIRE_EXP=1`
4. `AUTH_CHECK_ACTIVE_ON_REQUEST=1`
5. `AUTH_ENFORCE_TOKEN_VERSION=1`

**No** activar aún `AUTH_EMIT_COMPANY_IDS` / `AUTH_REQUIRE_USER_COMPANIES`.

**Revertir:** quitar variable y reiniciar servicio API.

## Fase E — Panel administrativo

1. Fijar `AUTH_USER_ADMIN_EMAILS` = correo gerencia autorizada.
2. Opcional: `AUTH_ASSIGN_ADM_EMAILS` = mismo correo si debe crear `adm`.
3. Reiniciar API.
4. Verificar mutaciones solo para esa identidad.
5. Asignar empresas en UI; revisar matriz; **después** considerar emit/require.

**Recuperación si se pierde acceso admin:** Coolify → set allowlist → restart.
No requiere SQL sobre `bsale.users`.

## Protección inicial de las 13 cuentas

- Mutaciones off hasta Fase E.
- Sin poblar `user_companies` automáticamente.
- Sin `AUTH_REQUIRE_USER_COMPANIES`.
- Sin resets/desactivaciones sobre cuentas reales en pruebas.
