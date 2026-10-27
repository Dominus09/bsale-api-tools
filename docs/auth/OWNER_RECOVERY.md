# Recuperación de cuenta OWNER

Las cuentas identificadas por `AUTH_OWNER_USER_IDS` **no** se desactivan, no cambian de rol, no modifican empresas ni restablecen contraseña desde **Administración → Usuarios y accesos**.

## Procedimiento seguro (fuera del panel)

1. Acceder al contenedor / host con credenciales de operaciones (Coolify), no desde la UI ERP.
2. Confirmar el `id` del propietario en `bsale.users` (consulta de solo lectura).
3. Para restablecer contraseña del OWNER:
   - Generar un hash bcrypt fuera de banda, o
   - Insertar un token de un solo uso en `app.auth_password_tokens` con propósito `reset` y entregarlo por canal seguro al titular, o
   - Usar un script de mantenimiento interno revisado (no expuesto al panel).
4. No eliminar ni vaciar `AUTH_OWNER_USER_IDS` salvo emergencia controlada (quedaría sin propietario en el panel).
5. Tras recuperar acceso, el OWNER inicia sesión normalmente; el rol en PostgreSQL puede seguir siendo `gerencia`.

## Emergencia: revocar OWNER en la aplicación

- Quitar o cambiar `AUTH_OWNER_USER_IDS` en Coolify y reiniciar el API.
- Sin ese ID, nadie obtiene `is_owner` ni privilegios OWNER.
- Las allowlists `AUTH_USER_ADMIN_EMAILS` / `AUTH_ASSIGN_ADM_EMAILS` siguen aplicando a gerencia/adm autorizados.

## No hacer

- No asignar “owner” como rol en `bsale.users`.
- No confiar en flags del frontend.
- No compartir tokens de reset por tickets públicos.
