# Panel de Diagnóstico ERP

## Objetivo

Herramienta **interna** para administración y desarrollo: ver de un vistazo el estado del backend, la conectividad a PostgreSQL, las últimas peticiones HTTP registradas en el proceso, logs recientes de aplicación (handlers en memoria) y errores agregados, **sin** sustituir a un stack profesional de observabilidad (Sentry, Grafana, Datadog, etc.).

## Ruta frontend

- **URL:** `/admin/diagnostico`
- **Título en UI:** Panel de Diagnóstico ERP
- Requiere sesión del dashboard (layout existente) y **rol de administración** en `localStorage` (`role`: `admin`, `superadmin`, `super_admin`, `administrator`). Si no aplica, se muestra un aviso y no se llama al API.

El enlace **Diagnóstico ERP** aparece en el sidebar bajo **Administración** solo para esos roles.

## Endpoints backend

Prefijo: **`/diagnostics`**

| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/diagnostics/health` | Estado del API, BD, uptime del proceso, versión, contadores del buffer, tiempo medio de respuesta (desde muestras en memoria). |
| GET | `/diagnostics/requests?limit=` | Últimas peticiones HTTP registradas por el middleware (query sanitizada, sin body). |
| GET | `/diagnostics/logs?limit=` | Logs recientes capturados por handler en memoria (`backend`, `uvicorn.error`). |
| GET | `/diagnostics/errors?limit=` | Errores recientes (HTTP 4xx/5xx en peticiones + niveles ERROR/CRITICAL en logs). |
| GET | `/diagnostics/endpoints` | `registered`: rutas declaradas en FastAPI; `observed`: agregación simple por método+path desde el buffer de peticiones. |

**Protección:** en todos los casos se exige cabecera `Authorization: Bearer <JWT staff>` emitido por `POST /login`, con rol de administración (misma lista que el frontend). Además:

- En **production** / **staging** el módulo solo responde si `ENABLE_DIAGNOSTICS=true` (y no está `DISABLE_DIAGNOSTICS=1`).
- En **development** (por defecto `ENVIRONMENT=development`) el diagnóstico suele estar habilitado sin variable extra.

Si el módulo está deshabilitado, las rutas responden **404** a propósito (no revelar superficie de ataque).

## Qué información muestra

### Health check

- `status`: `ok` o `degraded` (p. ej. BD caída).
- `backend`, `database`, `uptime`, `serverTime`, `version`, métricas derivadas del buffer.

### Requests

- Método, path (query con parámetros sensibles enmascarados), código HTTP, duración ms, email de usuario si el JWT es válido (sin mostrar el token), IP/origen/user-agent truncados y sanitizados, mensaje de error corto si la petición falló con excepción no capturada en el middleware.

### Logs

- Mensajes de loggers `backend` y `uvicorn.error` a nivel INFO+ (mensaje y excepción resumida sanitizada).

### Errores

- Unión controlada de fallos HTTP recientes y líneas de log ERROR/CRITICAL.

### Endpoints

- **Registrados:** introspección de `app.routes`.
- **Observados:** estadísticas desde el ring buffer (última llamada, media de ms, conteo de errores por ruta+método).

## Seguridad

### Qué datos no se muestran

- No se almacenan ni devuelven contraseñas, tokens completos, cabeceras `Authorization` ni cookies completas.
- No se registra el **body** de las peticiones (evita fugas de PII o credenciales en POST).

### Sanitización

- Query string: valores cuyas claves contienen subcadenas como `password`, `token`, `secret`, `apikey`, `refreshToken`, `clave`, `contraseña`, etc. se reemplazan por `[redacted]`.
- Texto libre (user-agent, errores): se trunca y se intenta redactar patrones tipo `Bearer …`.

### Cómo proteger la ruta

1. **Backend:** JWT staff + rol admin; en producción exigir `ENABLE_DIAGNOSTICS=true` explícito.
2. **Frontend:** comprobación de rol antes de consumir el API; el layout del dashboard ya exige login.
3. **Red:** restringir por VPN o IP allowlist frente al API en producción si el panel estuviera expuesto.

### Riesgos de dejarlo público

- Enumeración de rutas y patrones de uso.
- Fuga de emails de usuarios que llaman al API con JWT válido.
- Los buffers en memoria pueden contener fragmentos de URLs con datos de negocio (no secretos deliberados, pero sensibles según contexto).

## Variables de entorno

| Variable | Uso |
|----------|-----|
| `ENVIRONMENT` o `ENV` | `development` (por defecto en muchos setups) vs `production` / `staging` para exigir `ENABLE_DIAGNOSTICS`. |
| `ENABLE_DIAGNOSTICS` | `true` / `1` / `yes` para habilitar API y middleware en producción o staging. |
| `DISABLE_DIAGNOSTICS` | `true` / `1` / `yes` para desactivar todo el módulo (404). |
| `DIAGNOSTICS_MAX_LOGS` | Tamaño máximo aproximado del ring buffer (50–5000, default 500). |
| `APP_VERSION` | Cadena opcional devuelta en health (si no, se usa la versión de la app FastAPI / `1.0.0`). |
| `NEXT_PUBLIC_APP_VERSION` | (Frontend) versión mostrada en el panel para el bloque “frontend”. Opcional. |

Ejemplo en `.env.example` en la raíz del repositorio.

## Limitaciones actuales

- Los buffers son **en memoria**: se pierden al reiniciar el proceso del API.
- No hay persistencia en base de datos ni retención larga.
- No se ejecutan health checks a **servicios externos** (Bsale, ORS, etc.); eso queda como mejora futura documentada en la UI.
- El listado de rutas **registradas** puede ser largo y mezclar rutas técnicas (OpenAPI, etc.); las rutas `/docs`, `/openapi.json` y `/redoc` no se registran en el buffer de peticiones para reducir ruido.

## Próximas mejoras sugeridas

- Health checks opcionales a terceros con timeout corto y sin credenciales en la respuesta.
- Persistencia de eventos en PostgreSQL o cola.
- Integración con Sentry / OpenTelemetry.
- Métricas Prometheus + Grafana.
- Export CSV y alertas por ratio de 5xx.
- Auditoría por usuario admin y rotación de JWT con `JWT_SECRET` en variable de entorno (hoy el secreto del staff vive en código del módulo `auth`; **TODO** de seguridad global del proyecto).

## Cómo probar el panel

1. Levantar backend y frontend habituales.
2. Iniciar sesión en el ERP con un usuario staff cuyo `role` en BD sea `admin` (o equivalente listado arriba).
3. Abrir `/admin/diagnostico` y pulsar **Refrescar datos**.
4. En staging/producción: definir `ENABLE_DIAGNOSTICS=true` en el servicio del API y reiniciar.
