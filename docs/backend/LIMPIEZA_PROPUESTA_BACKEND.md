# Limpieza propuesta del backend ERP

## Importante

Este documento **no elimina ni modifica código**. Solo identifica candidatos de limpieza para **revisión manual** posterior.

---

## Limpieza segura

| Tipo | Archivo/Carpeta | Motivo | Evidencia | Riesgo | Verificación antes de eliminar |
|---|---|---|---|---|---|
| Documentación duplicada | Ninguno detectado como duplicado exacto | — | — | Bajo | — |
| Script con nombre erróneo | `backend/jobs/debug_full_bsalse_relationships.py` | Nombre con typo `bsalse` vs `bsale`; existe homónimo correcto | Comparar con `backend/jobs/debug_full_bsale_relationships.py` | Bajo | Confirmar cuál usan en runbooks/cron y unificar en un solo archivo |
| Token en código (no producción) | `ultimaoc.py` (raíz repo) | Token Bsale hardcodeado | `BSALE_TOKEN = "..."` en `ultimaoc.py` | **Medio** (exposición si se commitea) | Rotar token si alguna vez estuvo en remoto; mover a env |
| Token en código (no producción) | `pruebadatos.py` (raíz repo) | Token hardcodeado | `API_TOKEN = "..."` en `pruebadatos.py` | **Medio** | Igual que arriba |
| Secret JWT en código | `backend/routers/auth.py` | `SECRET = "quillotana_secret_key"` fijo | Línea ~22 en `auth.py` | **Alto** si se asume producción | Migrar a variable de entorno y rotar tokens emitidos |

---

## Limpieza con revisión manual

| Tipo | Archivo/Carpeta | Motivo | Evidencia | Riesgo | Verificación antes de eliminar |
|---|---|---|---|---|---|
| Scripts de sincronización fuera de `backend/jobs` | Varios `sync_*.py`, `docs_prueba_bsale.py`, `ultimaoc.py`, etc. en raíz | Duplican patrón de jobs/ops; confusión sobre “canónico” | Archivos en raíz del repo vs `backend/jobs/` | Medio | Buscar referencias en Docker/Coolify/README internos; decidir carpeta única (`backend/jobs` o `scripts/`) |
| Doble capa ERP bajo `/erp` | `backend/routers/erp.py` + `backend/routers/distribuidora_sync.py` | Ambos usan prefijo `/erp` (montaje en `backend/main.py`) | `backend/main.py` incluye ambos routers | Medio | Documentar qué panel consume cada ruta; evitar borrar hasta mapear frontend/proxy |
| ORM ausente / SQL disperso | `backend/repositories/distribuidora/*.py`, routers con SQL inline | Mantenimiento difícil; no hay capa ORM única | `grep` muestra SQL en routers y repos | Medio | No borrar; eventual refactor solo con tests de contrato |
| Utilidad que falla al importar sin env | `backend/utils/config.py` | `raise Exception` si falta `ORS_API_KEY` | `backend/utils/config.py` | Medio | Auditar quién importa `ors_client`/`config` en cold start |
| Tabla NocoDB hardcodeada | `backend/routers/dashboard.py`, `margin_problems.py`, `margin_export.py` | `TABLE_ANALYTICS = "m777i9qvqgbvpuk"` | Constantes en esos archivos | Medio | Confirmar ID de tabla en NocoDB y externalizar a env si aplica |
| Variable sensible (nombre) | `backend/database.py` | Token NocoDB vía `NocoDB_token` | `NOCODB_TOKEN = os.getenv("NocoDB_token")` | Bajo nombre, **medio** operativo | Estandarizar nombre env (`NOCODB_TOKEN`) sin romper deploy actual |
| Dependencia no referenciada en código | `requirements.txt` → `python-jose[cryptography]` | No hay `import jose` / `from jose` en `backend/` (búsqueda estática) | `grep` en `backend/` | Bajo | Confirmar con `pip show` / análisis de dependencias antes de quitar |

---

## No tocar todavía

| Tipo | Archivo/Carpeta | Motivo | Riesgo |
|---|---|---|---|
| Jobs de sync Bsale / Distribuidora | `backend/jobs/sync_bsale_distribuidora.py`, `backend/services/distribuidora/sync_service.py`, etc. | Producción depende de sync incremental/resync | **Alto** |
| Middleware CORS | `backend/cors_middleware.py`, `backend/main.py` | Afecta todos los clientes web | **Alto** |
| Rutas Distribuidora masivas | `backend/routers/distribuidora.py` | Rutero, mapas, ORS, rutas manuales | **Alto** |
| Auth login | `backend/routers/auth.py` | Autenticación staff + login cliente | **Alto** |
| SQL de esquema | `backend/sql/**/*.sql` | Definición de tablas/vistas usadas en prod | **Alto** |
