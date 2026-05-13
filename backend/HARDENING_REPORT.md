# FASE 5.1 — Higiene y seguridad base (HARDENING)

**Alcance:** reducir riesgo operacional **sin** mover carpetas, refactor masivo, cambios en frontend ni migración de jobs a Coolify.

**Referencias:** `SCRIPTS_REGISTRY.md`, `SCRIPTS_CONSOLIDATION_PLAN.md`. **Estructura (FASE 6):** [`STRUCTURE_MIGRATION_REPORT.md`](./STRUCTURE_MIGRATION_REPORT.md).

---

## 1. Riesgos corregidos (esta fase)

| Riesgo | Acción |
|--------|--------|
| Credenciales PostgreSQL en claro en `backend/test_db.py` | **Archivo eliminado.** Si el repo estuvo en remoto con ese contenido, **rotar** contraseña/host afectados. |
| Token Bsale literal en varios scripts Python | Sustituido por `BSALE_TOKEN` / `BSALE_TOKEN_SPA` + `.env` (vía helper central). |
| Scripts raíz de prueba con token fijo | `docs_prueba_bsale.py`, `ultimaoc.py`, `pruebadatos.py` leen token solo desde entorno. |
| JWT sin posibilidad de override por env | `backend/routers/auth.py`: lectura de `JWT_SECRET_KEY` o `SECRET_KEY`; si faltan, fallback de desarrollo + `logger.warning`. |

### Archivos tocados (código)

- **Nuevo:** `backend/utils/bsale_token_env.py` — `require_bsale_token()`, `read_bsale_token_from_env()`, carga opcional de `dotenv`.
- **Scripts:** `analyze_purchase_orders_relationships.py`, `debug_document_types.py`, `export_bsale_documents_test.py`, `debug_single_document.py`, `test_bsale_documents_office_1.py` (importan helper + `sys.path` al repo cuando hace falta).
- **Raíz repo:** `docs_prueba_bsale.py`, `ultimaoc.py`, `pruebadatos.py`.
- **Eliminado:** `backend/test_db.py`.
- **Auth:** `backend/routers/auth.py` — variable `SECRET` desde env con advertencia si se usa fallback.

---

## 2. Riesgos pendientes (prioridad sugerida)

| Prioridad | Riesgo | Detalle | Impacto |
|-----------|--------|---------|---------|
| **P1** | JWT fallback en código | Sigue existiendo cadena por defecto si no hay env; aceptable en dev, **no** en prod sin `JWT_SECRET_KEY`. | Compromiso de sesiones si alguien asume defaults en prod. |
| **P1** | Rotación de secretos expuestos | Cualquier token que haya estado en historial Git debe considerarse filtrado. | Acceso API Bsale / DB si no se rota. |
| **P2** | `frontend/lib/api.ts` — `DEMO_PASSWORD` | Fuera del alcance FASE 5.1 (no frontend); no modificado. | Solo entornos demo. |
| **P2** | Helpers HTTP Bsale duplicados | `_get`, throttles y límites repetidos en `jobs/` (`export_oc_analysis_to_excel`, `analyze_related_patterns`, `debug_full_bsale_relationships`). | Deriva / bugs al cambiar API; más superficie de mantenimiento. |
| **P2** | `get_connection()` sin context manager en muchos scripts | Conexión manual; riesgo de fugas si se amplía código. | Fugas de conexión bajo errores raros. |
| **P3** | Jobs de sync sin DRY_RUN global | Comportamiento esperado para sync; riesgo es **operador** (PG/URL equivocados). | Datos incorrectos o borrado lógico vía upsert. |
| **P3** | Scripts sin transacción explícita | Solo `cleanup_document_related_invalid_types.py` documenta BEGIN/backup/DELETE; el resto depende del servicio. | Inconsistencias parciales si se añaden scripts de escritura ad hoc. |
| **P3** | Scripts sin backup previo | Solo el cleanup tiene tabla `*_cleanup_backup`. | Dificulta rollback de otros mantenimientos. |

---

## 3. Propuestas técnicas (sin implementar en esta fase)

| Componente | Propuesta |
|--------------|-----------|
| **Helper PG único** | Exponer `get_connection()` + `contextmanager` opcional `with_connection()` en `backend/db.py` para scripts nuevos. |
| **Helper HTTP Bsale único** | Extender `BsaleClient` o módulo `bsale_http_debug.py` con GET paginado y retry; jobs debug dejan de copiar `_get`. |
| **Helper retry único** | Centralizar reintentos (429/5xx) al lado de `BsaleClient` o `requests.Session` con política única. |
| **Helper logging único** | `logging.basicConfig` + convención stdout/stderr documentada en `SCRIPTS_REGISTRY` / runbook Coolify. |

---

## 4. Inventario: DRY_RUN / backup / transacción (scripts relevantes)

| Script / job | DRY_RUN por defecto | Backup antes de mutar | Transacción explícita |
|--------------|---------------------|-------------------------|------------------------|
| `cleanup_document_related_invalid_types.py` | sí (`--execute` para escribir) | sí (`document_related_cleanup_backup`) | sí (una unidad) |
| `audit_document_related.py` | N/A (solo lectura) | N/A | N/A |
| `sync_bsale_distribuidora` / servicios | no | depende de sync (no TRUNCATE masivo en script suelto) | commits por diseño del servicio |
| `debug_*`, `export_*` masivos | no (algunos solo lectura API) | no | no |
| Jobs `export_oc_*` | no | no | no |

---

## 5. Variables de entorno documentadas (`.env.example`)

Se añadieron comentarios sugeridos para:

- `BSALE_TOKEN` / `BSALE_TOKEN_SPA`
- `JWT_SECRET_KEY` (o `SECRET_KEY`)

*(Ver diff en `.env.example`.)*

---

## 6. Impacto operativo

- **Desarrolladores:** deben tener `.env` con `BSALE_TOKEN` para ejecutar scripts Bsale que antes “funcionaban” con token en archivo.
- **CI:** si algún job ejecutaba esos scripts sin env, fallará de forma explícita (mejor que filtrar secretos).
- **Producción FastAPI:** definir `JWT_SECRET_KEY` para no depender del fallback ni del warning en logs.

---

## 7. Checklist post-despliegue

- [ ] Rotar credenciales que estuvieran en `test_db.py` si hubo exposición.
- [ ] Rotar token Bsale que haya estado en el historial del repo.
- [ ] Configurar `JWT_SECRET_KEY` en entornos no locales.
- [ ] Revisar pipelines que ejecuten scripts Bsale y añadan secretos desde Coolify / GitHub Actions.

---

*FASE 5.1 — generado como parte del endurecimiento mínimo acordado.*
