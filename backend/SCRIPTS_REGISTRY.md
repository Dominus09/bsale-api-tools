# Registro y gobernanza de scripts / jobs ERP (FASE 4)

**Estado:** inventario vivo. **FASE 6:** estructura física aplicada; ver [`STRUCTURE_MIGRATION_REPORT.md`](./STRUCTURE_MIGRATION_REPORT.md). Shims en `backend/scripts/` y en `backend/jobs/` (solo debug/export) mantienen compatibilidad.

**Plan de consolidación (FASE 5):** ver [`SCRIPTS_CONSOLIDATION_PLAN.md`](./SCRIPTS_CONSOLIDATION_PLAN.md) (clasificación A–E, fusiones, estructura final, naming).

**Endurecimiento (FASE 5.1):** [`HARDENING_REPORT.md`](./HARDENING_REPORT.md) · **Estructura (FASE 6):** [`STRUCTURE_MIGRATION_REPORT.md`](./STRUCTURE_MIGRATION_REPORT.md).

**Alcance auditado:** `backend/audits/`, `backend/maintenance/`, `backend/debug/`, shims en `backend/scripts/`, jobs core y shims en `backend/jobs/`, cadena SQL Distribuidora vía `sync_repo`, `backend/sql/`.

---

## 1. Estructura de carpetas propuesta (objetivo)

Separar **entrada operativa** (cron / Coolify) de **herramientas humanas** (debug, auditoría, utilidades) y de **DDL versionado**.

```
backend/
  jobs/                 # Entradas programables: sync, tareas recurrentes (invocables por -m o Coolify)
  audits/               # Solo lectura + informes (stdout/JSON/Excel opcional)
  maintenance/          # Cambios acotados en BD (cleanup, backfill puntual) con dry-run / backup
  rebuilds/             # Repoblados masivos o reconstrucción de tablas (misma disciplina que jobs pero mayor riesgo)
  migrations/           # Opcional: scripts Python que orquestan DDL; hoy DDL está en backend/sql/
  debug/                # Diagnóstico puntual, APIs, dumps (no producción)
  scripts/              # Residuo controlado: utilidades puntuales (hashes, one-off) o hasta deprecación
  sql/                  # Sin cambiar: DDL por dominio (distribuidora/, rutero, etc.)
```

**Nota (FASE 6):** la estructura de carpetas de este diagrama está **implementada**. `scripts/` contiene solo shims; ver `backend/scripts/README.md`.

**Nota:** `sql/` se mantiene como fuente de verdad DDL; `migrations/` podría alojar solo *runners* si en el futuro se separan de `sync_repo.ensure_distribuidora_schema`.

---

## 2. Clasificación usada en el registro

| Tipo | Significado |
|------|-------------|
| **job** | Proceso recurrente o invocable en producción (sync, rutero). |
| **audit** | Inspección de BD/salud; por defecto sin escritura. |
| **maintenance** | Cambio controlado en datos o esquema puntual (cleanup, backup previo). |
| **rebuild** | Reconstrucción masiva (p. ej. `document_related` completo); no hay script dedicado aún fuera de servicios. |
| **migration** | DDL / orden de aplicación de `.sql` (aquí: lista aplicada por app en Distribuidora). |
| **debug** | Investigación, exportaciones analíticas, pruebas API/BD no estándar. |
| **legacy** | Compatibilidad, typo, o artefacto a retirar / fusionar. |

---

## 3. Convenciones propuestas

### Naming

- Prefijo por dominio: `distribuidora_`, `rutero_`, `bsale_` cuando no esté claro por carpeta.
- Sufijo por acción: `_sync`, `_audit`, `_cleanup`, `_export`, `_debug`.
- Un entrypoint = un `main()` o `if __name__ == "__main__"` documentado en docstring con ejemplo `python -m …` o `python backend/…`.

### Logging

- Usar `logging.getLogger(__name__)`; nivel por env (`LOG_LEVEL`).
- En Coolify: salida estructurada en **stdout** para hechos; **stderr** para warnings/errores (alineado con `audit_document_related.py`).
- Evitar tokens, passwords y PII en logs (enmascarar IDs largos si aplica).

### Dry-run

- Scripts que **escriban** en BD: flag explícito `--execute` o `--dry-run` por defecto `True` (como `cleanup_document_related_invalid_types.py`).
- Jobs de sync: no suelen tener dry-run global; documentar “solo staging” si se añade.

### Backup

- Antes de `DELETE`/`TRUNCATE` en producción: tabla `*_backup` / `COPY … TO` / snapshot RDS; mismo `BEGIN` → backup → mutación → `COMMIT` cuando sea una unidad lógica.
- Registrar `cleaned_at` o `batch_id` en tablas de backup.

---

## 4. Registro de scripts y jobs (tabla)

| Ruta | Tipo | Riesgo | Modifica BD | Bsale API | Recomendado prod | Descripción breve |
|------|------|--------|-------------|-----------|------------------|-------------------|
| `backend/jobs/sync_bsale_distribuidora.py` | job | alto | sí | sí | sí (cron/Coolify) | Sync incremental Distribuidora: órdenes, ventas, relaciones; delega en `sync_service` / related. |
| `backend/jobs/sync_distribuidora_related.py` | job | medio | sí | sí | sí (cron/Coolify opcional) | Solo sync `document_related` vía `relateddetailid`; lock distinto al sync principal. |
| `backend/jobs/sync_rutero.py` | job | medio | sí | no (solo PG) | sí (cron/Coolify) | Sync `bsale.clients` → `bsale.rutero` (empresa 3, vendedores ruta). |
| `backend/jobs/sync_bsale_catalog.py` | job | alto | sí | sí | sí (cron/Coolify) | Catálogo Bsale: `sync_catalog` → precios → stock → SEC `units_per_box` → UPSERT `products_master` (sin borrar filas ni pisar cubicación manual). |
| `backend/audits/audit_products_master_logistics.py` | audit | bajo | no | no | sí (staging/prod) | KPIs PM/variants/products; `--write-doc` actualiza `docs/PRODUCTS_MASTER_LOGISTICS_AUDIT.md`. |
| `backend/jobs/debug_sync_related_oc.py` | debug (shim) | bajo | — | — | no | Reenvía a `backend.debug.debug_sync_related_oc` (`python -m backend.jobs.debug_sync_related_oc` sigue válido). |
| `backend/jobs/debug_full_bsale_relationships.py` | debug (shim) | bajo | — | — | no | Reenvía a `backend.debug.debug_full_bsale_relationships`. |
| `backend/jobs/debug_full_bsalse_relationships.py` | legacy (shim) | bajo | — | — | no | Reenvía a `backend.debug` (typo histórico). |
| `backend/jobs/analyze_related_patterns.py` | debug (shim) | bajo | — | — | no | Reenvía a `backend.debug.analyze_related_patterns`. |
| `backend/jobs/export_oc_analysis_to_excel.py` | debug (shim) | bajo | — | — | no | Reenvía a `backend.debug.export_oc_analysis_to_excel`. |
| `backend/jobs/export_oc_bs_only.py` | debug (shim) | bajo | — | — | no | Reenvía a `backend.debug.export_oc_bs_only`. |
| `backend/debug/debug_sync_related_oc.py` | debug | bajo | sí | sí | no | Sync related para **una** OC por número; implementación real. |
| `backend/debug/debug_full_bsale_relationships.py` | debug | bajo | solo lectura típica | sí | no | Diagnóstico OC↔fuentes de relación en Bsale + PG. |
| `backend/debug/debug_full_bsalse_relationships.py` | legacy | bajo | igual que arriba | sí | no | Compat typo → importa `backend.debug.debug_full_bsale_relationships`. |
| `backend/debug/analyze_related_patterns.py` | debug | bajo | solo lectura | sí | no | Muestreo API `relateddetailid` sobre OC recientes (throttle). |
| `backend/debug/export_oc_analysis_to_excel.py` | debug | bajo | solo lectura | sí | no | Excel análisis OC cruzando API + PG (`oc_analysis.xlsx`). |
| `backend/debug/export_oc_bs_only.py` | debug | bajo | no | sí | no | Listado OC solo API Bsale → Excel (sin cruce PG en flujo principal). |
| `backend/audits/audit_document_related.py` | audit | bajo | no | no | sí (Coolify read-only) | Implementación real; shim en `backend/scripts/audit_document_related.py`. |
| `backend/maintenance/cleanup_document_related_invalid_types.py` | maintenance | medio | sí (DELETE acotado) | no | no (manual aprobado) | Implementación real; shim en `scripts/`. |
| `backend/debug/analyze_purchase_orders_relationships.py` | debug | bajo | no | sí | no | Análisis API-only; token por env (`backend.utils.bsale_token_env`); shim en `scripts/`. |
| `backend/debug/export_bsale_documents_test.py` | debug | medio | opcional (test) | sí | no | Export/debug documentos Bsale; shim en `scripts/`. |
| `backend/debug/test_bsale_documents_office_1.py` | debug | medio | sí (tablas `app.*_bc_test`) | sí | no | Prueba descarga office 1; shim en `scripts/`. |
| `backend/debug/debug_document_types.py` | debug | bajo | no / mínimo | sí | no | Diagnóstico tipos documento; shim en `scripts/`. |
| `backend/debug/debug_single_document.py` | debug | bajo | no | sí | no | Inspección un documento; shim en `scripts/`. |
| `backend/maintenance/gen_staff_password_hash.py` | maintenance | bajo | no | no | solo operadores | Implementación real; shim en `scripts/`. |
| `backend/maintenance/gen_vendedores_app_password_hash.py` | maintenance | bajo | no | no | solo operadores | Hash bcrypt app vendedores; shim en `scripts/`. |
| `backend/test_db.py` | legacy | **crítico** | — | — | **no** | **Eliminado (FASE 5.1)** — credenciales en código; usar `get_connection()` + `PG_*`. |
| `backend/repositories/distribuidora/sync_repo.py` → `backend/sql/distribuidora/*.sql` | migration | alto | sí (DDL) | no | sí (deploy controlado) | Orden: `001`…`012` aplicado por `ensure_distribuidora_schema`. |
| Otros `backend/sql/**/*.sql` | migration | variable | sí (si se ejecutan) | no | según playbook | DDL/features fuera de la cadena auto-Distribuidora; ejecutar con proceso documentado. |

**`rebuild`:** hoy la reconstrucción masiva de `document_related` vive en `sync_related_service.sync_related_documents_range` invocable desde API/router, no como script suelto; fila conceptual: *rebuild = job con ventana + lock*, documentar en runbook.

---

## 5. Duplicados, obsoletos, peligrosos, Coolify

### Duplicados / solapamiento temático

- **OC → Excel / análisis:** `backend/debug/export_oc_analysis_to_excel.py`, `backend/debug/export_oc_bs_only.py`, `backend/debug/analyze_purchase_orders_relationships.py` (tres enfoques; unificar o marcar uno canónico).
- **Debug relaciones OC:** `backend/debug/debug_full_bsale_relationships.py` vs `backend/debug/debug_full_bsalse_relationships.py` (**duplicado real por typo**).
- **Descarga masiva documentos Bsale:** `backend/debug/export_bsale_documents_test.py` vs `backend/debug/test_bsale_documents_office_1.py` (similar; uno orientado a test schema).

### Obsoletos / candidatos a consolidación

- `backend/debug/debug_full_bsalse_relationships.py`: shim de compatibilidad (typo); migrar imports y borrar.
- `analyze_purchase_orders_relationships.py`: ~~reemplazar token~~ corregido (FASE 5.1); usar `.env`.
- `backend/test_db.py`: **eliminado** (FASE 5.1); rotar credenciales si alguna vez estuvo en remoto.

### Peligrosos

- **`cleanup_document_related_invalid_types.py --execute`:** mutación irreversible salvo restaurar desde `document_related_cleanup_backup`.
- **Jobs de sync** en entorno equivocado: sobrescritura masiva de `distribuidora.*` y `bsale.rutero`.
- **JWT:** si no se define `JWT_SECRET_KEY`, `auth.py` usa fallback de desarrollo y emite `warning` (ver `HARDENING_REPORT.md`).

### Recomendados como Coolify Jobs (o cron equivalente)

| Job sugerido | Comando típico | Notas |
|--------------|----------------|--------|
| Sync Distribuidora | `python -m backend.jobs.sync_bsale_distribuidora` | Core ERP; requiere token y PG. |
| Sync related | `python -m backend.jobs.sync_distribuidora_related` | Separar schedule del sync principal si se desea. |
| Sync rutero | `python -m backend.jobs.sync_rutero` | Sin Bsale HTTP si solo PG; confirmar dependencias. |
| Auditoría document_related | `python -m backend.audits.audit_document_related --stdout-only --no-plan-print` | Preferir módulo canónico; el path `backend/scripts/…` sigue como shim. |

**No** programar en Coolify sin revisión: `cleanup_*`, `debug_*`, `export_*` masivos.

---

## 6. Próximos pasos

1. ~~Crear carpetas y mover~~ **Hecho (FASE 6).**
2. ~~Actualizar imports básicos y docs internas~~ **Parcial:** revisar `docs/backend/*.md` y runbooks Coolify externos.
3. Eliminar `debug_full_bsalse_*` tras grep sin referencias.
4. Opcional: retirar shims de `backend/scripts/` cuando no haya dependencias.

---

*Última revisión de inventario: FASE 4 (documento generado en repo; rutas relativas al root del proyecto).*
