# FASE 6 — Informe de migración estructural

**Fecha:** reorganización controlada de tooling ERP (sin cambiar lógica de negocio, SQL core, `services/`, `routers/`, ni frontend).

**Referencias:** `SCRIPTS_REGISTRY.md`, `SCRIPTS_CONSOLIDATION_PLAN.md`, `HARDENING_REPORT.md`.

---

## 1. Carpetas creadas

| Ruta | Rol |
|------|-----|
| `backend/audits/` | Auditorías read-only |
| `backend/maintenance/` | Cleanup y utilidades operador |
| `backend/debug/` | Diagnóstico y exports analíticos |
| `backend/rebuilds/` | Reservado (futuro) |
| `backend/migrations/` | Reservado (runners DDL; DDL sigue en `backend/sql/`) |
| `backend/__init__.py` | Paquete explícito `backend` |

`backend/jobs/` y `backend/utils/` ya existían.

---

## 2. Archivos movidos (implementación real)

### Desde `backend/scripts/` → destino

| Origen | Destino |
|--------|---------|
| `scripts/audit_document_related.py` | `audits/audit_document_related.py` |
| `scripts/cleanup_document_related_invalid_types.py` | `maintenance/cleanup_document_related_invalid_types.py` |
| `scripts/gen_staff_password_hash.py` | `maintenance/gen_staff_password_hash.py` |
| `scripts/gen_vendedores_app_password_hash.py` | `maintenance/gen_vendedores_app_password_hash.py` |
| `scripts/analyze_purchase_orders_relationships.py` | `debug/analyze_purchase_orders_relationships.py` |
| `scripts/debug_document_types.py` | `debug/debug_document_types.py` |
| `scripts/debug_single_document.py` | `debug/debug_single_document.py` |
| `scripts/export_bsale_documents_test.py` | `debug/export_bsale_documents_test.py` |
| `scripts/test_bsale_documents_office_1.py` | `debug/test_bsale_documents_office_1.py` |

### Desde `backend/jobs/` → `backend/debug/`

| Origen | Destino |
|--------|---------|
| `jobs/debug_sync_related_oc.py` | `debug/debug_sync_related_oc.py` |
| `jobs/debug_full_bsale_relationships.py` | `debug/debug_full_bsale_relationships.py` |
| `jobs/debug_full_bsalse_relationships.py` | `debug/debug_full_bsalse_relationships.py` |
| `jobs/analyze_related_patterns.py` | `debug/analyze_related_patterns.py` |
| `jobs/export_oc_analysis_to_excel.py` | `debug/export_oc_analysis_to_excel.py` |
| `jobs/export_oc_bs_only.py` | `debug/export_oc_bs_only.py` |

### Permanecen en `backend/jobs/` (CORE)

- `sync_bsale_distribuidora.py`
- `sync_distribuidora_related.py`
- `sync_rutero.py`
- `__init__.py`

---

## 3. Compatibilidad (shims)

### `backend/scripts/*.py`

Archivos **delgados** que insertan la raíz del repo en `sys.path` y reenvían al módulo canónico. Permiten seguir ejecutando, por ejemplo:

`python backend/scripts/audit_document_related.py`

### `backend/jobs/debug_*.py` y `export_oc_*.py`, `analyze_related_patterns.py`

Reemplazados por shims que importan desde `backend.debug.*` para conservar:

`python -m backend.jobs.debug_sync_related_oc`

---

## 4. Imports y rutas ajustados

| Área | Cambio |
|------|--------|
| `backend/debug/debug_full_bsalse_relationships.py` | Importa `main` desde `backend.debug.debug_full_bsale_relationships`. |
| `backend/debug/*.py` | Docstrings / mensajes de uso actualizados a `python -m backend.debug.…`. |
| `backend/services/distribuidora/sync_related_service.py` | Referencias de ayuda a `python -m backend.debug.debug_sync_related_oc`. |
| `backend/sql/update_staff_password_example.sql` | Comentario con `python -m backend.maintenance.gen_staff_password_hash`. |
| `sql/bsale_vendedores_app.sql` | Comentario con ruta `backend/maintenance/…`. |
| `backend/README.md` | Tabla de carpetas + enlaces a informes. |

**No modificado:** lógica dentro de `sync_service`, repositorios, routers, SQL Distribuidora `001…012`, FastAPI `main.py`.

---

## 5. Riesgos

| Riesgo | Mitigación |
|--------|------------|
| CI o Coolify invoca ruta antigua `python backend/jobs/debug_*.py` como **archivo** | Usar `-m` o actualizar comando al nuevo módulo; shims en `jobs/` cubren `-m backend.jobs.*`. |
| `python backend/scripts/…` sin cwd en raíz del repo | Shims añaden `sys.path` al padre del paquete `backend`. |
| Confusión `backend.debug` vs stdlib `debug` | Siempre importar como `backend.debug` (calificado). |

---

## 6. Pendientes sugeridos

1. Actualizar documentación larga en `docs/backend/*.md` (referencias a `backend/jobs/` para herramientas de debug).
2. Coolify: preferir comandos canónicos `python -m backend.audits.audit_document_related` y `python -m backend.debug.*`.
3. Tras un periodo de gracia, **eliminar shims** de `backend/scripts/` y `backend/jobs/` si ya no hay referencias.
4. Poblar `rebuilds/` con un script/runbook cuando exista reconstrucción masiva aprobada.

---

## 7. Verificación rápida

Desde la raíz del repositorio (con entorno y `PG_*` si aplica):

```bash
python -c "import backend.jobs.sync_bsale_distribuidora; import backend.audits.audit_document_related; import backend.debug.debug_sync_related_oc"
python -m backend.jobs.debug_sync_related_oc --help 2>nul || python -m backend.debug.debug_sync_related_oc
```

*(El segundo comando solo comprueba resolución de módulo si no se pasan args numéricos.)*

---

*FASE 6 — estructura física alineada al plan de consolidación.*
