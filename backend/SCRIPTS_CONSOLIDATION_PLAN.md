# FASE 5 — Plan de consolidación de scripts ERP

**Estado:** plan de consolidación; **FASE 6** aplicó la estructura de carpetas y movimientos (ver [`STRUCTURE_MIGRATION_REPORT.md`](./STRUCTURE_MIGRATION_REPORT.md)). No se refactorizó lógica de negocio.

**Higiene (FASE 5.1):** [`HARDENING_REPORT.md`](./HARDENING_REPORT.md).

**Entrada:** `backend/SCRIPTS_REGISTRY.md` (inventario y riesgos).

**Objetivo final:** base estable para Coolify (jobs), automatización de syncs, evolución multiempresa, y capas financieras/márgenes sin caos operativo.

---

## 1. Clasificación A–E por artefacto

Leyenda: un mismo archivo puede llevar **varias** etiquetas (ej. CORE + DANGEROUS si mal usado).

### A) CORE_PRODUCTION — deben sobrevivir y estar gobernados

| Artefacto | Rol |
|-----------|-----|
| `backend/jobs/sync_bsale_distribuidora.py` | Sync oficial Distribuidora (órdenes, ventas, relaciones vía servicios). |
| `backend/jobs/sync_distribuidora_related.py` | Sync dedicado `document_related` (cron opcional). |
| `backend/jobs/sync_rutero.py` | Sync rutero PG. |
| `backend/audits/audit_document_related.py` | Salud `document_related` (stdout-first; Coolify-friendly). |
| `backend/repositories/distribuidora/sync_repo.py` + `backend/sql/distribuidora/001…012.sql` | DDL Distribuidora aplicado en deploy / ensure schema. |
| `backend/services/distribuidora/*` | **No son scripts**; son la implementación que los jobs deben seguir invocando (única fuente de lógica de negocio). |

### B) LEGACY — congelar, sustituir o eliminar en fase posterior

| Artefacto | Acción futura (post-aprobación) |
|-----------|--------------------------------|
| `backend/jobs/debug_full_bsalse_relationships.py` | Eliminar tras grep sin referencias; usar solo `debug_full_bsale_relationships.py`. |
| `backend/test_db.py` | **Eliminado** (FASE 5.1); ver `HARDENING_REPORT.md`. |
| `backend/scripts/analyze_purchase_orders_relationships.py` | Opcional: absorber en `debug/` unificado OC (ver fusión); token ya vía env (FASE 5.1). |

### C) DEBUG_ONLY — útiles a mano; nunca como job Coolify silencioso

| Artefacto |
|-----------|
| `backend/debug/debug_sync_related_oc.py` |
| `backend/debug/debug_full_bsale_relationships.py` |
| `backend/debug/debug_full_bsalse_relationships.py` |
| `backend/debug/analyze_related_patterns.py` |
| `backend/debug/analyze_purchase_orders_relationships.py` |
| `backend/debug/export_oc_analysis_to_excel.py` |
| `backend/debug/export_oc_bs_only.py` |
| `backend/debug/debug_document_types.py` |
| `backend/debug/debug_single_document.py` |
| `backend/debug/export_bsale_documents_test.py` |
| `backend/debug/test_bsale_documents_office_1.py` |

### D) DUPLICATED — solape funcional (fusionar o acotar “canónico”)

| Grupo | Miembros | Propuesta conceptual |
|-------|-----------|------------------------|
| OC → análisis / Excel | `export_oc_analysis_to_excel.py`, `export_oc_bs_only.py`, `analyze_purchase_orders_relationships.py` | **Un** CLI `debug_oc_pipeline.py` (o módulo) con subcomandos: `with-pg`, `api-only`, `relateddetail-range`; o mantener 2 como máximo (PG+API vs API-only). |
| Debug relaciones | `debug_full_bsale_relationships.py` + shim `debug_full_bsalse_*` | Un solo módulo; shim borrado. |
| Descarga masiva docs | `export_bsale_documents_test.py`, `test_bsale_documents_office_1.py` | Diferenciar por nombre/carpeta: uno **test schema** (`debug_bsale_documents_bc_test.py`), otro **export genérico**; compartir helpers HTTP. |
| HTTP GET Bsale en jobs | `export_oc_analysis_to_excel.py`, `analyze_related_patterns.py`, `debug_full_bsale_relationships.py` | Cada uno redefine `_get` / patrones similares → extraer a `backend/services/distribuidora/bsale_debug_http.py` o métodos en `BsaleClient` en fase refactor. |

### E) DANGEROUS — requieren runbook, entorno y/o secretos seguros

| Artefacto | Por qué |
|-------------|---------|
| `backend/test_db.py` | ~~Credenciales en código~~ **eliminado** (FASE 5.1). |
| `analyze_purchase_orders_relationships.py` | ~~Token en código~~ corregido (FASE 5.1); pendiente consolidación con otros exports OC. |
| `cleanup_document_related_invalid_types.py --execute` | DELETE + dependencia de backup explícito. |
| `sync_*` jobs | Escritura masiva en PG; riesgo de **entorno equivocado** (staging vs prod). |
| Futuro `rebuild_*` / TRUNCATE `document_related` | Aún no script suelto; cuando exista: misma categoría + advisory lock documentado. |

---

## 2. Propuestas: fusionar / eliminar / renombrar / mover (cuando se ejecute la fase técnica)

### Fusionar (refactor posterior)

1. **Helpers HTTP Bsale** duplicados (`_get`, paginación simple) en `export_oc_analysis_to_excel`, `analyze_related_patterns`, `debug_full_bsale_relationships` → módulo compartido bajo `services/distribuidora/` (p. ej. funciones delgadas sobre `BsaleClient`).
2. **OC export/análisis** → un entrypoint con flags en lugar de tres scripts paralelos (mantener compatibilidad con alias `python -m …` deprecados una versión).

### Eliminar (tras checklist de seguridad)

1. ~~`backend/test_db.py`~~ eliminado (FASE 5.1).
2. `backend/jobs/debug_full_bsalse_relationships.py` (tras cero referencias).
3. ~~Token en claro dentro de `analyze_purchase_orders_relationships.py`~~ corregido (FASE 5.1); valoración de deprecar archivo por solape con otros exports OC.

### Renombrar (alinear naming; ver sección 6)

| Actual | Nombre objetivo (ejemplo) |
|--------|---------------------------|
| `sync_bsale_distribuidora.py` | `sync_distribuidora_bsale.py` o mantener si ya referenciado en Coolify (costo de cambio). |
| `export_oc_analysis_to_excel.py` | `debug_export_oc_analysis_xlsx.py` |
| `debug_sync_related_oc.py` | `debug_sync_distribuidora_related_oc.py` |

**Regla:** renombrar solo en PR dedicado + actualizar docs/Coolify en el mismo merge.

### Mover de carpeta (árbol final — sección 5)

| Origen actual | Destino propuesto |
|----------------|-------------------|
| `scripts/audit_document_related.py` | `audits/audit_document_related.py` |
| `scripts/cleanup_document_related_invalid_types.py` | `maintenance/cleanup_document_related_invalid_types.py` |
| `scripts/debug_*.py`, `export_bsale_*`, `test_bsale_*`, `analyze_purchase_*` | `debug/` |
| `scripts/gen_*_password_hash.py` | `maintenance/` o `scripts/` residual “ops one-liner” |
| `jobs/sync_*.py` | permanecen `jobs/` (CORE) |
| `jobs/debug_*`, `export_*`, `analyze_related_patterns.py` | `debug/` (separar mentalmente “no prod”) |

**Estado FASE 6:** filas anteriores **aplicadas**; `backend/scripts/` contiene shims hacia `audits/`, `maintenance/`, `debug/`; `backend/jobs/` contiene shims hacia `backend.debug` para los comandos `-m backend.jobs.*` antiguos.

---

## 3. Imports compartidos y duplicación técnica

### Imports compartidos (hoy)

- `from backend.db import get_connection` — scripts/jobs/servicios (correcto; **no** duplicar pool propio en scripts nuevos).
- `from backend.services.distribuidora.bsale_client import BsaleClient` — vía servicios en producción; jobs debug repiten instanciación + helpers.
- `from backend.services.distribuidora.sync_service import _bsale_token, …` — jobs de análisis acoplados a helper “privado” `_bsale_token`; consolidación futura: **`public_token_resolver()`** en módulo pequeño `bsale_auth.py` o API explícita en `BsaleClient`.

### Utilidades / lógica repetida (candidatos a librería interna)

- Wrappers `_get(client, path, params)` idénticos o casi entre jobs.
- Límites `DETAILS_LIMIT` / `RELATED_LIMIT` / sleep — centralizar constantes por perfil (`DEBUG_THROTTLE`, `SYNC_THROTTLE`).
- Construcción de rangos de fechas UTC (sync ya tiene utilidades; debug reimplementa en parte).

### Código duplicado / Bsale

- Patrones `documents.json`, `details.json`, `relateddetailid` aparecen en **sync_related_service** (canónico) y en **debug_full_bsale_relationships** / **analyze_related_patterns** (solo lectura diagnóstico).  
  **Objetivo:** debug solo llama primitivas compartidas o `BsaleClient` extendido, no copia URLs/paginación crítica.

### PostgreSQL

- Una sola vía: `get_connection()` + cierre explícito o context manager (estandarizar en scripts nuevos con `with get_connection() as conn:` cuando se refactorice).

---

## 4. Estructura operacional FINAL propuesta

`services/` **ya existe** en el backend: contiene la lógica de negocio; los entrypoints no deben reimplementar sync.

```
backend/
  jobs/              # Solo CORE_PRODUCTION: sync_*.py invocables por Coolify/cron
  audits/            # audit_*.py: lectura + informes (stdout/JSON opcional)
  maintenance/       # cleanup_*, gen_*_hash, backfills puntuales; dry-run por defecto
  rebuilds/          # rebuild_*.py o runbooks versionados (document_related full, etc.)
  debug/             # debug_*, export_* analítico, test_* contra APIs o tablas test
  migrations/        # (opcional) runners Python DDL; hoy DDL en sql/
  services/          # Lógica compartida (Bsale, sync, órdenes, márgenes futuros) — SIN mover fuera
  sql/               # DDL versionado por dominio
  SCRIPTS_REGISTRY.md
  SCRIPTS_CONSOLIDATION_PLAN.md
```

**Principio:** `jobs/` queda **delgado** (orquestación); `services/` conserva reglas de negocio y cliente Bsale.

---

## 5. Naming estándar (prefijos)

| Prefijo | Carpeta típica | Ejemplo |
|---------|-----------------|---------|
| `sync_` | `jobs/` | `sync_distribuidora_bsale.py` |
| `audit_` | `audits/` | `audit_document_related.py` |
| `cleanup_` | `maintenance/` | `cleanup_document_related_invalid_types.py` |
| `rebuild_` | `rebuilds/` | `rebuild_distribuidora_document_related_range.py` |
| `debug_` | `debug/` | `debug_distribuidora_oc_relationships.py` |
| `export_` | `debug/` o `audits/` según solo lectura y volumen | `export_oc_analysis_xlsx.py` |
| `gen_` | `maintenance/` | `gen_staff_password_hash.py` |

Sufijos opcionales: `_xlsx`, `_range`, `_oc`, `_distribuidora`, para multiempresa futura.

---

## 6. Secuencia de ejecución recomendada (post-FASE 5, cuando se apruebe código)

1. **Seguridad:** ~~borrar `test_db.py`~~ hecho (FASE 5.1); ~~quitar secretos del repo (`analyze_purchase_orders_relationships.py`)~~ token Bsale movido a env; rotar tokens expuestos en historial Git.
2. **Compat:** eliminar shim `debug_full_bsalse_*`.
3. **Estructura:** PR1 carpetas + moves mecánicos + actualizar `python -m` en docs y Coolify.
4. **Refactor:** PR2 helpers Bsale compartidos (sin cambiar comportamiento).
5. **Fusión:** PR3 unificar exports OC (mantener deprecación 1 release).
6. **Multiempresa:** parametrizar `COMPANY_ID` / `OFFICE_ID` vía env en jobs CORE (hoy fijos en servicios — roadmap aparte en `services/`).

---

## 7. Objetivo final (Coolify + escala)

| Meta | Cómo apoya este plan |
|------|----------------------|
| Migrar jobs a Coolify | Solo `jobs/sync_*.py` + `audits/audit_* --stdout-only` como comandos estándar. |
| Automatizar syncs | Un solo lugar de verdad (`services/`); jobs sin lógica duplicada. |
| Multiempresa | Naming y carpetas por dominio preparan `distribuidora_*` vs futuros prefijos. |
| Financiero / márgenes | Nuevos jobs `sync_*` / `audit_*` encajan sin mezclar con `debug/` ni secretos en scripts. |

---

*Documento FASE 5. Mantener alineado con `SCRIPTS_REGISTRY.md` tras cada cambio de inventario.*
