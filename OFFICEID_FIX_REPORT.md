# FASE 7.14 — Corrección global `officeId` → `officeid` (Bsale API)

**Fecha:** 2026-05-19  
**Hipótesis validada en 7.13:** Bsale documentación Chile usa query param **`officeid`** (minúsculas). El código enviaba **`officeId`** (camelCase), probablemente **ignorado** por la API → listados multi-sucursal (3, 4, 5) y filtro solo en Python (`documents_repo`).

---

## 1. Resumen ejecutivo

| Aspecto | Antes | Después |
|--------|--------|---------|
| Query param listado documentos | `officeId=1` | `officeid=1` |
| Query param `relateddetailid` | `officeId=1` | `officeid=1` |
| Centralización | Disperso en ~12 archivos | `merge_bsale_office_query()` en `bsale_params.py` |
| Logs debug | — | `[OFFICE_FILTER_DEBUG]` con `OFFICE_FILTER_DEBUG=1` |
| Variables Python `OFFICE_ID`, `office_id` | Sin cambio | Sin cambio |
| Frontend `officeId` → API propia `office_id` | Sin cambio | Sin cambio (no es Bsale) |

---

## 2. Clasificación de ocurrencias `officeId`

### A) Requests API Bsale — **corregidos**

| Archivo | Endpoint / uso |
|---------|----------------|
| `backend/services/distribuidora/sync_service.py` | `GET /documents.json` vía `_documents_get_resync` |
| `backend/services/distribuidora/sync_related_service.py` | `GET /documents.json?relateddetailid=` (2 sitios) |
| `backend/services/distribuidora/bsale_client.py` | Log URL final si `officeid` en params |
| `backend/debug/debug_related_graph_oc.py` | related |
| `backend/debug/analyze_related_patterns.py` | related |
| `backend/debug/export_oc_analysis_to_excel.py` | related |
| `backend/debug/debug_full_bsale_relationships.py` | related + listado emisión |
| `backend/debug/debug_document_types.py` | listado + related |
| `backend/debug/analyze_purchase_orders_relationships.py` | listado + related |
| `backend/debug/export_bsale_documents_test.py` | listado emisión |
| `backend/debug/test_bsale_documents_office_1.py` | listado emisión |

### B) Variables internas Python — **no modificadas**

| Archivo | Motivo |
|---------|--------|
| `sync_service.py` / `sync_related_service.py` | `OFFICE_ID = 1` (constante interna) |
| `documents_repo.py` | `office_id` en filtro local defensivo |
| `frontend/lib/api.ts`, páginas compras | `officeId` → query `office_id` hacia **backend propio**, no Bsale |

### C) Docs / comentarios — **referencia histórica**

| Archivo | Acción |
|---------|--------|
| `OFFICE_FILTER_AUDIT.md` | Auditoría 7.13 (estado pre-fix) |
| `DOCUMENTS_MAY_2026_RUNBOOK.md`, `OC_66615_INVESTIGATION.md`, etc. | Pendiente actualizar menciones `officeId` si se desea alinear docs |

Comentario en `documents_repo.py` actualizado: filtro `officeid` en API.

---

## 3. Implementación nueva

### `backend/services/distribuidora/bsale_params.py`

- `BSALE_QUERY_OFFICE_ID = "officeid"`
- `merge_bsale_office_query(params, office_id)` — inyecta `officeid`, elimina `officeId` legacy
- `log_office_filter_debug_response(...)` — URL final tras GET

### Activar logs temporales

```powershell
$env:OFFICE_FILTER_DEBUG = "1"
$env:LOG_LEVEL = "INFO"
python -m backend.jobs.backfill_documents_may_2026
```

Ejemplo esperado:

```text
[OFFICE_FILTER_DEBUG] documents_get_resync officeid=1 params={'limit': 50, 'offset': 0, 'emissiondaterange': '[...]', 'officeid': 1}
[OFFICE_FILTER_DEBUG] documents_get_resync GET /documents.json officeid=1 url=https://api.bsale.io/v1/documents.json?...&officeid=1
```

---

## 4. Ejemplos request antes / después

### Listado por fecha (`documents.json`)

**Antes (ignorado por API según hipótesis):**

```http
GET /v1/documents.json?limit=50&offset=0&emissiondaterange=[1714521600,1714607999]&officeId=1
```

**Después:**

```http
GET /v1/documents.json?limit=50&offset=0&emissiondaterange=[1714521600,1714607999]&officeid=1
```

### Related por línea OC

**Antes:**

```http
GET /v1/documents.json?relateddetailid=12345&limit=50&offset=0&officeId=1
```

**Después:**

```http
GET /v1/documents.json?relateddetailid=12345&limit=50&offset=0&officeid=1
```

Sub-recursos (`/documents/{id}/details.json`, etc.) **no** llevan filtro office (sin cambio).

---

## 5. Validación obligatoria

### Entorno requerido

- `BSALE_TOKEN` o `BSALE_TOKEN_SPA`
- `PG_HOST`, `PG_DB`, `PG_USER`, `PG_PASSWORD` (opcional `PG_PORT`)

En el entorno del agente **no había** `.env` ni token → backfill no ejecutado aquí.

### 5.1 Documents backfill

```powershell
cd "c:\Users\user\OneDrive\Proyectos cursor\bsale-api-tools"
.\venv\Scripts\Activate.ps1
$env:OFFICE_FILTER_DEBUG = "1"
python -m backend.jobs.backfill_documents_may_2026 2>&1 | Tee-Object -FilePath backfill_docs_may_after.log
```

**Criterio de éxito:** contar líneas `Documento omitido por office distinta` — debe ser **0** o residual excepcional.

```powershell
Select-String -Path backfill_docs_may_after.log -Pattern "Documento omitido por office distinta" | Measure-Object
```

**Antes (7.13):** muchos omitidos con `office_id` 3, 4, 5 en respuesta API.  
**Después (esperado):** API solo devuelve office 1 → omitidos ≈ 0.

### 5.2 Related backfill

```powershell
python -m backend.jobs.backfill_related_may_2026 2>&1 | Tee-Object -FilePath backfill_related_may_after.log
```

Revisar en panel ERP / SQL:

- Sin relaciones cruzadas de otras sucursales
- Terminales 1/6/9 coherentes
- OCs pendientes vs facturadas estables

### 5.3 Frontend

Tras backfills en BD:

- OCs facturadas no reaparecen como pendientes
- Montos cuadran con Bsale office 1
- Terminales correctos
- Diferencias ERP desaparecen

---

## 6. Métricas antes / después (plantilla)

Completar tras ejecutar en entorno con credenciales:

| Métrica | Antes (7.13) | Después (7.14) |
|---------|--------------|----------------|
| Logs `Documento omitido por office distinta` (mayo backfill) | _alto (offices 3,4,5)_ | _pendiente medición_ |
| `documents_processed` backfill mayo | _N_ | _N'_ |
| `document_api_pages` | _alto si API no filtraba_ | _esperado menor_ |
| Items API con `office.id` ≠ 1 en primera página | frecuente | esperado 0 |
| Related: aristas office cruzada | posible | esperado ↓ |

---

## 7. Impacto esperado downstream

| Área | Efecto |
|------|--------|
| `distribuidora.documents` | Menos ruido API; menos `skipped_other_office` |
| `document_related` | Menos documentos ajenos en `relateddetailid` |
| Terminales ERP | Menos NC/ventas de otras sucursales |
| OCs pendientes/facturadas | Menos “reaparición” por datos cruzados |
| Carga API | Menor volumen por página (solo office 1) |

El filtro defensivo en `documents_repo.document_dict_from_bsale` **se mantiene** (no se elimina).

---

## 8. Archivos tocados (diff 7.14)

```
backend/services/distribuidora/bsale_params.py          (nuevo)
backend/services/distribuidora/bsale_client.py
backend/services/distribuidora/sync_service.py
backend/services/distribuidora/sync_related_service.py
backend/repositories/distribuidora/documents_repo.py    (comentario)
backend/debug/debug_related_graph_oc.py
backend/debug/analyze_related_patterns.py
backend/debug/export_oc_analysis_to_excel.py
backend/debug/debug_full_bsale_relationships.py
backend/debug/debug_document_types.py
backend/debug/analyze_purchase_orders_relationships.py
backend/debug/export_bsale_documents_test.py
backend/debug/test_bsale_documents_office_1.py
OFFICEID_FIX_REPORT.md                                  (este archivo)
```

---

## 9. Prueba rápida local (sin API)

```powershell
python -c "from backend.services.distribuidora.bsale_params import merge_bsale_office_query; assert merge_bsale_office_query({'officeId':99},1)=={'officeid':1}"
```

---

## 10. Próximos pasos operativos

1. Ejecutar backfills 5.1 y 5.2 con credenciales y rellenar sección 6.
2. Si persisten omitidos con `officeid=1`, abrir ticket Bsale o probar filtro combinado `officeid` + `companyid=3`.
3. Tras validar, desactivar `OFFICE_FILTER_DEBUG` en jobs programados.
4. Opcional: actualizar runbooks (`DOCUMENTS_MAY_2026_RUNBOOK.md`) `officeId` → `officeid`.
