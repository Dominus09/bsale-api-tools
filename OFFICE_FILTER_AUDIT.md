# FASE 7.13 — Auditoría filtro `officeId` en documents sync

**Fecha:** 2026-05-19  
**Alcance:** diagnóstico únicamente (sin cambios de código).  
**Síntoma reportado:** `backfill_documents_may_2026` recibe documentos de `office_id` 3, 4 y 5 en la respuesta API; `documents_repo` los descarta con log *"Documento omitido por office distinta"*.

---

## 1. Resumen ejecutivo

| Pregunta | Conclusión |
|----------|------------|
| ¿Se envía filtro de sucursal en listados? | **Sí**, en cada GET listado vía `_documents_get_resync`. |
| ¿Nombre del parámetro coincide con Bsale? | **Probablemente no**: documentación oficial usa **`officeid`** (minúsculas); el código envía **`officeId`** (camelCase). |
| ¿La API filtra por sucursal? | **Comportamiento observado: no** (o no de forma fiable): llegan ítems con `office.id` ∈ {3,4,5,…} pese al parámetro enviado. |
| ¿Por qué el backfill “funciona”? | **Filtro defensivo en Python** (`document_dict_from_bsale`) antes del upsert. |
| Clasificación del bug | **Implementación del nombre de query param** (hipótesis principal) + **comportamiento/limitación API Bsale** con `emissiondaterange` (secundario). |

---

## 2. Constantes y mezcla company / office

Definidas en `backend/services/distribuidora/sync_service.py`:

```python
COMPANY_ID = 3   # empresa (Quillotana SPA / Distribuidora en modelo interno)
OFFICE_ID = 1    # sucursal objetivo (Distribuidora)
```

| Concepto | Valor código | En request API listado | En filtro local (`documents_repo`) |
|----------|--------------|-------------------------|-------------------------------------|
| Empresa | `company_id = 3` | **No** se envía `companyId` en GET listado | `company.id` del JSON debe ser 3 |
| Sucursal | `office_id = 1` | Se envía como query param (ver §4) | `office.id` del JSON debe ser 1 |

**Importante:** Los `office_id` 3, 4 y 5 que aparecen en logs son **IDs de sucursal** en el JSON (`office.id`), no `company_id`. No hay evidencia en código de confusión `COMPANY_ID` ↔ `OFFICE_ID` en el query string; la confusión operativa sería interpretar “office 3” como empresa.

---

## 3. Flujo `backfill_documents_may_2026` → `_fetch_documents_single_day_resync`

```
backend/jobs/backfill_documents_may_2026.py
  → backfill_distribuidora_documents_may_2026_documents_only()
      → _fetch_documents_single_day_resync()   # por cada día UTC
          → _documents_get_resync()            # único GET listado
              → _append_items_from_bsale_response()
                  → document_dict_from_bsale(..., default_office_id=OFFICE_ID)
                      → upsert_documents() si row != None
```

- **No** se aplica `_allowed_document_type_ids` en el backfill mayo (entran todos los tipos que devuelva la API).
- **Sí** se aplica filtro de sucursal/empresa en `documents_repo` (descarte local).

Fragmento relevante de `_fetch_documents_single_day_resync` (`sync_service.py` ~493–526):

```python
params = {
    "limit": pl,
    "offset": offset,
    "emissiondaterange": f"[{desde_ts},{hasta_ts}]",
}
data = _documents_get_resync(client, params)
```

`_utc_day_timestamp_bounds` alinea el rango al día calendario **UTC** (misma convención que `sync_documents.py` legacy).

---

## 4. Request real enviado (A, B, F)

### 4.1 Punto único de listado

Toda paginación de documentos del sync productivo pasa por **`_documents_get_resync`** (`sync_service.py` ~96–162):

```python
def _documents_get_resync(client: BsaleClient, params: dict[str, Any]) -> dict[str, Any]:
    params = {**params, "officeId": OFFICE_ID}
    url = f"{BASE_BSALE}/documents.json"
    r = client.session.get(
        url,
        headers={"access_token": client._token},
        params=params,
        timeout=90,
    )
```

- **Método:** GET  
- **Host/ruta:** `https://api.bsale.io/v1/documents.json` (`BASE_BSALE` en `bsale_client.py`)  
- **Auth:** header `access_token` (no query)  
- **Parámetros finales** (merge del caller + forzado):

| Parámetro | Valor ejemplo (un día mayo 2026) | Origen |
|-----------|-----------------------------------|--------|
| `limit` | 25–50 | `_resync_page_limit()` / `LIMIT_BSALE` |
| `offset` | 0, 50, 100, … | paginación |
| `emissiondaterange` | `[1714521600,1714607999]` | día UTC |
| **`officeId`** | **`1`** | **siempre inyectado aquí** |

### 4.2 URL ejemplo (codificación `requests`)

Para el día **2026-05-15** UTC (timestamps ilustrativos):

```http
GET https://api.bsale.io/v1/documents.json?limit=50&offset=0&emissiondaterange=%5B1715731200%2C1715817599%5D&officeId=1
Header: access_token: <BSALE_TOKEN|BSALE_TOKEN_SPA>
```

### 4.3 Documentación Bsale vs implementación (B)

Fuente: [API Chile — Documentos](https://apichile.bsalelab.com/lista-de-endpoints/documentos) (parámetros GET listado):

| Documentación Bsale | Código actual |
|---------------------|---------------|
| `officeid` | `officeId` |
| `emissiondaterange` | `emissiondaterange` ✓ |
| `documenttypeid` | no usado en backfill listado |
| `limit`, `offset` | ✓ |

Otros endpoints del mismo código usan **minúsculas** en query params relacionados, p. ej. `relateddetailid` en `sync_related_service.py` — coherente con la doc Bsale, **no** con `officeId`.

**Hipótesis principal:** si Bsale solo reconoce `officeid`, el parámetro `officeId=1` sería **ignorado**; el listado quedaría filtrado solo por `emissiondaterange` (y alcance del token → típicamente empresa), devolviendo **todas las sucursales** con emisión en ese rango.

**Verificación recomendada (manual, sin cambiar prod):**

1. Misma llamada con `officeid=1` en lugar de `officeId=1`.  
2. Comparar conteo de ítems y distribución de `office.id` en `items[]`.  
3. Opcional: `curl -v` y revisar query string exacto en access log.

---

## 5. Cliente Bsale (`bsale_client.py`)

`BsaleClient.get(path, params)`:

- Concatena `BASE_BSALE + path` si `path` es relativo.  
- Pasa `params` a `requests` **sin renombrar** claves.  
- **No** añade `officeId` por sí solo (solo lo hace `_documents_get_resync`).

Sub-recursos por documento (`/documents/{id}/details.json`, etc.) **no** llevan `officeId`; no afectan el listado masivo.

---

## 6. Todos los llamados a `GET /documents.json` (listado)

| Origen | Función | `officeId` en query | Otros params |
|--------|---------|---------------------|--------------|
| Sync / resync / backfill mayo | `_documents_get_resync` ← `_fetch_documents_single_day_resync`, `_fetch_documents_window` | **Sí** (`officeId=1`) | `emissiondaterange`, `limit`, `offset` |
| Sync related | `BsaleClient.get("/documents.json", {...})` | **Sí** (`officeId=1`) | `relateddetailid`, `limit`, `offset` |
| Scripts debug / export | varios en `backend/debug/*` | **Sí** (`officeId=1`) | según script |
| Legacy raíz | `sync_documents.py` | **No** envía office | solo `emissiondaterange` |

**Conclusión:** el pipeline productivo de documentos **sí intenta** filtrar por sucursal en listado; el legacy `sync_documents.py` es un camino distinto (sin office en query).

---

## 7. Respuesta API y filtro local (C, G)

### 7.1 Qué devuelve la API (observado)

Logs de operación: ítems con sucursales **3, 4, 5** en el JSON de respuesta, pese a `officeId=1` en el request.

Eso indica que el filtro **no** se aplica en servidor (o no con el nombre de parámetro usado).

### 7.2 Filtro Python (`documents_repo.py`)

`document_dict_from_bsale` (~86–159):

1. `company.id` debe ser `3` → si no, log *"Documento omitido por company distinta"* y `skipped_other_company++`.  
2. `office.id` debe existir y ser `1` → si no, log *"Documento omitido por office distinta"* y `skipped_other_office++`.  
3. Si pasa, `office_id` persistido = `int(office.id)` del JSON (no se fuerza `1` sin validar).

Comentario en código (línea ~97): *"defensa adicional al filtro officeId en API"* — confirma que **ya se asumía** que la API podía devolver otras sucursales.

### 7.3 Evidencia en repo

`backend/debug/test_bsale_documents_office_1.py` (~347–354): tras el GET con `officeId`, **filtra en cliente** `if oid != OFFICE_ID: continue` — patrón explícito de desconfianza hacia el filtro API.

---

## 8. `documenttypeid` y paginación (C)

| Tema | Backfill mayo | Efecto |
|------|---------------|--------|
| `documenttypeid` | **No** enviado | La API puede devolver tipos 1, 6, 9, 33, etc.; no explica offices 3–5. |
| Paginación | `offset` += `len(items)`; **mismos** params cada página (incl. `officeId`) | No hay bug de “pierde officeId en página 2”. |
| `emissiondaterange` + office | Ambos en misma request | Doc Bsale los lista como filtros independientes; si `officeid` no aplica, el rango de fechas sigue activo → volumen alto multi-sucursal. |

---

## 9. `relateddetailid` y `officeId` (E)

`sync_related_service.py` — `_fetch_all_items_for_relateddetailid` / resolución OC:

```python
client.get(
    "/documents.json",
    {
        "relateddetailid": detail_id,
        "limit": RELATED_DETAIL_PAGE_LIMIT,
        "offset": offset,
        "officeId": OFFICE_ID,
    },
)
```

- Mismo nombre **`officeId`** (camelCase).  
- Doc Bsale menciona `relatedDetailId` en estructura JSON; param de listado relacionado suele ir en minúsculas en APIs Chile (`relateddetailid` en código ✓).  
- Post-filtro: `_office_id_from_blob`, `_related_item_office_id`, validación contra BD (`office_id=1`) y métricas `related_office_mismatch_*`.

**Riesgo:** si `officeId` no filtra en listado por fecha, tampoco es seguro asumir que filtra en `relateddetailid` sin prueba; hoy el código **compensa** con lógica de aceptación/rechazo por office en ítem y en BD.

---

## 10. Impacto potencial

| Área | Impacto |
|------|---------|
| **documents / backfill mayo** | Más páginas API, más CPU/logs, `skipped_other_office` alto; solo persiste office 1. Datos finales en BD **correctos** si el filtro local no tiene bugs. |
| **details / attributes / references** | Solo se descargan hijos de documentos **ya persistidos** (office 1). No se “contaminan” por offices 3–5 en cabeceras omitidas. |
| **related sync** | Posibles llamadas extra y rechazos por office; métricas y reglas `_accept_related_triple` mitigan. |
| **ERP frontend** | Consultas sobre `distribuidora.documents` con `office_id=1` coherentes **si** el backfill terminó; KPIs pueden subestimar si muchos ítems se omiten (no se guardan otras sucursales — es intencional). |
| **Rendimiento / cuotas Bsale** | Costo API inflado por traer documentos que se descartan después. |

---

## 11. Clasificación del bug (tabla final)

| Hipótesis | Probabilidad | Notas |
|-----------|--------------|-------|
| Parámetro incorrecto (`officeId` vs `officeid`) | **Alta** | Alineado con doc Bsale y con patrón `relateddetailid` en minúsculas. |
| API Bsale ignora office con `emissiondaterange` | Media | Podría ser consecuencia del nombre incorrecto; probar con `officeid` aislado. |
| Paginación | Baja | Params estables en todas las páginas. |
| Mezcla `company_id` / `office_id` en query | Baja | No se envía company en listado; token acota empresa. |
| Implementación Python del merge | Baja | `officeId` siempre se fuerza a `1` al final del dict. |

---

## 12. Por qué el backend filtra localmente (G)

Por diseño explícito:

1. Comentario y logs en `documents_repo`.  
2. Script de prueba `test_bsale_documents_office_1.py` filtra post-API.  
3. Métricas `skipped_other_office` / `skipped_other_company` en stats de sync.

El filtro local **no** es un bug; es **compensación** ante un filtro API que no cumple lo esperado (o no está bien parametrizado).

---

## 13. Pruebas manuales sugeridas (8, 9)

### 13.1 Confirmar ruta en OpenAPI

Tras deploy del código actual:

1. Abrir `/docs`.  
2. Buscar `POST`/`GET` bajo tag **Operaciones** o rutas `/operaciones/*` (telemetría; distinto tema).  
3. Para documents: buscar operación de listado vía resync no está expuesta como endpoint propio; el backfill es job interno.  
4. Validar en **runtime** con log startup o script:

```bash
python -m backend.scripts.verify_operaciones_routes   # telemetría
# Para documents: inspeccionar rutas no aplica; usar curl directo a Bsale abajo
```

### 13.2 `curl` contra Bsale (misma ventana)

```bash
# A) Como el código hoy (officeId camelCase)
curl -s -G "https://api.bsale.io/v1/documents.json" \
  -H "access_token: $BSALE_TOKEN" \
  --data-urlencode "limit=5" \
  --data-urlencode "offset=0" \
  --data-urlencode "emissiondaterange=[1715731200,1715817599]" \
  --data-urlencode "officeId=1" \
  | jq '[.items[]? | {id, office: .office.id, company: .company.id}]'

# B) Según documentación Bsale (officeid minúsculas)
curl -s -G "https://api.bsale.io/v1/documents.json" \
  -H "access_token: $BSALE_TOKEN" \
  --data-urlencode "limit=5" \
  --data-urlencode "offset=0" \
  --data-urlencode "emissiondaterange=[1715731200,1715817599]" \
  --data-urlencode "officeid=1" \
  | jq '[.items[]? | {id, office: .office.id}]'
```

Comparar conteos y `office.id` en A vs B.

### 13.3 App móvil vs backend

- **Documents listado backfill:** `GET https://api.bsale.io/v1/documents.json?...&officeId=1` (hoy).  
- **Heartbeat / gps_track:** `POST /operaciones/...` — **no** relacionado con este audit.  
- Si en el futuro la app llamara listados Bsale directos, alinear nombre de param con doc Bsale.

---

## 14. Archivos revisados

| Archivo | Rol |
|---------|-----|
| `backend/services/distribuidora/sync_service.py` | `_documents_get_resync`, `_fetch_documents_single_day_resync`, constantes, backfill mayo |
| `backend/services/distribuidora/bsale_client.py` | Cliente HTTP genérico |
| `backend/repositories/distribuidora/documents_repo.py` | Filtro local company/office |
| `backend/jobs/backfill_documents_may_2026.py` | Job mayo |
| `backend/services/distribuidora/sync_related_service.py` | `relateddetailid` + `officeId` |
| `backend/debug/test_bsale_documents_office_1.py` | Evidencia filtro cliente |
| `sync_documents.py` (raíz) | Legacy sin `officeId` en query |
| Documentación Bsale API Chile | Parámetro `officeid` |

---

## 15. Próximo paso (fuera de esta auditoría)

Cuando se autorice corrección (FASE posterior):

1. Unificar query param a **`officeid`** (y revisar `relateddetailid` / `officeId` en related).  
2. Prueba A/B en staging con conteo por `office.id`.  
3. Opcional: log DEBUG en `_documents_get_resync` con URL final (`r.url` de `requests`) y histograma `office.id` por página.  
4. Documentar en `DOCUMENTS_MAY_2026_RUNBOOK.md`.

**No se modificó código en esta fase**, conforme alcance 7.13.
