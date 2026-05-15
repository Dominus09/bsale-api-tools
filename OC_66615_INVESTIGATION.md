# Investigación — OC 66615 y detección vía `relateddetailid` (backfill mayo 2026)

## Contexto del síntoma

La OC **66615** consta como facturada en Bsale (factura visible en la UI), pero el job **`backfill_related_may_2026`** no deja traza operacional en **`distribuidora.document_related`** para esa factura.

Ese backfill **solo** usa la ruta API documentada en el runbook y en el servicio de sync related:

- `GET /v1/documents/{document_id}/details.json` → `detail.id` por línea.
- `GET /v1/documents.json?relateddetailid={detail_id}&officeId=1` → documentos “enganchados” a esa **línea de detalle** de la OC.

No consulta `references.json` ni mezcla con `document_references` (sync principal).

Referencias en código: módulo `backend/services/distribuidora/sync_related_service.py` (cabecera del archivo y `RELATED_MAY_2026_RUNBOOK.md`).

---

## Limitación de esta corrida (evidencia en vivo)

En el entorno donde se redactó este informe **no estaba disponible** `BSALE_TOKEN` / `BSALE_TOKEN_SPA` ni acceso a la base `PG_*`. Por tanto **no se adjuntan aquí respuestas JSON reales** de Bsale para la OC 66615.

Lo siguiente es **metodología reproducible** y **conclusión técnica** a partir del contrato del pipeline y de la semántica habitual de la API Bsale v1.

---

## Metodología recomendada (evidencia A vs B)

### 1) Resolver `document_id` de la OC 66615

- En BD: `distribuidora.documents` con `document_type_id = 33`, `number = 66615`, `company_id = 3`, `office_id = 1` (mismos filtros que `debug_sync_related_for_document`).
- O vía API: listar `GET /v1/documents.json` con `emissiondaterange` del día UTC de emisión de esa OC y filtrar ítems con `documentType.id = 33` y `number = 66615`.

### 2) A) `details.json` + `relateddetailid` (canal operacional del backfill)

Para cada `detail_id` devuelto en `GET /v1/documents/{document_id}/details.json`:

- Llamar `GET /v1/documents.json?relateddetailid={detail_id}&officeId=1&limit=50&offset=0` (paginar si hace falta).
- Anotar si aparece la factura esperada: `id`, `number`, `documentType.id` (típicamente **6** factura afecta, **1** boleta, **9** NC según política del proyecto).

**Criterio:** si **ninguna** página devuelve la factura para **ningún** `detail_id` de la OC, entonces **`relateddetailid` no cubre este caso** (al menos en el momento de la consulta), y el backfill actual **no puede** poblar `document_related` por diseño.

### 3) B) `references.json` (canal tributario / documento en sync principal)

- `GET /v1/documents/{document_id}/references.json`
- Comparar `items` (o estructura equivalente) con la factura conocida (por `document.id` / `number` / `documentType.id`).

En el sync de documentos, las referencias se persisten con `replace_document_references` cuando `document_type_id` está en `(1, 6, 9, 33)` — ver `backend/services/distribuidora/sync_service.py` (bloque que hace `client.get(.../references.json)`).

**Criterio:** si aquí **sí** aparece la factura y en `relateddetailid` **no**, la relación que ve el negocio está modelada por Bsale a nivel **documento / referencia**, no a nivel **línea OC → documento vía `relateddetailid`**.

---

## Semántica detectada (dos canales distintos)

| Canal | Endpoint principal | Tabla típica en este repo | Semántica habitual |
|--------|---------------------|----------------------------|---------------------|
| **A — Operacional por línea** | `documents.json?relateddetailid=` + `details.json` | `distribuidora.document_related` | Despacho / facturación **por línea de OC**: qué documentos “consumen” o referencian **esa línea** (`detail_id`). Es lo que el backfill mayo 2026 materializa. |
| **B — Referencias de documento** | `documents/{id}/references.json` | `distribuidora.document_references` | Vínculos **de documento a documento** (tributarios, XML/DTE, notas de referencia, etc.). **No** es la fuente de `document_related` en el diseño actual. |

Bsale puede mostrar en la UI que “esta OC está facturada” usando información que **solo** viaja por **referencias** o por reglas internas, mientras que el índice invertido **`relateddetailid`** (por línea) **no** devuelve filas para esa factura. Eso no implica bug de FK ni de backfill: es **desalineación semántica** entre lo que el usuario interpreta como “relación” y lo que el endpoint `relateddetailid` indexa.

---

## Conclusión técnica (pendiente de su JSON en vivo)

Hasta pegar los resultados reales de los pasos anteriores, la conclusión operativa es:

1. **`relateddetailid` cubre este caso solo si** la API devuelve la factura en al menos una respuesta `documents.json?relateddetailid=<detail_id>` para algún `detail_id` de la OC 66615. Si no devuelve nada relevante, **no cubre** el caso para propósitos de `document_related`.
2. **`references.json` puede cubrir el caso** (desde el punto de vista de “¿existe un vínculo documento↔documento en Bsale?”) **aunque** `relateddetailid` esté vacío. Eso explicaría factura visible y backfill related “silencioso”.
3. **Recomendación operacional**
   - Para **trazabilidad operacional OC↔venta por línea**, seguir usando / exigiendo datos en **`relateddetailid`** y `document_related` (como hoy).
   - Para **“¿está referenciada la factura en la OC?”** a nivel documento, consultar **`references.json`** y la tabla **`document_references`** (sync de documentos), o una vista/reporte que una ambas fuentes **sin** mezclar semánticas en la misma tabla sin criterio explícito.
   - Si la evidencia en vivo confirma factura solo en **B**, el siguiente paso de producto sería definir si se necesita un **indicador de negocio** (no solo `document_related`) — **fuera del alcance de este informe** (solo investigación).

---

## Comandos útiles (reproducir en su entorno)

Con token y BD configurados:

```bash
python -m backend.debug.debug_sync_related_oc 66615
```

Eso ejecuta el mismo flujo `details.json` + `relateddetailid` que el backfill, para esa OC, y devuelve JSON con contadores y filas `document_related` tras la corrida.

Para inspección solo lectura multi-endpoint (referencias + related), el patrón está en `backend/debug/debug_document_types.py` (secciones `references.json` y `relateddetailid`).

---

## Hallazgos consolidados

| Pregunta | Estado en este documento |
|----------|---------------------------|
| ¿`relateddetailid` devuelve la factura para la OC 66615? | **No verificado aquí** (sin token). Ver sección metodología §2. |
| ¿`references.json` incluiría la factura? | **No verificado aquí**. Ver §3. |
| ¿El backfill “debería” haberla detectado vía related? | Solo si §2 es afirmativo; el diseño actual **no** usa §3 para `document_related`. |

Cuando disponga de los JSON, puede anexar al final de este archivo dos bloques: `RELATEDDETAILID_FULL` y el listado compacto de `references.json`, y cerrar el caso con “A sí / A no / B sí / B no”.
