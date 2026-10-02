# BSALE_RAW — Verificación en vivo (fase 2, READ-ONLY)

**Estado: PENDIENTE DE EJECUCIÓN.** Los tokens (`BSALE_TOKEN_Mini`, `BSALE_TOKEN_Romero`, `BSALE_TOKEN_SPA`) no están disponibles en el entorno de desarrollo local. **No se ha realizado ninguna request a Bsale** (0 requests por empresa).

Herramienta preparada: `backend/debug/bsale_raw_live_probe.py`.

- Sólo GET; hosts permitidos `api.bsale.io` y `credential.bsale.io`.
- ≥ 1,05 s entre requests; empresas en secuencia (≤ 1 req/s global).
- Tope de 60 requests por empresa; sin reintentos; aborta la empresa ante un 429.
- Nunca imprime tokens (la ruta de `credential.bsale.io` se registra como `/v1/instances/basic/<TOKEN>.json`; errores de red sólo por tipo).
- La salida JSON contiene sólo ids, conteos, nombres de claves y metadatos (sin RUT, nombres, emails ni direcciones de clientes).

Ejecución (en una terminal con los tokens en el entorno, nunca como argumento ni en un `.env` dentro del repo, que hoy **no** está en `.gitignore`):

```powershell
$env:BSALE_TOKEN_Mini   = "<token>"
$env:BSALE_TOKEN_Romero = "<token>"
$env:BSALE_TOKEN_SPA    = "<token>"
python -m backend.debug.bsale_raw_live_probe --out "$env:TEMP\bsale_raw_probe.json"
```

Requests estimadas: ~30 por empresa 1 y 2, ~45 empresa 3 (incluye documentos).

---

## Plan de pruebas y estado

| # | Prueba | Empresa | Endpoint sanitizado | DOCUMENTED | OBSERVED IN LIVE API | Conclusión |
|---|---|---|---|---|---|---|
| 1 | Instancia / cpnId | 1, 2, 3 | `credential.bsale.io/v1/instances/basic/<TOKEN>.json` | Devuelve `id`, `code`, `name`, `state`, `country` | PENDIENTE | UNRESOLVED |
| 2 | Headers de rate limit | 1, 2, 3 | todas las respuestas | Límite 3.000 req / 300 s (FAQ). Headers no documentados | PENDIENTE | UNRESOLVED |
| 3 | `state` en productos | 1, 2, 3 | `/v1/products.json?state=0\|1&limit=1`, sin `state` | `state` 0 activo / 1 inactivo | PENDIENTE | UNRESOLVED |
| 4 | `state` en variantes + 10203 (emp. 1), 31300/31301 (emp. 3) | 1, 2, 3 | `/v1/variants.json?...`, `/v1/variants/{id}.json` | ídem | PENDIENTE | UNRESOLVED |
| 5 | Stock: count y filtros `variantid` / `officeid` | 1, 2, 3 | `/v1/stocks.json?limit=1`, `?variantid=`, `?officeid=` | Filtros `officeid`, `variantid`, `code`, `barcode` | PENDIENTE | UNRESOLVED |
| 6 | Detalles de lista de precio + consulta puntual | 1, 2, 3 | `/v1/price_lists/{id}/details.json?limit=2`, `?variantid=` | `id`, `variantValue`, `variantValueWithTaxes`, `variant` | PENDIENTE | UNRESOLVED |
| 7 | Costos / history | 1, 2, 3 (≤ 3 variantes) | `/v1/variants/{id}/costs.json` | `averageCost`, `history[]` (sin paginación documentada) | PENDIENTE | UNRESOLVED |
| 8 | `generationdaterange` en `/documents.json` | 3 | `/v1/documents.json?generationdaterange=[t0,t1]` vs sin filtro | Sólo documentado en `/documents/summary.json` | PENDIENTE | UNRESOLVED |
| 9 | OC tipo 33 recientes | 3 | `/v1/documents.json?documenttypeid=33&emissiondaterange=[...]` | Filtros documentados | PENDIENTE | UNRESOLVED |
| 10 | `expand=[details]` vs `/details.json` paginado | 3 | `/v1/documents/{id}.json?expand=[details]` | `expand` documentado; límite de la colección no | PENDIENTE | UNRESOLVED |
| 11 | Clientes: paginación, `state`, campos de fecha | 1, 2, 3 | `/v1/clients.json?state=0\|1&limit=1` | Sin filtro de fecha documentado | PENDIENTE | UNRESOLVED |
| 12 | Rutas de webhook | 1, 2, 3 | `/v2/products/{id}.json`, `/v2/variants/{id}.json`, `/v2/stocks.json?variant=&office=`, `/v2/price_lists/{pl}/details.json?variant=`, `/documents/{id}.json` | Ejemplos de `resource` en la doc de webhooks | PENDIENTE | UNRESOLVED |

Criterios de veredicto ya codificados en el script:

- **P8:** `REJECTED` si HTTP ≥ 400; `ACCEPTED_BUT_IGNORED` si el count filtrado = count sin filtro; `SUPPORTED_AND_FILTERS` sólo si el count baja **y** todos los `generationDate` devueltos caen en la ventana; si no, `INCONCLUSIVE`.
- **P10:** compara `len(details.items)` del expand con el total paginado de `/details.json`; con ≤ 25 líneas el resultado es `INCONCLUSIVE` porque no prueba truncamiento.

---

## INFERRED

Nada se infiere todavía: sin datos en vivo.

## Cambios de diseño aplicados en esta fase (sin API)

- Consumidor de webhooks: consulta **exactamente** la ruta `resource` entregada por Bsale (no reescribe la versión). Patrones aceptados por topic: `/v2/...` para product/variant/price/stock y `/documents/{id}.json` para document. Anti-SSRF: sólo rutas relativas esperadas, coherentes con `resourceId`/`officeId`/`priceListId`, sobre `https://api.bsale.io`. Ver `backend/services/bsale_raw/webhooks/__init__.py` y tests.
