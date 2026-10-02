# BSALE_RAW — Verificación en vivo (fase 2, READ-ONLY)

**Estado: FASE 2 APROBADA (2026-10-02).**

Las pruebas las ejecutó el operador con `backend/debug/bsale_raw_live_probe.py`:
- sólo GET;
- ≤ 1 req/s por empresa, sin reintentos y abortando ante un 429;
- sin imprimir tokens.

Las conclusiones OBSERVED de este documento son las informadas por el operador al aprobar la fase. El JSON sanitizado de la corrida **no está disponible en el entorno de desarrollo**; si se requiere evidencia cruda, adjuntarlo fuera del repo.

Nada de lo observado modifica todavía los syncs legacy.

Leyenda:

- **DOCUMENTED**: dice la documentación oficial.
- **OBSERVED IN LIVE API**: visto en las respuestas reales.
- **INFERRED**: deducción de diseño a partir de lo observado.
- **UNRESOLVED**: sigue abierto.

---

## Resultados por prueba

| # | Prueba | Empresa | Endpoint sanitizado | HTTP | Count | DOCUMENTED | OBSERVED IN LIVE API | Conclusión |
|---|---|---|---|---|---|---|---|---|
| 1 | Instancia / cpnId | 1, 2, 3 | `credential.bsale.io/v1/instances/basic/<TOKEN>.json` | 200 | — | `id`, `code`, `name`, `state`, `country` | cpnId C1=**96674**, C2=**5807**, C3=**21884** (distintos) | Mapa cpnId → company_id confirmado |
| 2 | Headers de rate limit | 1, 2, 3 | todas las respuestas normales | — | — | 3.000 req / 300 s (FAQ); headers no documentados | **NOT_OBSERVED**: sin headers de cuota en respuestas normales | Limiter local conservador + 429/Retry-After. Alcance del límite (token / IP / instancia): no demostrado |
| 3 | `state` en productos | 1, 2, 3 | `/v1/products.json?state=0\|1&limit=1`, sin `state` | 200 | por empresa | `state` 0 activo / 1 inactivo | Sin `state` devuelve **activos + inactivos** | Full scan: 1 consulta sin `state`; `state=0/1` sólo auditoría |
| 4 | `state` en variantes | 1, 2, 3 | `/v1/variants.json?...`, `/v1/variants/{id}.json` | 200 | por empresa | ídem | Sin `state` devuelve activos + inactivos | ídem |
| 5 | Stock count y filtros | 1, 2, 3 | `/v1/stocks.json?limit=1`, `?variantid=`, `?officeid=`, ambos | 200 | C1 **12.587**, C2 **1.264**, C3 **35.160** | Filtros `officeid`, `variantid` | `variantid`, `officeid` y combinados **funcionan** | El count del listado da el volumen sin paginar todo |
| 6 | Precios | 1, 2, 3 | `/v1/price_lists/{id}/details.json?limit=2`, `?variantid=` | 200 | por lista | `id`, `variantValue`, `variantValueWithTaxes`, `variant` | Consulta puntual por `variantid` operativa | Refresh puntual price_list + variant viable |
| 7 | Costos | 1, 2, 3 (pocas variantes) | `/v1/variants/{id}/costs.json` | 200 | — | `averageCost`, `history[]` | `averageCost`, **`totalCost`**, `history`; history **sin metadata de paginación** | Guardar JSON completo; **no** declarar history como histórico completo |
| 8 | `generationdaterange` | 3 | `/v1/documents.json?generationdaterange=[t0,t1]` | **403** | — | Sólo documentado en `/documents/summary.json` | Rechazado | **REJECTED**: no usar en bsale_raw; riesgo en consumidor legacy |
| 9 | OC tipo 33 | 3 | `/v1/documents.json?documenttypeid=33&emissiondaterange=[...]` | 200 | **32** en la ventana | Filtros documentados | Funciona; links `details`/`sellers`/`references`; **sin stock directo** | Flujo: document → details paginados → variantes → stock puntual |
| 10 | `expand=[details]` | 3 | `/v1/documents/{id}.json?expand=[details]` vs `/details.json` | 200 | — | `expand` documentado | No se pudo demostrar completitud | **INCONCLUSIVE**: no depender de expand |
| 11 | Clientes | 1, 2, 3 | `/v1/clients.json?state=0\|1&limit=1`, sin `state` | 200 | por empresa | Sin filtro de fecha | Sin `state` devuelve activos + inactivos | Full scan sin `state`; sin incremental por fecha |
| 12 | Rutas webhook | 1, 2, 3 | `/v2/products/{id}.json`, `/v2/variants/{id}.json`, `/v2/stocks.json?variant=&office=`, `/v2/price_lists/{pl}/details.json?variant=` | 200 / **503** transitorio en un product | — | Ejemplos `resource` en la doc | Respuestas V2 con envelope **`code` + `data`** (no forma V1) | GET exacto → guardar original → refresh canónico V1 |
| — | Conteo global documentos | 3 | `/v1/documents.json?limit=1` | 200 | **3.880.542** | — | — | **PROHIBIDO** full scan global de documentos |

---

## DOCUMENTED (relevante)

- Paginación `limit` ≤ 50 / `offset`; respuestas con `count`, `items`, `next`.
- `state` 0 = activo, 1 = inactivo.
- Rate limit 3.000 requests / 300 s.
- Webhooks: `cpnId`, `resource`, `resourceId`, `topic`, `action`, `send`. `resource` es `/v2/...` en product/variant/price/stock y `/documents/{id}.json` en document.
- `generationdaterange` sólo en `/documents/summary.json`.

## OBSERVED IN LIVE API

1. cpnId: C1 = 96674, C2 = 5807, C3 = 21884 (distintos).
2. products / variants / clients sin `state` → activos + inactivos.
3. Stock count: C1 12.587, C2 1.264, C3 35.160; filtros `variantid`, `officeid` y combinados funcionan.
4. Ningún header de cuota en respuestas normales.
5. `/v1/documents.json?generationdaterange=...` → HTTP 403.
6. Documentos C3 sin filtros: count 3.880.542.
7. OC 33 C3: `documenttypeid=33` + `emissiondaterange` funciona; 32 documentos en la ventana; documento con links details/sellers/references y sin stock directo.
8. `expand=[details]`: no concluyente.
9. Costos: `averageCost`, `totalCost`, `history`; history sin metadata de paginación.
10. V2: envelope `code` + `data`; un GET de product V2 devolvió 503 transitorio.

## INFERRED (decisiones de diseño)

- Full scan de catálogo y clientes = 1 barrido sin `state`; los barridos `state=0/1` quedan como auditoría puntual.
- Stock: volumen manejable por empresa (C3 ≈ 704 páginas de 50) → scanner continuo particionado por company + office, NO destructivo; reconcile destructivo separado.
- Prioridad central por empresa (P0 targeted … P6 clients/config) en un único limiter compartido.
- Documentos: webhook + incremental `emissiondaterange` acotado con overlap + reconcile por ventanas; jamás full scan global.
- Integridad de detalles: siempre `/documents/{id}/details.json` paginado.
- V2: evidencia cruda en una tabla aparte; tablas operativas RAW sólo con forma V1.

## UNRESOLVED

- Alcance del rate limit (token / IP / instancia).
- ¿Llega webhook al anular o modificar documentos (PUT)?
- ¿Stock devuelve filas con quantity = 0 para todo par variante×sucursal? ¿Incluye variantes inactivas?
- ¿Costos responde para variantes inactivas? ¿History truncado? (sin metadata no se puede probar)
- ¿offices / taxes / document_types / product_types / price_lists sin `state` incluyen inactivos? (sólo se verificó en products, variants, clients)
- ¿Unicidad por empresa de los ids de hijos (document_details, reception_details, …)?
- Recepciones / consumos: ¿existe rango de fechas no documentado?
- Fallback del consumidor legacy que usa `generationdaterange` (ver riesgo abajo).

## Riesgo explícito: consumidor legacy con `generationdaterange`

`backend/services/distribuidora/sync_service.py` → `sync_bsale_distribuidora_incremental` (modo pedidos "dual"):

1. primero llama a `_fetch_documents_window(..., date_range_field="emissiondaterange", finalize_log=False)`;
2. después llama a `_fetch_documents_window(..., date_range_field="generationdaterange", finalize_log=True)`.

Si el 403 se reproduce en ese contexto, el segundo tramo no aporta documentos. Además, como ese tramo es el que finaliza el log, podría marcar el run como fallido aunque el tramo por emisión haya funcionado. El efecto real depende del manejo de errores interno, que no se audita aquí.

**No se modifica todavía.** Antes hay que auditar su fallback y revisar los logs de producción.

## Cambios de diseño aplicados en scaffold y docs

- `core/rate_limit.py`: `RequestPriority` P0–P6 y `PriorityRateLimiter` único por empresa (todos los consumidores lo comparten).
- `core/registry.py` y `resources/*`:
  - `request_priority` por recurso;
  - stock `partition_by_office`;
  - documentos `full_scan_global_allowed=False` con `reconcile_window_days=45`;
  - `generationdaterange` prohibido.
- `core/freshness.py`: `scope` (`global`, `office:<id>`, …; convención en `core/registry.py`).
- `webhooks`: tareas `RESOURCE_EXACT` → `CANONICAL_V1` → `DERIVED`; `classify_exact_response` detecta el envelope V2.
