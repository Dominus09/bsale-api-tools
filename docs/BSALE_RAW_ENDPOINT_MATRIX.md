# BSALE_RAW — Matriz de endpoints (fuente: documentación oficial Bsale Chile)

Fuente: <https://docs.bsale.dev/first-steps/> y páginas Chile enlazadas (`/productos-y-servicios`, `/variantes`, `/stocks`, `/listas-de-precio`, `/documentos`, `/documentos/webhooks`, `/productos-y-servicios/webhooks`, `/clientes`, `/sucursales`, `/impuestos`, `/tipos-de-documentos`, `/tipos-de-productos-y-servicios`, `/webhooks`, `/configuracion/webhooks`, `/FAQ`). Consultadas el 2026-10-01.

**Regla:** todo lo que no está explícito en la documentación se marca `NEEDS_LIVE_VERIFICATION` (abreviado **NLV**). No se programa ningún endpoint que no esté en esta matriz.

---

## 0. Hechos transversales (documentados)

| Tema | Documentación oficial |
|---|---|
| Base URL | `https://api.bsale.io/v1`; header `access_token`. Sólo SSL. |
| Paginación | `limit` (default 25, **máx 50**) + `offset` (default 0). Respuesta con `href`, `count`, `limit`, `offset`, `items`, `next`. |
| Proyección | `fields=[a,b]`; `expand=[rel1,rel2]` en listados e ítems. |
| Fechas | Enteros Unix. `emissionDate`/`expirationDate`: "no se debe aplicar zona horaria, solo considerar la fecha". `generationDate`: fecha y hora. |
| Relaciones | Nodos `{"href": ..., "id": "12"}`; el `id` llega como **string**. |
| `state` | En todos los recursos con estado: **0 = activo, 1 = inactivo** (productos, variantes, listas de precio, clientes, sucursales, impuestos, tipos, documentos, usuarios). |
| Borrado | Productos y variantes **no se borran**, se desactivan (`state=1`) y se notifica con webhook `PUT`. |
| Rate limit | FAQ: `429 Too Many Requests` al exceder **3.000 requests × 300 segundos** (~10 req/s). Alcance (token / instancia / IP): **NLV**. Headers de rate limit: no documentados (**NLV**). |
| Errores | 400, 401 (token), 402 (instancia bloqueada por no pago), 403, 404, 405, 429, 500, 502. La FAQ indica que un 500 "The requested resource is not available" puede deberse al rate limit. |
| Instancia | `GET https://credential.bsale.io/v1/instances/basic/{access_token}.json` → `id` (= `cpnId` de los webhooks), `code` (RUT), `name`, `state`, `country`. **Host distinto** a `api.bsale.io`: requiere allow-list explícita. |

### Webhooks (documentados)

- Activación por correo a `ayuda@bsale.app` con la URL (SSL). Payload JSON: `cpnId`, `resource`, `resourceId`, `topic`, `action`, `send` (unix).
- Topics CL: `product` (post/put), `variant` (post/put), `price` (sólo put, + `priceListId`), `stock` (sólo put, + `officeId`), `document` (post, + `officeId`).
- Rutas `resource` en los ejemplos oficiales. No todos los topics usan `/v2`:

  | topic | ejemplo oficial de `resource` |
  |---|---|
  | product | `/v2/products/{id}.json` |
  | variant | `/v2/variants/{id}.json` |
  | price | `/v2/price_lists/{pl}/details.json?variant={id}` |
  | stock | `/v2/stocks.json?variant={id}&office={office}` |
  | document | `/documents/{id}.json` (sin versión) |

- El consumidor consulta **exactamente** la ruta `resource` entregada, sin reescribir su versión, con estas protecciones anti-SSRF:
  - solo acepta rutas relativas que calcen con el patrón documentado del topic;
  - exige coherencia con `resourceId`, `officeId` y `priceListId`;
  - solo usa el host `https://api.bsale.io`;
  - usa el token de la empresa resuelta por `cpnId`.

  Rutas fuera de patrón se rechazan (`FAILED_FINAL`) hasta documentarlas (`backend/services/bsale_raw/webhooks`). Si las rutas `/v2` o sin versión responden con el mismo token y formato es **NLV**.
- Documentado: los webhooks de stock "representan movimientos de entradas y salidas de stock, ya sea por recepción de productos, tomas de inventario, consumos o despachos".
- Documentado: en el webhook de documento "podrán obtener el objeto document, pero con los datos de stock disponible de cada variante incluida". Cómo se expone ese stock en la respuesta es **NLV**.
- No se documentan: firma o secreto del webhook, reintentos de entrega, orden de entrega ni exactly-once. Se asume **at-least-once, sin orden** y **sin autenticación propia** → validación por `cpnId` conocido + reconsulta a la API (nunca confiar en el contenido del webhook como dato).

---

## 1. Matriz por recurso

Las frecuencias son la **propuesta** de la sección 2. "Tabla RAW" refiere al modelo conceptual en `BSALE_RAW_ARCHITECTURE.md`.

### 1.1 Stocks — CRÍTICO

| Campo | Valor |
|---|---|
| Endpoint | `GET /v1/stocks.json`; `GET /v1/stocks/{id}.json` |
| Clave Bsale | `id` del registro de stock; clave operativa `(variant.id, office.id)` |
| Relaciones | `variant{id}`, `office{id}` |
| Campos importantes | `id`, `quantity`, `quantityReserved`, `quantityAvailable`, `variant.id`, `office.id` |
| Paginación | limit ≤ 50 / offset |
| Filtros | `officeid`, `variantid`, `code` (SKU), `barcode`, `fields`, `expand` |
| Filtro state | No documentado |
| Webhook | `stock` (put, `resourceId` = variante, `officeId`) y `document` (post) |
| Incremental | **No hay filtro por fecha de modificación documentado.** El incremental real es: webhook → refresco puntual `variantid`+`officeid`; documento nuevo → refresco de sus variantes |
| Full reconcile | Barrido por sucursal (`officeid`) paginado; snapshot completo por empresa con fusible de % de stale (patrón ya existente en `snapshot_reconcile.py`) |
| Rate limit | 1 request por página de 50. Volumen real por empresa: **NLV** (variantes × sucursales con fila) |
| Frecuencia propuesta | Webhook inmediato; escáner continuo que cubra cada empresa en ≤ 15 min; full reconcile cada 6 h |
| SLA frescura | 15 min (peor caso sin webhook); objetivo < 1 min con webhook |
| Tabla RAW | `bsale_raw.stocks` (estado ACTUAL, sin historia por poll) |
| Riesgos / NLV | ¿filas con `quantity=0` para todo par variante×sucursal o sólo con movimiento?; ¿incluye variantes inactivas?; packs = sólo stock físico (documentado); webhook `/v2/stocks.json?variant=&office=` ↔ `/v1?variantid=&officeid=` |

### 1.2 Documentos — CRÍTICO (prioridad empresa 3 / `document_type_id` 33 = OC de vendedores)

| Campo | Valor |
|---|---|
| Endpoint | `GET /v1/documents.json`; `GET /v1/documents/{id}.json`; `GET /v1/documents/count.json` |
| Clave Bsale | `id` |
| Relaciones | `document_type`, `client`, `office`, `user`, `coin`, `references`, `document_taxes`, `details`, `sellers`, `attributes`, `payments` |
| Campos importantes | `id`, `emissionDate`, `expirationDate`, `generationDate`, `number`, `totalAmount`, `netAmount`, `taxAmount`, `exemptAmount`, `state`, `commercialState` (NLV, no está en la tabla de atributos CL), `informedSii`, `responseMsgSii`, `token`, `urlPdf`, `address`, `municipality`, `city`, `relatedDetailId` |
| Paginación | limit ≤ 50 / offset |
| Filtros documentados | `emissiondate`, `expirationdate`, `emissiondaterange=[desde,hasta]`, `number`, `token`, `documenttypeid`, `clientid`, `clientcode`, `officeid`, `informedsii`, `codesii`, `totalamount`, `referencecode`, `referencenumber`, `rcofdate`, `detailid`, `state` |
| Filtro state | Sí: `state=0` activos, `state=1` inactivos |
| Webhook | `document` — documentado **sólo** `action=post` (creación) + `officeId` |
| Incremental | `emissiondaterange` + `documenttypeid` + `officeid` con solape. **`generationdaterange` sólo está documentado en `/documents/summary.json`, NO en `/documents.json`** (el sync actual de Distribuidora lo usa → NLV crítico) |
| Full reconcile | Re-barrido por `emissiondaterange` en ventanas de días hacia atrás (p. ej. 45 días para OC 33) + `count.json` para comparar totales por ventana |
| Rate limit | 1 request por página + hijos (details/references/sellers) por documento si `expand` no basta |
| Frecuencia propuesta | Webhook inmediato; incremental cada 2 min (empresa 3 / tipo 33, ventana 2 días, solape 10 min); resto de tipos cada 15 min; reconcile nocturno 45 días |
| SLA frescura | 5 min para OC 33 empresa 3; 30 min para el resto |
| Tabla RAW | `bsale_raw.documents` |
| Riesgos / NLV | ¿Bsale envía webhook al anular o modificar (PUT)? No documentado → por eso el reconcile es obligatorio; `emissionDate` sin zona horaria (ventanas deben solaparse ±1 día); OC modificadas después de emitidas no se detectan por `emissiondaterange` si su emisión es antigua; `expand=[details]` ¿pagina a 25? |

**Acción derivada documentada como requisito de negocio:** cuando llega una OC 33 (webhook o incremental), refrescar inmediatamente el stock de sus variantes (`/v1/stocks.json?variantid=X`) porque una OC puede reservar stock (`quantityReserved`).

### 1.3 Detalles de documento — CRÍTICO

| Campo | Valor |
|---|---|
| Endpoint | `GET /v1/documents/{id}/details.json`; `GET /v1/documents/{id}/details/{detailId}.json` |
| Clave Bsale | `id` del detalle (+ `document_id` padre como columna) |
| Relaciones | `variant{id, description, code}` |
| Campos | `id`, `lineNumber`, `quantity`, `netUnitValue`, `totalUnitValue`, `netAmount`, `taxAmount`, `totalAmount`, `netDiscount`, `totalDiscount`, `variant`, `note`, `relatedDetailId` |
| Paginación | Sí (`count/limit/offset`, default 25) — paginar siempre |
| Webhook / incremental | Heredado del documento |
| Full reconcile | Reemplazo del set de detalles del documento en la misma transacción (detalles desaparecidos → borrar en RAW sólo si el documento se leyó completo) |
| Tabla RAW | `bsale_raw.document_details` |
| Riesgos | `variant.id` en el detalle es int en el ejemplo (no string): parsear ambos. Filtro `relateddetailid` usado por Distribuidora: no aparece en la lista de filtros de `documents.json` (aparece `detailid`) → **NLV** |

### 1.4 Referencias de documento — CRÍTICO

| Campo | Valor |
|---|---|
| Endpoint | `GET /v1/documents/{id}/references.json`; `/references/{refId}.json` |
| Campos | `id`, `referenceDate`, `number` (string), `reason`, `dte_code{id}` |
| Particularidad | Documentado: "Retorna sólo referencias electrónicas (XML)" |
| Tabla RAW | `bsale_raw.document_references` |

### 1.5 Vendedores de documento — CRÍTICO

| Campo | Valor |
|---|---|
| Endpoint | `GET /v1/documents/{id}/sellers.json` |
| Campos | Ítems = usuarios: `id`, `firstName`, `lastName` (puede haber >1, p. ej. documento generado desde varias notas de venta) |
| Clave RAW | `(company_id, document_id, user_id)` — el ítem no tiene id propio de relación |
| Tabla RAW | `bsale_raw.document_sellers` |

### 1.6 Productos — ALTO

| Campo | Valor |
|---|---|
| Endpoint | `GET /v1/products.json`; `/v1/products/{id}.json`; `/v1/products/{id}/variants.json`; `/v1/products/{id}/product_taxes.json`; `/v1/products/count.json` |
| Clave | `id` |
| Relaciones | `product_type{id}`, `product_taxes{href}`; variantes vía `/products/{id}/variants.json` o `variants.json?productid=` |
| Campos | `id`, `name`, `description`, `classification`, `ledgerAccount`, `costCenter`, `allowDecimal`, `stockControl`, `printDetailPack`, `state`, `prestashopProductId`, `presashopAttributeId` (sic), `product_type`, `product_taxes` |
| Filtros | `name`, `producttypeid`, `classification`, `state`, `fields`, `expand` |
| Filtro state | Sí (0 activo / 1 inactivo) |
| Webhook | `product` (post/put; desactivación = put) |
| Incremental | No hay filtro por fecha de modificación → webhook + reconcile |
| Full reconcile | `products.json?state=0` + `products.json?state=1` (dos barridos explícitos; el comportamiento del listado **sin** `state` es NLV) |
| Frecuencia | Webhook inmediato; reconcile cada 2 h |
| SLA | 2 h |
| Tabla RAW | `bsale_raw.products` |

### 1.7 Variantes — ALTO

| Campo | Valor |
|---|---|
| Endpoint | `GET /v1/variants.json`; `/v1/variants/{id}.json`; `/v1/variants/count.json`; `/v1/variants/{id}/attribute_values.json`; `/v1/variants/{id}/costs.json` |
| Clave | `id` |
| Relaciones | `product{id}`, `attribute_values`, `costs` |
| Campos | `id`, `description`, `unlimitedStock`, `allowNegativeStock`, `state`, `barCode`, `code`, `imagestionCenterCost`, `imagestionAccount`, `imagestionConceptCod`, `imagestionProyectCod`, `imagestionCategoryCod`, `imagestionProductId`, `serialNumber`, `prestashopCombinationId`, `prestashopValueId`, `product{id}`, `attribute_values{href}`, `costs{href}` |
| Filtros | `barcode`, `code`, `serialnumber`, `productid`, `state`, `fields`, `expand` |
| Webhook | `variant` (post/put) |
| Incremental | Webhook; para hidratar referencias faltantes: `GET /v1/variants/{id}.json` (punto) |
| Full reconcile | `variants.json?state=0` + `variants.json?state=1` |
| Frecuencia / SLA | Webhook inmediato; reconcile cada 2 h / 2 h |
| Tabla RAW | `bsale_raw.variants` |
| Riesgos | SKU y barcode no son únicos ni PK; RAW guarda la relación variante→producto tal cual la entrega Bsale |

### 1.8 Listas de precio (metadata) — BAJO

| Campo | Valor |
|---|---|
| Endpoint | `GET /v1/price_lists.json`; `/v1/price_lists/{id}.json`; `/v1/price_lists/count.json` |
| Campos | `id`, `name`, `description`, `state`, `coin`, `details` |
| Filtros | `name`, `coinid`, `state`, `fields`, `expand` |
| Webhook | No (el webhook `price` es por detalle) |
| Frecuencia / SLA | Cada 6 h / 6 h |
| Tabla RAW | `bsale_raw.price_lists` |

### 1.9 Precios por variante (detalles de lista) — MEDIO

| Campo | Valor |
|---|---|
| Endpoint | `GET /v1/price_lists/{id}/details.json`; `/details/{detailId}.json` |
| Clave | `id` del detalle; clave operativa `(price_list_id, variant_id)` |
| Campos | `id`, `variantValue`, `variantValueWithTaxes`, `variant{id}` |
| Filtros | `variantid`, `code`, `barcode`, `expand` |
| Filtro state / fecha | No documentados |
| Webhook | `price` (put, `resourceId` = variante, `priceListId`) |
| Incremental | Webhook → `details.json?variantid=X` en la lista `priceListId` |
| Full reconcile | Barrido completo de cada lista activa; snapshot con fusible |
| Frecuencia / SLA | Webhook inmediato; reconcile cada 2 h / 2 h |
| Tabla RAW | `bsale_raw.variant_prices` |

### 1.10 Costos por variante — MEDIO

| Campo | Valor |
|---|---|
| Endpoint | `GET /v1/variants/{id}/costs.json` |
| Respuesta | `averageCost` (string con decimal), `history[]` con `reception_detail{id}`, `admissionDate`, `cost`, `availableFifo` |
| Paginación | No documentada para `history` (**NLV**) |
| Webhook | No existe webhook de costo |
| Estrategia eficiente | El costo cambia por **recepciones** de stock. En vez de pedir costos de todas las variantes: (1) costo inmediato para variantes nuevas (webhook `variant` post); (2) leer recepciones nuevas (`/v1/stocks/receptions.json?admissiondate=`) y sus detalles → refrescar costos sólo de esas variantes; (3) refresco selectivo bajo demanda; (4) barrido completo rotativo cada 2 h con presupuesto de requests |
| Rate limit | 1 request por variante: es el recurso más caro. Volumen real: **NLV** |
| SLA | 2 h |
| Tabla RAW | `bsale_raw.variant_costs` (JSON completo incluyendo `history`) |
| Riesgos | ¿responde para variantes inactivas?; ¿`history` completo o truncado?; `averageCost` como string |

### 1.11 Recepciones de stock y detalles — MEDIO

| Campo | Valor |
|---|---|
| Endpoint | `GET /v1/stocks/receptions.json`; `/receptions/{id}.json`; `/receptions/{id}/details.json` |
| Filtros | `admissiondate`, `documentnumber`, `officeid`, `fields`, `expand` |
| Campos detalle | `id`, `quantity`, `cost`, `variantStock`, `serialNumber`, `variant{id}` |
| Incremental | `admissiondate` (día exacto; rango **no documentado** → iterar días con solape) |
| Uso | Disparador de refresco de costos y stock |
| Tablas RAW | `bsale_raw.stock_receptions`, `bsale_raw.stock_reception_details` |

### 1.12 Consumos de stock y detalles — MEDIO

| Campo | Valor |
|---|---|
| Endpoint | `GET /v1/stocks/consumptions.json`; `/consumptions/{id}.json`; `/consumptions/{id}/details.json`; tipos `GET /v1/stock_consumption_types.json` (`cntId`, `cntI18nName`, `cntActive`, `cntCode`) |
| Filtros | `consumptiondate`, `officeid` |
| Incremental | `consumptiondate` día exacto con solape |
| Tablas RAW | `bsale_raw.stock_consumptions`, `bsale_raw.stock_consumption_details` (tipos de consumo: tabla extra opcional, ver arquitectura) |

### 1.13 Clientes — ALTO

| Campo | Valor |
|---|---|
| Endpoint | `GET /v1/clients.json`; `/v1/clients/{id}.json`; `/clients/{id}/contacts.json`; `/clients/{id}/addresses.json`; `/clients/{id}/attributes.json`; `/clients/count.json` |
| Filtros | `code` (RUT), `firstname`, `lastname`, `email`, `paymenttypeid`, `state` |
| Webhook | **No documentado en CL** |
| Incremental | No hay filtro de fecha → full reconcile; refresco puntual cuando un documento trae un `client.id` desconocido |
| Frecuencia / SLA | Reconcile cada 6 h; punto inmediato por documento / 6 h |
| Tabla RAW | `bsale_raw.clients` |
| Riesgos | Coordenadas viven hoy en un campo libre (`facebook`, ver `sync_clients.py`): RAW lo guarda tal cual, la interpretación queda en `bsale` |

### 1.14 Sucursales, impuestos, tipos de documento, tipos de producto — BAJO

| Recurso | Endpoint | Filtros documentados | Tabla RAW |
|---|---|---|---|
| Sucursales | `/v1/offices.json`, `/offices/{id}.json` | `name`, `address`, `country`, `city`, `municipality`, `costcenter`, `state` | `bsale_raw.offices` |
| Impuestos | `/v1/taxes.json`, `/taxes/{id}.json` | `name`, `code`, `ledgeraccount`, `state` | `bsale_raw.taxes` |
| Tipos de documento | `/v1/document_types.json`, `/document_types/{id}.json` | `name`, `codesii`, `ledgeraccount`, `iselectronicdocument`, `state`; expand `book_type` | `bsale_raw.document_types` |
| Tipos de producto | `/v1/product_types.json`, `/product_types/{id}.json`, `/product_types/{id}/attributes.json` | `name`, `state` | `bsale_raw.product_types` |

Todos: sin webhook, sin filtro de fecha → full reconcile cada 6 h, SLA 6 h. Barrido explícito `state=0` y `state=1`.

### 1.15 Recursos documentados fuera del alcance de fase 1

Usuarios (`/v1/users.json`), devoluciones (`/v1/returns.json`, filtro `returndate`, `officeid`), pagos, formas de pago, monedas, descuentos, tipos de despacho, atributos dinámicos, documentos de terceros. Se añadirán al registry sólo cuando exista un consumidor en el ERP (las devoluciones ya tienen sync propio, ver índice).

---

## 2. Propuesta de frecuencias y presupuesto de requests

Presupuesto por empresa: **5 req/s** (50 % del límite documentado de 3.000/300 s) para dejar margen a los syncs legacy que comparten token. Configurable por `BSALE_RAW_RPS_<company_id>`.

| Recurso | Webhook | Incremental / escáner | Full reconcile | SLA |
|---|---|---|---|---|
| stocks | inmediato | escáner continuo (ciclo ≤ 15 min por empresa) | 6 h | 15 min |
| documents (emp. 3, tipo 33) | inmediato | cada 2 min, ventana 2 días, solape 10 min | nocturno 45 días | 5 min |
| documents (resto) | inmediato | cada 15 min | nocturno 7 días | 30 min |
| products / variants | inmediato | — | 2 h | 2 h |
| variant_prices | inmediato | — | 2 h | 2 h |
| variant_costs | (variant post) | recepciones nuevas → variantes afectadas | rotativo 2 h | 2 h |
| clients | — | punto por documento | 6 h | 6 h |
| receptions / consumptions | — | cada 30 min (día actual + anterior) | nocturno 7 días | 2 h |
| offices, taxes, document_types, product_types, price_lists | — | — | 6 h | 6 h |

Los intervalos se confirman tras medir volúmenes reales (sección 3).

---

## 3. Dudas que requieren prueba en vivo (NEEDS_LIVE_VERIFICATION) contra las 3 APIs

Ejecutar con un script de sólo lectura, autorizado explícitamente, empresa por empresa:

1. **Rate limit:** alcance del límite 3.000/300 s (¿por token, instancia o IP?) y si la API devuelve headers de rate limit / `Retry-After`.
2. **`cpnId` por empresa:** obtener `id` de instancia de cada token vía `credential.bsale.io` (necesario para mapear webhooks → `company_id`).
3. **Listados sin `state`:** ¿`products.json`, `variants.json`, `offices.json`, etc. devuelven activos e inactivos, o sólo activos? (Caso real: variantes 10203 emp. 1, 31300/31301 emp. 3 referenciadas por stock y ausentes localmente.)
4. **Stock:** volumen de filas por empresa; ¿filas con cantidad 0?; ¿filas para variantes inactivas?; ¿`variantid`+`officeid` devuelve exactamente 1 fila?
5. **Webhooks `/v2`:** ¿`/v2/stocks.json?variant=&office=` y `/v2/price_lists/{pl}/details.json?variant=` responden con el mismo token? ¿Mismo formato que `/v1`?
6. **Documento webhook:** ¿se recibe algo al anular (`state=1`) o modificar un documento?
7. **`generationdaterange` en `/documents.json`:** ¿funciona aunque no esté documentado? (lo usa el sync Distribuidora actual).
8. **`expand=[details]`** en listados de documentos: ¿trae todos los detalles o se trunca a 25?
9. **Costos:** ¿`history` completo?; ¿responde para variantes inactivas?; tiempo de respuesta medio.
10. **Recepciones / consumos:** ¿existe filtro de rango de fecha no documentado?
11. **`commercialState`** en documentos CL: ¿existe en el JSON?
12. **Stock en webhook de documento:** dónde y cómo aparece el "stock disponible de cada variante incluida".
