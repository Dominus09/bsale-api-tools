# BSALE_RAW — Matriz de endpoints (fuente: documentación oficial Bsale Chile)

Fuente: <https://docs.bsale.dev/first-steps/> y páginas Chile enlazadas (`/productos-y-servicios`, `/variantes`, `/stocks`, `/listas-de-precio`, `/documentos`, `/documentos/webhooks`, `/productos-y-servicios/webhooks`, `/clientes`, `/sucursales`, `/impuestos`, `/tipos-de-documentos`, `/tipos-de-productos-y-servicios`, `/webhooks`, `/configuracion/webhooks`, `/FAQ`). Consultadas el 2026-10-01.

**Regla:** todo lo que no está explícito en la documentación se marca `NEEDS_LIVE_VERIFICATION` (abreviado **NLV**). No se programa ningún endpoint que no esté en esta matriz.

**Fase 2 aprobada (2026-10-02):** las NLV verificadas se marcan **OBSERVED** o **REJECTED**; el detalle está en `BSALE_RAW_LIVE_VERIFICATION.md`. Datos clave:

- **cpnId:** C1 = 96674, C2 = 5807, C3 = 21884.
- **Stock count:** C1 12.587, C2 1.264, C3 35.160.
- **Documentos C3:** 3.880.542, por lo que el full scan global está prohibido.
- **`generationdaterange` en `/documents.json`:** 403 (REJECTED).

### Prioridad central de requests (por empresa / token)

Todos los consumidores de una empresa comparten **un único** `PriorityRateLimiter` (`core/rate_limit.py`). Cada token liberado se asigna al waiter de mayor prioridad (FIFO dentro del mismo nivel).

| Prioridad | Uso |
|---|---|
| P0 | webhook / refresh puntual (targeted) |
| P1 | OC 33 (documentos, detalles, refs, sellers) |
| P2 | stock (escáner y reconcile) |
| P3 | precios |
| P4 | catálogo (productos, variantes) |
| P5 | costos, recepciones, consumos |
| P6 | clientes y configuración |

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
| Rate limit | FAQ: `429 Too Many Requests` al exceder **3.000 requests × 300 segundos** (~10 req/s). Alcance (token / instancia / IP): **NLV** (sigue abierto). Headers de cuota: **OBSERVED: no aparecen** en respuestas normales → limiter local conservador + manejo de 429 / `Retry-After`. |
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

  Rutas fuera de patrón se rechazan (`FAILED_FINAL`) hasta documentarlas (`backend/services/bsale_raw/webhooks`).
- **OBSERVED:** las rutas `/v2` responden con el mismo token, pero con envelope **`code` + `data`** (≠ forma V1); un GET V2 de product devolvió **503 transitorio**.
- **Flujo adoptado:**
  1. GET exacto de `resource` (P0);
  2. guardar la respuesta original como evidencia (`webhook_resource_responses`);
  3. refresh **canónico V1** del mismo recurso (`/v1/products/{id}.json`, `/v1/variants/{id}.json`, `/v1/price_lists/{pl}/details.json?variantid=`, `/v1/stocks.json?variantid=&officeid=`, `/v1/documents/{id}.json`).

  Las tablas operativas RAW sólo reciben forma V1. Un 5xx en el GET exacto no bloquea el refresh V1.
- Documentado: los webhooks de stock "representan movimientos de entradas y salidas de stock, ya sea por recepción de productos, tomas de inventario, consumos o despachos".
- Documentado: en el webhook de documento "podrán obtener el objeto document, pero con los datos de stock disponible de cada variante incluida". **OBSERVED:** el documento V1 no trae stock directo (sólo links a details / sellers / references) → stock puntual por variante.
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
| Filtros OBSERVED | `variantid`, `officeid` y combinados **funcionan** |
| Escáner frecuente | Particionado por **empresa + sucursal**; **NO destructivo** (sólo UPSERT); checkpoint en `sync_cursors`; frescura en `sync_state` con scope `office:<id>` |
| Reconcile destructivo | Separado y menos frecuente, por empresa + sucursal: snapshot completo de la sucursal en staging → fusible de % → elimina las filas de esa sucursal no vistas (patrón de `snapshot_reconcile.py`). Es el **único** modo que borra |
| Prioridad | Webhook / targeted (P0) y OC 33 → stock puntual (P0) siempre adelantan al escáner (P2) |
| Rate limit | 1 request por página de 50. **OBSERVED:** C1 12.587 (~252 pág.), C2 1.264 (~26), C3 35.160 (~704) |
| Frecuencia propuesta | Webhook inmediato; escáner continuo ≤ 15 min por empresa (C3 ≈ 704 req ≈ 2,5 min a 5 rps); reconcile destructivo cada 6 h |
| SLA frescura | 15 min por empresa + sucursal (peor caso sin webhook); objetivo < 1 min con webhook |
| Tabla RAW | `bsale_raw.stocks` (estado ACTUAL, sin historia por poll) |
| Riesgos / NLV | ¿filas con `quantity=0` para todo par variante×sucursal o sólo con movimiento?; ¿incluye variantes inactivas?; packs = sólo stock físico (documentado) |

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
| Volumen OBSERVED | C3 sin filtros: **3.880.542** documentos → **full scan global PROHIBIDO** (`full_scan_global_allowed=False`) |
| Incremental | `emissiondaterange` + `documenttypeid` + `officeid` acotado, con solape. **OBSERVED** C3: `documenttypeid=33` + `emissiondaterange` → 32 docs en la ventana de prueba |
| `generationdaterange` | **REJECTED (HTTP 403)** en `/v1/documents.json`. Prohibido en bsale_raw (`FORBIDDEN_DOCUMENT_FILTERS`). **Riesgo:** `distribuidora/sync_service.py` (`sync_bsale_distribuidora_incremental`, modo dual) lo usa; no se modifica sin auditar su fallback |
| Full reconcile | Re-barrido por `emissiondaterange` en ventanas (45 días OC 33, 7 días resto) + `count.json` por ventana. Nunca sin ventana |
| Rate limit | 1 request por página + hijos (details/references/sellers) por documento. **`expand=[details]` INCONCLUSIVE** → no depender de expand |
| Frecuencia propuesta | Webhook inmediato; incremental cada 2 min (empresa 3 / tipo 33, ventana 2 días, solape 10 min); resto de tipos cada 15 min; reconcile nocturno 45 días |
| SLA frescura | 5 min para OC 33 empresa 3; 30 min para el resto |
| Tabla RAW | `bsale_raw.documents` |
| Riesgos / NLV | ¿Bsale envía webhook al anular o modificar (PUT)? No documentado → por eso el reconcile es obligatorio; `emissionDate` sin zona horaria (ventanas deben solaparse ±1 día); OC modificadas después de emitidas no se detectan por `emissiondaterange` si su emisión es antigua |

**Flujo OC 33 (OBSERVED: el documento trae links a details / sellers / references, pero NO stock directo):**

1. webhook `document` (o incremental) → `/v1/documents/{id}.json`;
2. `/v1/documents/{id}/details.json` **paginado completo** (fuente de integridad);
3. variantes distintas de los detalles;
4. `/v1/stocks.json?variantid=X&officeid=Y` puntual (P0) para cada variante en la sucursal del documento.

### 1.3 Detalles de documento — CRÍTICO

| Campo | Valor |
|---|---|
| Endpoint | `GET /v1/documents/{id}/details.json`; `GET /v1/documents/{id}/details/{detailId}.json` |
| Clave Bsale | `id` del detalle (+ `document_id` padre como columna) |
| Relaciones | `variant{id, description, code}` |
| Campos | `id`, `lineNumber`, `quantity`, `netUnitValue`, `totalUnitValue`, `netAmount`, `taxAmount`, `totalAmount`, `netDiscount`, `totalDiscount`, `variant`, `note`, `relatedDetailId` |
| Paginación | Sí (`count/limit/offset`, default 25) — paginar siempre. Es la **fuente de integridad** (no `expand`) |
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
| Full reconcile | **1 barrido sin `state`** (OBSERVED: devuelve activos + inactivos); se guarda el `state` de cada ítem. `state=0` / `state=1` sólo para auditoría |
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
| Full reconcile | 1 barrido sin `state` (OBSERVED: activos + inactivos); `state=0/1` sólo auditoría |
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
| Respuesta | `averageCost` (string con decimal), `history[]` con `reception_detail{id}`, `admissionDate`, `cost`, `availableFifo`. **OBSERVED:** `averageCost`, `totalCost`, `history` |
| Paginación | **OBSERVED:** `history` sin metadata de paginación → se guarda el JSON completo pero **NO se declara histórico completo** |
| Webhook | No existe webhook de costo |
| Estrategia eficiente | El costo cambia por **recepciones** de stock. En vez de pedir costos de todas las variantes: (1) costo inmediato para variantes nuevas (webhook `variant` post); (2) leer recepciones nuevas (`/v1/stocks/receptions.json?admissiondate=`) y sus detalles → refrescar costos sólo de esas variantes; (3) refresco selectivo bajo demanda; (4) **escáner continuo de baja prioridad (P5)** que recorre todas las variantes con checkpoint en `sync_cursors`, SLA ≈ 2 h |
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
| Incremental | No hay filtro de fecha → full reconcile (1 barrido sin `state`, OBSERVED: activos + inactivos); refresco puntual cuando un documento trae un `client.id` desconocido |
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

Todos: sin webhook, sin filtro de fecha → full reconcile cada 6 h, SLA 6 h. Barrido sin `state`; si incluye inactivos en estos recursos sigue **NLV** (sólo se verificó en products / variants / clients) → la auditoría `state=1` se ejecuta en el primer reconcile.

### 1.15 Recursos documentados fuera del alcance de fase 1

Usuarios (`/v1/users.json`), devoluciones (`/v1/returns.json`, filtro `returndate`, `officeid`), pagos, formas de pago, monedas, descuentos, tipos de despacho, atributos dinámicos, documentos de terceros. Se añadirán al registry sólo cuando exista un consumidor en el ERP (las devoluciones ya tienen sync propio, ver índice).

---

## 2. Propuesta de frecuencias y presupuesto de requests

Presupuesto por empresa: **5 req/s** (50 % del límite documentado de 3.000/300 s) para dejar margen a los syncs legacy que comparten token. Configurable por `BSALE_RAW_RPS_<company_id>`.

| Recurso | Prioridad | Webhook | Incremental / escáner | Full reconcile | SLA |
|---|---|---|---|---|---|
| stocks | P2 (P0 si targeted) | inmediato → exacto + V1 puntual | escáner continuo NO destructivo por empresa + sucursal (ciclo ≤ 15 min) | destructivo por sucursal cada 6 h | 15 min por sucursal |
| documents (emp. 3, tipo 33) | P1 | inmediato | cada 2 min, ventana 2 días, solape 10 min | por ventana 45 días, nocturno | 5 min |
| documents (resto) | P1 | inmediato | cada 15 min | por ventana 7 días, nocturno | 30 min |
| variant_prices | P3 | inmediato | — | 2 h | 2 h |
| products / variants | P4 | inmediato | — | 2 h (1 barrido sin `state`) | 2 h |
| variant_costs | P5 | (variant post → P0) | recepciones nuevas + escáner continuo | — (el escáner cubre todo en ≈ 2 h) | ≈ 2 h |
| receptions / consumptions | P5 | — | cada 30 min (día actual + anterior) | por ventana 7 días, nocturno | 2 h |
| clients | P6 | — | punto por documento | 6 h | 6 h |
| offices, taxes, document_types, product_types, price_lists | P6 | — | — | 6 h | 6 h |

**Prohibido:** full scan global de documentos (3,88 M en C3) y `generationdaterange` en `/documents.json`.

---

## 3. Estado de las dudas NEEDS_LIVE_VERIFICATION (tras fase 2)

| # | Duda | Estado |
|---|---|---|
| 1 | Rate limit: headers / alcance | Headers: **OBSERVED ausentes**. Alcance: **abierto** |
| 2 | `cpnId` por empresa | **OBSERVED**: 96674 / 5807 / 21884 |
| 3 | Listados sin `state` | **OBSERVED** activos + inactivos en products, variants, clients. Configuración: **abierto** |
| 4 | Stock: volumen y filtros | **OBSERVED** 12.587 / 1.264 / 35.160; filtros `variantid` / `officeid` funcionan. `quantity=0` e inactivas: **abierto** |
| 5 | Webhooks `/v2` | **OBSERVED** responden; envelope `code` + `data` (≠ V1); 503 transitorio → exacto + V1 canónico |
| 6 | Webhook al anular / modificar documento | **abierto** (reconcile por ventana obligatorio) |
| 7 | `generationdaterange` en `/documents.json` | **REJECTED (403)**; riesgo legacy registrado |
| 8 | `expand=[details]` | **INCONCLUSIVE** → details paginado |
| 9 | Costos | **OBSERVED** `averageCost`, `totalCost`, `history` sin paginación. Inactivas / truncado: **abierto** |
| 10 | Recepciones / consumos rango de fecha | **abierto** (se itera por día con solape) |
| 11 | `commercialState` | **abierto** (se guarda en `payload` si existe) |
| 12 | Stock en documento | **OBSERVED**: sin stock directo, sólo links → stock puntual por variante |
