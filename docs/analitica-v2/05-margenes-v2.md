# Analítica V2.2 — Márgenes V2 (implementado localmente)

Estado: backend, pantalla `/analitica-v2/margenes` y tests implementados localmente.
Sin commit, sin deploy, sin migraciones. `/margins` legacy sigue intacto.

## Motor

| Módulo | Responsabilidad |
|---|---|
| `commercial_scope.py` | Listas evaluadas = autorizadas (`COMMERCIAL_PRICE_LISTS`) ∩ vigentes en RAW. Retiradas (`RETIRED_PRICE_LISTS`) nunca se evalúan; si reaparecen vigentes → discrepancia |
| `margin_math.py` | Fórmulas Decimal sin división por cero |
| `margin_engine.py` | Estado por SKU × lista, calificadores, validez y trazabilidad (puro) |
| `promotion_exceptions.py` | Contrato de excepciones con Promociones V2 (fuente hoy "no integrada") |
| `margins_repo.py` | SQL: `bsale_raw.price_lists`, `bsale_raw.variant_prices`, `bsale.margin_rules` |
| `margins_service.py` | Composición con Control de Costos V2, filtros, orden, paginación, caché 120 s |

Costos: se reutiliza `CostControlV2Service.sku_items()` (mismo cálculo que Control de Costos V2);
no hay fórmulas de costo duplicadas.

### Alcance comercial

| company_id | Evaluadas | Retiradas |
|---|---|---|
| 3 | 12 Supermercado, 13 Ruta/Web, 16 Melinka | 14 COMODITI ALCOHOLES |
| 1, 2 | sin configurar (422 al consultar márgenes; además hoy 409 por RAW incompleto) | — |

Estados de lista: `evaluada`, `autorizada_no_vigente` (discrepancia), `retirada`,
`retirada_reactivada` (discrepancia), `vigente_no_autorizada` (informativo), `no_vigente`.

### Cuatro costos separados

| Campo | Origen | Uso |
|---|---|---|
| `costs.bsale_average_net` | `variant_costs.average_cost` | Informativo |
| `costs.last_reception_net` | Última compra comparable (factura/guía) | Informativo |
| `costs.reference` | Base elegida explícitamente (`cost_basis`): `promedio_bsale` (defecto) o `ultimo_comparable`; bruto = neto × (1 + (IVA + ILA)/100) con el motor tributario ([02](02-riesgos-tributarios.md)); incluye tasas, factor y validación | Base del recargo |
| `costs.validated_gross` | No existe aún → `no_disponible` | Futuro |

Nunca se reemplaza una base por otra: si el SKU no tiene costo en la base elegida queda
`sin_costo` aunque tenga la otra; el usuario elige la base explícitamente. No se usan máximos
históricos.

### Fórmulas

- Recargo % = (Precio bruto / Costo bruto − 1) × 100 → comparado contra `min_margin` / `max_margin`.
- Margen sobre venta % = (Precio bruto − Costo bruto) / Precio bruto × 100 → informativo.
- Precio objetivo mínimo = Costo bruto × (1 + mín/100), redondeado al peso superior; máximo al peso
  inferior. Provisional mientras la validez sea provisional.
- Precio bruto = `variant_prices.variant_value_with_taxes` (valor original Bsale, sin recalcular).
- Comparaciones con Decimal sin redondear; en JSON se informa redondeado a 2 decimales.

### Estados

Estado principal (precedencia): `sin_precio` → `sin_costo` → `sin_regla` →
`requiere_revision_tributaria` (bruto no calculable por impuestos) → `bajo_politica` / `dentro_politica` /
`sobre_politica` → `promocion_vigente` / `remate_vigente` (solo con excepción verificada).

Calificadores: `costo_no_verificado` (costo preliminar o confiabilidad requiere revisión /
desactualizado), `politica_provisional` (regla pendiente de revisión Excel),
`promocion_pendiente_verificacion` (bajo política y Promociones V2 no integrado o precio no coincide).

`confirmation`: un estado comparativo es `confirmado` solo sin calificadores; hoy todos son
`calculado`. `validity`: `provisional` mientras la política no esté validada o el costo no esté
verificado (hoy: todos provisionales).

Precios sin valor: sin fila en la lista, `missing_since`, bruto nulo, ≤ 0 o simbólico (< $10) →
`sin_precio`, sin porcentajes.

Reglas: coincidencia por empresa + lista + tipo (o regla general de lista si existiera).
Regla inactiva o sin mínimo → `sin_regla`. `max_margin = 0` → observación
`maximo_cero_requiere_revision` y nunca produce `sobre_politica`.

### Excepciones por promoción (contrato)

`PromotionException(company_id, variant_id, price_list_id, kind promocion|remate, starts_on,
ends_on, applicable_gross_price, approval pendiente|aprobada|rechazada, reference)`.
Solo una excepción aprobada, vigente, específica y con precio Bsale igual al aplicable reemplaza
`bajo_politica` por `promocion_vigente` / `remate_vigente`. Los riesgos de costo (confiabilidad,
anomalías) se siguen mostrando.

> **Actualización Etapa 3:** la fuente ahora es `CampaignExceptionSource` (Promociones V2 +
> promociones anteriores, estas últimas nunca conceden excepción). Si las tablas V2 no existen,
> el resultado sigue siendo `fuente_no_integrada`. Nuevos checks: `excepcion_pendiente_aprobacion`,
> `conflicto_promociones`, `precio_no_verificable`, `promocion_legacy_no_verificable`. Cada fila
> incluye `risks` (`venta_bajo_costo`, `costo_sospechoso`, `costo_desactualizado`,
> `costo_no_verificado`, `sin_costo_valido`, `precio_no_verificado`), visibles aunque haya
> excepción. Detalle en [06-promociones-v2.md](06-promociones-v2.md).

## Endpoints (permiso `margins`: gerencia, adm, admin, finanzas)

| Ruta | Descripción |
|---|---|
| `GET /analytics-v2/margins/summary?company_id&cost_basis` | Totales por estado, por lista, política y alcance |
| `GET /analytics-v2/margins/filters?company_id` | Listas evaluadas, tipos, estados, bases, órdenes |
| `GET /analytics-v2/margins/scope?company_id` | Alcance y discrepancias de listas |
| `GET /analytics-v2/margins/rows?company_id&q&price_list_id&product_type_id&status&qualifier&reliability&cost_basis&variant_state&sort&page&page_size` | Tabla paginada (≤ 200) |
| `GET /analytics-v2/margins/products/{variant_id}?company_id` | Todas las listas evaluadas de un SKU |
| `GET /analytics-v2/margins/policy-review?company_id&only_issues` | Reglas con estado de revisión, observaciones, alcance y combinaciones faltantes |

Solo GET. El historial de costos se abre desde la pantalla con la ficha de Control de Costos V2,
visible solo con permiso `costs` (finanzas ve márgenes pero no el detalle de recepciones).

## Comparación con legacy

| Aspecto | Legacy (`/margins`, `margin_analysis_view`, `price_list_control_service`, `erp_margin_dashboard`) | Márgenes V2 |
|---|---|---|
| Precio | `bsale.variant_prices.price_gross` (sync legacy) | `bsale_raw.variant_prices.variant_value_with_taxes` original |
| Costo | `bsale.variant_cost.average_cost_net × products.tax_factor`; si `tax_factor` es NULL/0 usa **1** (neto tratado como bruto) | Base explícita; bruto solo con perfil tributario reconocido, si no `costo_no_verificado` |
| Costo en control de precios | `analytics.cost_reception_history` con máximo válido histórico | Última compra comparable o promedio Bsale, a elección, sin máximos |
| Recargo | `(P − C)/C × 100` en la vista (equivalente) | `(P/C − 1) × 100` |
| Margen sobre venta | Algunos módulos comparan `(P − C)/P` contra `min_margin` (mezcla de definiciones) | Solo informativo; la política se compara siempre con el recargo |
| Listas | Todas las listas con precio; no mira `state` | Solo autorizadas y vigentes en RAW; COMODITI fuera |
| Sin regla | LEFT JOIN: puede verse como sin alerta | Estado `sin_regla` explícito |
| `max = 0` | Se compara literalmente (todo queda "sobre") | Observación, sin `sobre_politica` |
| Promociones | No consideradas | Contrato de excepción; bajo política queda "por verificar" |
| Provisional | No distingue | `validity` + `confirmation` en cada fila |

## Cobertura y restricciones tributarias

- Calculable (provisional): perfiles `iva`, `iva_ila` (un ILA) y `exento_verificado`.
- No calculable → `requiere_revision_tributaria` sin porcentajes ni precio objetivo:
  `iva_anticipado` (harina, carne), `ila_combinado`, `impuesto_no_reconocido`,
  `configuracion_inconsistente`, `sin_impuestos` y `no_consultado` (sin impuestos no es exento).
- El precio bruto Bsale incluye los impuestos configurados en el producto; el costo bruto usa la
  misma combinación. Validar con la consulta 8 de `cost_gross_tax_validation.sql`.
- `NET_COST_VERIFIED = False` y `TAX_CONFIG_VERIFIED = False`: ningún resultado es "validado".

## Estado de validación contra RAW real

El rol MCP disponible (solo lectura) puede leer `bsale.margin_rules`, `bsale.companies`,
`bsale_raw.variants`, `bsale_raw.offices` y `bsale_raw.sync_state`; **no** puede leer
`variant_prices`, `price_lists`, `variant_costs`, `products`, `product_types`, `product_taxes`,
`taxes` ni recepciones. No se ampliaron permisos.

Verificado con datos reales:
- 361 reglas (108 company 1, 105 company 2, 148 company 3). Company 3: listas 12, 13, 14, 16 con 37
  reglas cada una, sin `product_type_id` nulo, sin `min > max`, columnas reales incluyen
  `rule_key`, `price_list_name`, `product_type_name`.
- `max_margin = 0`: 22 reglas en lista 14 (COMODITI) y 1 por lista en 12, 13 y 16
  (CIGARRILLOS, mín 0 y máx 0).
- `sync_state`: precios RAW sincronizados hoy para listas 12, 13 y 16 (6.123 filas cada una) y para
  listas 2–11; **no** hay sincronización de precios de la lista 14 (el sync barre listas activas),
  lo que indica que COMODITI está inactiva en RAW (confirmar con consulta 9).

Pendiente de ejecución manual en pgAdmin (archivo
`backend/sql/analytics_v2/checks/raw_cost_control_validation.sql`): consultas 1–14, en especial
9 (vigencia de listas), 11 (calidad de precios), 12 (factor bruto/neto), 13 (combinaciones sin
regla) y 14 (EXPLAIN). Los números de márgenes no deben considerarse validados hasta ejecutarlas.

Observación: RAW tiene listas 2–11 con precios sincronizados. Si alguna es vigente en Bsale aparecerá
como `vigente_no_autorizada` (informativo, no evaluada). Confirmar con gerencia si alguna debe
agregarse al alcance.

## Riesgos de seguridad pendientes

| Riesgo | Estado |
|---|---|
| JWT sin vencimiento | Preparado: `JWT_EXPIRE_MINUTES` agrega `exp`/`iat`; `JWT_REQUIRE_EXP=true` rechaza tokens sin `exp`. Desactivado por defecto (no rompe sesiones vigentes). Activación recomendada: primero `JWT_EXPIRE_MINUTES` (p. ej. 720), y tras ese plazo `JWT_REQUIRE_EXP` |
| Sesión vencida | Backend responde 401 "Sesión expirada"; las pantallas V2 limpian la sesión y redirigen a `/login` |
| Secreto de desarrollo | Si `ENVIRONMENT`/`ENV` no es `production`/`staging`/`prod` y falta `JWT_SECRET_KEY`, se usa la clave fija de desarrollo. Verificar variables en el servidor; no se cambió el comportamiento para no romper el arranque |
| Permisos por empresa | Solo claim opcional `company_ids` + empresa activa + RAW listo. Falta tabla usuario↔empresa (migración futura) |
| `/companies` público | Sin cambios (solo nombres) |
| Lectura vs escritura | Márgenes V2 solo expone GET; no hay acciones de precio |
