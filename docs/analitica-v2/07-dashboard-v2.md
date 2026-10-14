# Analítica V2 — Etapa 4: Dashboard V2

Estado: **implementación local en validación**. Sin commit ni despliegue. `/dashboard` (legacy) no se modifica.

Pantalla `/analitica-v2/dashboard` que reúne costos, márgenes, listas de precios, ofertas y remates.
Solo lectura sobre BSALE RAW y `app.*`; no escribe precios en Bsale ni modifica reglas de margen.

## Arquitectura

| Capa | Archivo | Rol |
|---|---|---|
| Contratos | `backend/services/analytics_v2/contracts.py` | `DataClass`, `FreshnessStatus`, `SEVERE_MARGIN_RISKS`, umbrales del dashboard |
| Costos | `cost_control_service.py` → `overview()` | Indicadores de costo (mismo motor que Control de Costos V2) |
| Márgenes | `margins_service.py` → `overview()` | Indicadores por lista (mismo motor que Márgenes V2) |
| Promociones | `promotions_service.py` → `summary()` | KPIs de campañas (mismo motor que Promociones V2) |
| Composición | `dashboard_service.py` | Permisos por bloque, aislamiento de fallos, frescura, confiabilidad, caché |
| API | `backend/routers/analytics_v2.py` | `GET /analytics-v2/dashboard` |
| Pantalla | `frontend/app/(dashboard)/analitica-v2/dashboard/page.tsx` | Ruta nueva |
| Componentes | `frontend/components/analytics-v2/dashboard/*` | Secciones y ficha de producto |

El dashboard **no tiene fórmulas propias**: cada número sale de `overview()` / `summary()` del motor
correspondiente, que a su vez reutiliza las mismas filas que las pantallas de detalle. Un número del
dashboard y el listado filtrado al que enlaza salen del mismo cálculo.

## Endpoint

`GET /analytics-v2/dashboard?company_id=&cost_basis=promedio_bsale|ultimo_comparable&refresh=false`
(defecto `promedio_bsale`)

- Requiere permiso `margins` (gerencia, adm, admin, finanzas). Comercial recibe 403.
- La empresa se valida en cada llamada, también cuando la respuesta sale de caché.
- Respuesta: `reliability`, `freshness`, `margins`, `costs`, `taxes`, `promotions` y `meta`.
- Cada bloque es `{status: ok | sin_permiso | no_disponible, reason, elapsed_ms, data}`:
  - `costs` exige permiso de costos (gerencia); finanzas lo ve como `sin_permiso`.
  - `taxes` exige permiso de márgenes o costos (cobertura tributaria, sin montos de costo).
  - `promotions` exige `promotion_capabilities.view`.
  - Si un bloque falla (`tiempo_agotado`, `error_de_datos`, `sin_alcance_comercial`, `error_interno`),
    se hace rollback y los demás bloques siguen; no se expone el detalle del error.
- Caché de 60 s por empresa + base de costo + permisos + día. No se guarda en caché una respuesta
  con bloques fallidos. Una escritura en Promociones V2 invalida la caché.

## Indicadores

**Márgenes** (solo listas vigentes y autorizadas; COMODITI y listas no vigentes quedan fuera y se
listan como excluidas):

- Totales: SKUs únicos, combinaciones SKU × lista y listas evaluadas.
- Bajo política sin promoción ni remate vigente (SKUs y combinaciones).
- Promoción por verificar (precio Bsale no coincide o campaña sin aprobar).
- Excluidos por promoción o remate vigente verificado, y cuántos de ellos tienen riesgo de costo.
- Venta bajo costo (incluye promociones y remates: el riesgo no se oculta).
- Clasificación del dato: confirmado, provisional o no disponible.
- Tabla por lista: SKUs, con precio, bajo, dentro, sobre, promo/remate, no disponible y fecha de
  sincronización de precios. Cada número enlaza a Márgenes V2 filtrado.
- Mayores brechas bajo política y promociones/remates con riesgo de costo.
- Política: reglas en alcance, pendientes de revisión, máximo 0, con observaciones y faltantes.

**Costos:** SKUs activos, con costo confiable, que requieren revisión, con anomalía alta, alzas
relevantes (≥ umbral de Control de Costos V2) y alertas de severidad alta de los últimos 30 días.

**Impuestos y costo bruto comercial:** SKUs con impuestos identificados, con bruto calculable, sin
costo, con impuestos sospechosos (revisión tributaria) y sin configuración; cobertura %,
combinaciones, anomalías con ejemplos y catálogo RAW. Detalle en
[02-riesgos-tributarios.md](02-riesgos-tributarios.md).

**Ofertas y remates:** campañas vigentes y próximas, productos en oferta y en remate, pendientes de
aprobación o verificación, e ítems vencidos sin revertir en Bsale.

### Clasificación del dato

| Clase | Regla |
|---|---|
| No disponible | Estado `sin_precio`, `sin_costo`, `sin_regla`, `costo_no_verificado` o `requiere_revision_tributaria` |
| Confirmado | Cálculo confirmado y validez `validado` (política revisada y costo verificado) |
| Provisional | Cualquier otro caso |

Mientras la política de márgenes siga pendiente de revisión y el costo bruto sea preliminar, todo
resultado calculado es **provisional**.

### Promociones y alertas

Una combinación con promoción o remate vigente, aprobado y con precio verificado contra Bsale no
cuenta como alerta "bajo política". Sus riesgos de costo graves (`venta_bajo_costo`,
`costo_sospechoso`, `costo_desactualizado`, `sin_costo_valido`) siguen visibles en
"Promociones y remates con riesgo de costo" y en "Venta bajo costo".

### Frescura y confiabilidad

Por recurso RAW (`price_lists`, `variant_prices`, `variant_costs`, `stock_receptions`, `variants`,
`products`, `product_taxes`, `stocks`) se toma el peor estado entre sus scopes. El scope `point`
(sincronización puntual bajo demanda) se ignora si el recurso tiene barridos periódicos
(`price_list:N`, `office:N`, `admission_date`), porque su antigüedad no mide la frescura del recurso.

- `con_error`: el último intento terminó en `FAILED`.
- `sin_datos`: no hay ninguna sincronización exitosa registrada.
- `atrasada`: el último éxito tiene más de 36 h.
- `al_dia`: en cualquier otro caso.

El nivel global es "no disponible" si el bloque de márgenes no está disponible, "provisional" si hay
cualquier advertencia y "confirmado" solo sin advertencias.

## Navegación

- Las tarjetas y celdas enlazan con filtros por URL: `/analitica-v2/margenes?status=&qualifier=&price_list_id=&cost_basis=&sort=&tab=politica`,
  `/analitica-v2/costos?reliability=&sort=&min_change=&tab=alertas&variant_id=` y
  `/analitica-v2/promociones?tab=&campaign_id=`. Los valores se validan contra las listas conocidas.
- Un clic en un producto abre su ficha: el SKU en todas las listas evaluadas y, con permiso de costos,
  el historial de costos y recepciones.

## Rendimiento

- Una sola conexión read-only con `statement_timeout` de 25 s y `lock_timeout` de 3 s.
- Los motores reutilizan cachés propios (costos y márgenes 120 s); los ítems de costo del día se
  calculan una vez por contexto y los usan márgenes y costos.
- Test con 6.000 SKUs bajo 10 s en memoria. El tiempo real debe medirse con `EXPLAIN ANALYZE` en la
  empresa 3 antes de publicar.

## Pendiente de validación con datos reales

Verificado (SELECT, empresa 3, 14-10-2026): los ocho recursos existen en `bsale_raw.sync_state`
con esos nombres; los scopes periódicos están al día y solo los scopes `point` de `stocks` y
`variant_prices` tenían éxitos del 10-10.

- `alert_candidates` de 30 días: volumen y tiempo.
- `variant_prices.fetched_at` por lista.
- Persistencia funcional de Promociones V2 en PostgreSQL (tablas y triggers ya verificados).
- Tiempos con `EXPLAIN` en la empresa 3.
