# Analítica V2 — Etapa 3: Promociones y Remates V2

Estado: **implementación local en validación**. Migración **no ejecutada**. Sin commit ni despliegue.

Módulo de administración comercial: registra campañas de oferta y remate, su aprobación y su
historial, y verifica contra BSALE RAW si el precio está realmente aplicado. **No cambia precios en
Bsale**; registrar o aprobar una campaña no prueba que el precio esté aplicado.

## Arquitectura

| Capa | Archivo | Rol |
|---|---|---|
| Migración | `backend/sql/analytics_v2/001_commercial_campaigns_v2.sql` | Tablas nuevas aditivas (no ejecutada) |
| Dominio puro | `backend/services/analytics_v2/promotion_campaigns.py` | Vigencia, transiciones, validaciones, verificación de precio, mapeo legacy, ventanas de medición |
| Excepciones | `backend/services/analytics_v2/promotion_exceptions.py` | Resolución de excepción de margen (7 condiciones) |
| Repositorio | `backend/services/analytics_v2/promotions_repo.py` | SQL de lectura (RAW + app) y escritura (solo `app.commercial_*_v2`) |
| Fuente para Márgenes | `backend/services/analytics_v2/promotion_source.py` | `CampaignExceptionSource` |
| Servicio | `backend/services/analytics_v2/promotions_service.py` | Lecturas, escrituras, permisos, historial |
| Permisos | `backend/services/analytics_v2/access.py` | `promotion_capabilities`, `require_promotions_*` |
| API | `backend/routers/analytics_v2.py` | `/analytics-v2/promotions/*` |
| Pantalla | `frontend/app/(dashboard)/analitica-v2/promociones/page.tsx` | Ruta nueva, separada de `/promotions` |

La vigencia (`programada`, `vigente`, `finalizada`, `cancelada`) se calcula al consultar contra la
fecha de Chile (`America/Santiago`): una campaña deja de valer al día siguiente de su término sin
cron. Una cancelación vale desde el día de cancelación, también medido en hora de Chile
(`cancelled_at` es `timestamptz`; una cancelación a las 22:30 del día 13 corta la campaña el 12).

## Modelo de datos (nuevo, aditivo)

- `app.commercial_campaigns_v2`: empresa, tipo (`oferta` | `remate`), nombre, fechas, aprobación
  (`borrador` → `pendiente_aprobacion` → `aprobada` | `rechazada`; `cancelada`), responsable,
  observaciones, usuario y fecha de creación, envío, aprobación, rechazo y cancelación, motivo,
  `legacy_promotion_id`, `version` (concurrencia optimista).
- `app.commercial_campaign_items_v2`: variante, producto, lista (`price_list_id` numérico), SKU,
  código de barras y nombre al registrar, **precio normal Bsale al registrar** (leído en backend),
  precio promocional, motivo de remate, **stock de referencia** (leído en backend), estado
  `activo` | `retirado` (no se borra), `version`.
- `app.commercial_campaign_history_v2`: historial inmutable (trigger rechaza UPDATE/DELETE) con
  evento, estado anterior/nuevo, valores anteriores/nuevos (JSONB), motivo, usuario, rol y fecha.

Restricciones: FK compuesta campaña–empresa, unicidad de ítem activo por campaña + variante +
lista, CHECKs de fechas, precios, motivos y consistencia de aprobación/cancelación. Triggers
impiden DELETE de campañas e ítems.

Tablas anteriores (`app.promotions`, `app.promotion_items`, `app.promotion_companies`,
`app.promotion_price_snapshot`): **sin cambios**, solo lectura. Nunca volver a ejecutar
`promotions_module.sql` (contiene `DROP TABLE product_offers`).

## Excepción de margen

Se concede solo si se cumplen todas: misma empresa; misma variante; lista evaluada; campaña
aprobada; vigente hoy; precio promocional igual al precio Bsale leído hace ≤ 3 días; sin conflicto
con otra campaña activa del mismo SKU y lista. Orden de resolución:

1. Sin tablas V2 → `fuente_no_integrada`.
2. Sin candidatos aprobados → `excepcion_pendiente_aprobacion`, `promocion_legacy_no_verificable` o `sin_excepcion`.
3. Otra campaña activa para el mismo SKU y lista → `conflicto_promociones`.
4. Precio Bsale ausente o antiguo → `precio_no_verificable`.
5. Precio distinto → `excepcion_no_verificada`.
6. Todo coincide → `excepcion_verificada` (estado `promocion_vigente` / `remate_vigente`).

Solo se reemplaza `bajo_politica`. Los riesgos (`venta_bajo_costo`, `costo_sospechoso`,
`costo_desactualizado`, `costo_no_verificado`, `sin_costo_valido`, `precio_no_verificado`)
siguen visibles. Un remate puede tener recargo menor, pero no oculta venta bajo costo.

## Endpoints

Todos exigen sesión y `company_id`; la empresa se valida en backend (`RAW_READY_COMPANY_IDS`).
Los cuerpos rechazan campos no declarados (precio normal, stock o empresa enviados por el
navegador → 422).

| Método | Ruta | Permiso |
|---|---|---|
| GET | `summary`, `filters`, `campaigns`, `campaigns/{id}`, `items?section=`, `history`, `legacy` | ver |
| GET | `products/search` | editar |
| POST | `campaigns` | crear |
| PATCH | `campaigns/{id}` | editar |
| POST / PATCH | `campaigns/{id}/items`, `campaigns/{id}/items/{item_id}`, `.../remove` | editar |
| POST | `campaigns/{id}/submit` | editar |
| POST | `campaigns/{id}/approve`, `campaigns/{id}/reject` | aprobar |
| POST | `campaigns/{id}/cancel` | cancelar |
| POST | `legacy/{promotion_id}/import` | crear |

Permisos: ver = gerencia/adm/admin + finanzas + comercial; crear/editar = gerencia/adm/admin +
comercial; aprobar/cancelar = solo gerencia/adm/admin. No hay endpoints DELETE.

Cambios materiales en una campaña aprobada (fechas, productos, precio, motivo de remate) la
devuelven a `pendiente_aprobacion` y quedan en el historial.

## Pantalla `/analitica-v2/promociones`

Secciones: resumen mensual, ofertas vigentes, remates, próximas campañas, historial, pendientes de
verificación, y promociones anteriores (solo lectura con importación opcional como borrador).
Filtros: mes, lista, tipo, aprobación, vigencia y producto. KPIs: campañas vigentes, productos en
oferta, productos en remate, campañas finalizadas, excepciones verificadas y pendientes.

## Medición comercial futura

Cada campaña expone ventanas antes / durante / después de igual duración y su duración real. Las
métricas (unidades, venta neta, rotación, stock) **no están implementadas** y se devuelven vacías.

## Compatibilidad con promociones anteriores

Auditoría previa (producción, solo lectura): 3 promociones, 3 ítems, 5 precios congelados. Se
muestran sin modificarse; nunca otorgan excepción. La importación crea un borrador V2 solo con los
ítems cuya variante y lista se identifican sin ambigüedad en RAW; es idempotente.

## Pendiente antes de publicar

- Revisar y ejecutar la migración en un entorno de prueba con Postgres; conceder permisos de
  escritura sobre las tablas nuevas al usuario de la API.
- Confirmar la existencia del rol `comercial` en usuarios.
- Validar con gerencia el flujo de aprobación (hoy se permite autoaprobación, registrada).
