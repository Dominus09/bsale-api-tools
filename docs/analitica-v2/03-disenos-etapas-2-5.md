# Analítica V2 — Diseños Etapas 2 a 5 (no implementadas)

**Orden de desarrollo vigente** (ajuste de prioridades): primero el motor comercial sobre RAW
(Costos V2 → Márgenes V2 → Promociones y Remates V2 → Dashboard V2, con motor central de cálculo,
seguridad y trazabilidad). La actualización definitiva de la Política con Excel va después; el
módulo de Política solo se prepara (diagnóstico, estado de revisión, importación controlada).
El menú productivo no se reemplaza hasta validar los módulos nuevos.

Ninguna de estas etapas está implementada. Todas reutilizan `contracts.py`, `access.py`,
`tax_profile.py` y el servicio de costos de la Etapa 1. Toda migración aquí descrita es
**propuesta** y requiere revisión y autorización antes de ejecutarse.

---

## Etapa 2 — Política de Márgenes V2

### Reglas actuales (se preservan)

`bsale.margin_rules` tiene hoy 361 reglas: `company_id`, `product_type_id`, `price_list_id`,
`min_margin`, `max_margin`, `active`, `notes`, con índice único
`(company_id, price_list_id, COALESCE(product_type_id, -1))`.
**No se corrige, desactiva ni elimina ninguna regla automáticamente.**

Decisión vigente (ajuste de prioridades): las 361 reglas **no son la política definitiva**. Se
reemplazarán con un Excel que entregará el negocio. Hasta entonces todas tienen estado
`pendiente_revision` (`POLICIES_REVIEWED = False`) y todo resultado de margen que dependa de ellas
se marca `provisional` (`commercial_scope.result_validity`). Un resultado solo es `validado`
cuando la regla está validada y el costo base está verificado.

Las reglas de listas no vigentes en RAW (p. ej. COMODITI) se conservan como historia, pero no
entran en márgenes, dashboard ni alertas de política (`rule_in_commercial_scope`).

### Clave y fórmula

- Clave: empresa + lista de precios + tipo de producto.
- `Recargo % = (Precio bruto / Costo bruto − 1) × 100`.
  `min_margin` y `max_margin` se interpretan como recargo sobre costo (no margen sobre precio).
- El costo bruto usado es el costo comercial válido de la Etapa 4. Mientras sea preliminar, el
  resultado se marca `costo_no_verificado`.

### Diagnóstico de reglas (solo lectura, informativo)

| Hallazgo | Detección | Acción |
|---|---|---|
| Política faltante | Combinación lista activa × tipo con productos activos sin regla | Listar; el usuario crea la regla |
| Lista no vigente | Regla cuyo `price_list_id` tiene `state ≠ 0`, `missing_since` o no existe en RAW | Marcar como histórica; no desactivar ni alertar |
| `max_margin = 0` | Ambiguo: "sin máximo" o error de carga | Marcar para decisión; tratar como sin máximo solo si se aprueba |
| `min > max` (con max > 0) | Inconsistencia | Marcar |
| Regla duplicada por NULL | `product_type_id` NULL junto a reglas por tipo | Documentar la precedencia (tipo específico > NULL) |

### Historial y permisos (migración propuesta)

- `app.margin_rule_history(id, rule_id, company_id, changed_at, changed_by, field, old_value, new_value, reason)`,
  escrita en la misma transacción que el PATCH. Sin triggers sobre la tabla existente.
- Editar reglas: solo gerencia/admin (`costs`) y empresa autorizada. Lectura: gerencia.
- Endpoints propuestos: `GET /analytics-v2/policy/rules`, `GET /analytics-v2/policy/diagnostics`,
  `PATCH /analytics-v2/policy/rules/{id}` (con motivo obligatorio), `GET /analytics-v2/policy/rules/{id}/history`.

### Actualización controlada desde Excel (preparación)

Los cálculos leen la política como datos (empresa + lista + tipo → mínimo y máximo), nunca como
constantes en el código: actualizar porcentajes no requiere reprogramar.

Flujo propuesto (tablas nuevas; migración revisada antes de crearlas):

1. **Carga**: `app.margin_policy_imports(id, company_id, file_name, file_sha256, uploaded_by, uploaded_at, status)`
   y `app.margin_policy_import_rows(import_id, row_number, company_id, price_list_id, product_type_id, min_markup, max_markup, raw_values, validation_errors)`.
   El Excel se valida (empresa existente, lista y tipo existentes en RAW, porcentajes numéricos, min ≤ max).
2. **Comparación** contra `bsale.margin_rules`, fila por fila: `nueva`, `cambia`, `igual`,
   `ausente_en_excel`, `invalida`, `lista_no_vigente`. Se muestra en pantalla antes de aplicar.
3. **Aplicación** (solo gerencia, confirmación explícita, una transacción): actualiza o inserta
   reglas, escribe `app.margin_rule_history` con `import_id` y marca las reglas importadas como
   `validada` en `app.margin_rule_review(rule_id, status, import_id, reviewed_by, reviewed_at)`.
   Las reglas `ausente_en_excel` no se borran: quedan `pendiente_revision`.
4. **Reversión**: cada importación conserva los valores anteriores en el historial.

Se necesita el formato del Excel (columnas y si usa ids o nombres de lista y tipo) para definir el
mapeo. El estado de revisión vive en una tabla aparte para no alterar `bsale.margin_rules`.

La pantalla muestra "Políticas pendientes de revisión": reglas vigentes `pendiente_revision`,
combinaciones lista vigente × tipo sin regla, y reglas de listas no vigentes (solo informativo).

---

## Etapa 3 — Promociones V2

> **Implementación local (en validación):** ver [06-promociones-v2.md](06-promociones-v2.md).
> Difiere de este diseño en nombres (`app.commercial_campaigns_v2` / `_items_v2` / `_history_v2`)
> y en que la verificación contra Bsale se calcula al consultar, no se guarda como estado.

### Situación actual

`app.promotions`, `app.promotion_items` (por `barcode`), `app.promotion_companies` y
`app.promotion_price_snapshot` (con `price_list` como texto). Se **preservan** sin cambios; la
pantalla actual sigue operando. Las rutas `/promotions*` ya exigen sesión (Etapa 0).
`/sucursales/ofertas` usa datos en memoria (no persiste).

### Modelo propuesto (tablas nuevas, migración revisada)

- `app.commercial_events_v2(id, company_id, kind: oferta|remate, name, starts_at, ends_at, status, created_by, created_at, approved_by, approved_at, reason)`
- `app.commercial_event_items_v2(id, event_id, company_id, variant_id, price_list_id, promo_gross_price, expected_bsale_price, exception_reason, created_at)`
  con unicidad `(event_id, company_id, variant_id, price_list_id)`.
- `app.commercial_event_history_v2` (cambios de estado y precio, quién y cuándo).

### Reglas

1. **Excepción obligatoria por empresa + SKU + lista**: una promoción o remate que deje el precio bajo
   la política exige `exception_reason` y aprobación de gerencia por cada combinación.
2. **Verificación contra Bsale**: el precio vigente se lee de RAW (detalle de lista de precios). Estados:
   `aplicada` (coincide), `pendiente_en_bsale`, `difiere`, `vencida_sin_revertir`.
   V2 nunca escribe precios en Bsale.
3. **Remate**: permite precio bajo costo con motivo (vencimiento, descontinuado, daño) y fecha de término obligatoria.
4. **Las alertas de anomalía de costo no se ocultan** por una promoción vigente; la promoción solo cambia
   el estado de cumplimiento de margen (`promocion_vigente` o `remate_vigente`).
5. **Ventas (estructura futura)**: `app.commercial_event_sales_v2(event_id, variant_id, office_id, date, units, net_amount)`
   alimentada por una consulta RAW de documentos de venta. Solo se define la estructura; no se implementa.

### Migración desde `app.promotions`

Mapear `barcode` → `variant_id` y el texto `price_list` → `price_list_id` en RAW por empresa. Los ítems
ambiguos (barcode repetido o lista no encontrada) quedan en una tabla de revisión; no se migran a ciegas.
La pantalla legacy se retira solo cuando V2 esté validada.

---

## Etapa 4 — Márgenes V2

> Implementado localmente en Analítica V2.2: ver `05-margenes-v2.md` (fuente vigente de reglas y contratos).

Alcance: solo listas vigentes según RAW (`commercial_scope.resolve_price_list_scope`:
`state = 0` y sin `missing_since`). Para la empresa 3 se esperan 12, 13 y 16; si RAW difiere,
la respuesta lo informa (`matches_expected = false`) en vez de forzar la lista. COMODITI y
cualquier lista no vigente quedan fuera, sin importar su nombre.

Cada fila lleva `validity` (`provisional` | `validado`) y sus motivos
(`politica_pendiente_revision`, `costo_no_verificado`). Mientras las políticas no se revisen con
el Excel, todos los resultados son provisionales y la pantalla lo indica.

### Insumos por SKU × lista

- Precio bruto vigente de la lista activa (RAW).
- Costo comercial válido (definición pendiente; ver riesgos tributarios).
- Regla de la Etapa 2 (empresa + lista + tipo).
- Evento comercial vigente de la Etapa 3.

### Estado de cumplimiento (precedencia)

1. `sin_precio`: sin precio en la lista activa
2. `sin_costo`: sin costo comercial
3. `sin_regla`: sin política aplicable
4. `remate_vigente` / `promocion_vigente`: evento aprobado y verificado en Bsale (`promocion_pendiente_verificacion` si aún no coincide)
5. `costo_no_verificado`: costo preliminar o con confiabilidad `requiere_revision`
6. `bajo_politica` / `dentro_politica` / `sobre_politica`: según min y max

Se muestran recargo actual, recargo mínimo y máximo, precio sugerido al mínimo, y la diferencia.
Todo se calcula en el backend con paginación, en un solo servicio que consume las Etapas 1 a 3.

---

## Etapa 5 — Dashboard V2

> **Implementación local (en validación):** ver [07-dashboard-v2.md](07-dashboard-v2.md).
> `/dashboard` legacy se mantiene sin cambios.

Una sola llamada `GET /analytics-v2/dashboard?company_id=` que agrega los servicios V2 (sin fórmulas propias):

- Frescura RAW por recurso.
- Costos: SKUs por confiabilidad, alertas altas en 30 días, alzas relevantes.
- Márgenes: SKUs bajo política por lista, sin costo, sin regla.
- Promociones: vigentes, pendientes de verificación en Bsale, vencidas sin revertir.
- Políticas: pendientes de revisión y faltantes en listas vigentes.

Solo considera listas vigentes en RAW; las listas no vigentes (COMODITI) no aparecen ni generan
alertas. Los indicadores de margen se rotulan "provisional" mientras la política no esté validada.

Cada tarjeta enlaza a la pantalla V2 con el filtro aplicado. Reemplazará `/dashboard`
(`/margin-summary`, `/margin-alerts`) cuando esté validado.

---

## Tests requeridos por etapa (sección 10)

- Aislamiento de empresa y precios solo de listas vigentes en RAW (implementado: `test_analytics_v2_commercial_scope.py`).
- Listas no vigentes sin alertas de política; sus reglas siguen existiendo.
- Resultados provisionales hasta validar política y costo (implementado en el mismo test).
- Importación Excel: comparación sin aplicar, aplicación con historial, reglas ausentes no borradas.
- Recargo con la fórmula exacta; costo cero o nulo → `sin_costo`; sin regla → `sin_regla`.
- Promoción vigente y remate (bajo costo con motivo); excepción obligatoria por empresa + SKU + lista.
- Alertas de costo que persisten con promoción activa.
- Paginación, auth y planes de consulta (EXPLAIN) sobre empresa 3.
