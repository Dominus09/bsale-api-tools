# Analítica V2 — Arquitectura común y contratos

Estado: Etapa 0 y Etapa 1 implementadas localmente (sin commit, sin deploy, sin migraciones).
Etapas 2–6 solo diseñadas (ver `03-disenos-etapas-2-5.md` y `04-plan-limpieza.md`).

## Principios

1. **Fuente única: BSALE RAW (`bsale_raw.*`)**. Ningún cálculo V2 lee tablas legacy
   (`bsale.variant_cost`, `app.cost_*`, vistas `margin_analysis_*`). Un test estático lo verifica.
2. **Solo lectura**. Toda consulta pasa por `open_readonly_connection` +
   `assert_sql_is_read_only`; la transacción se abre `READ ONLY` y termina en `ROLLBACK`.
   No se leen `payload`, `note` ni `user` de RAW.
3. **Fórmulas en un solo lugar**. Clasificación, anomalías, confiabilidad e impuestos viven en
   módulos puros de `backend/services/analytics_v2/`. El frontend solo formatea.
4. **Valores originales**. El costo promedio Bsale, el costo de recepción y el costo comercial
   válido se exponen por separado; nunca se sobrescriben ni se corrigen automáticamente.
5. **Aislamiento por empresa**. Todas las consultas filtran por `company_id` en cada CTE.
   Solo las empresas en `RAW_READY_COMPANY_IDS` (hoy `{3}`) responden; el resto devuelve 409.
6. **Dinero como string decimal exacto** en JSON (`money()`), para no perder precisión.

## Módulos backend

| Módulo | Responsabilidad |
|---|---|
| `contracts.py` | Versión de contrato, enums, umbrales, paginación, serialización |
| `access.py` | Permiso `costs` (gerencia/adm/admin) + resolución de empresa |
| `movement_classification.py` | Clasificación de movimientos (Python y CASE SQL equivalente) |
| `tax_profile.py` | Perfil tributario por producto y costo bruto preliminar |
| `cost_signals.py` | Anomalías por línea y confiabilidad por SKU |
| `commercial_scope.py` | Listas vigentes desde RAW (no por nombre), reglas fuera de alcance, validez `provisional`/`validado` |
| `cost_control_repo.py` | SQL RAW parametrizado |
| `cost_control_service.py` | Orquestación, filtros, orden y paginación en backend; caché 120 s por empresa |
| `routers/analytics_v2.py` | Endpoints `/analytics-v2/*` |

## Seguridad (Etapa 0)

- **Login fail-closed**: se eliminó el modo demo y los datos mock de `frontend/lib/api.ts`.
  Un error de red muestra un mensaje y no autentica. Un token demo heredado
  (`demo-token-12345`) se descarta al cargar la sesión.
- **`/promotions*` protegido** con `require_staff_user` a nivel de router.
- **V2**: cada endpoint exige token válido (401), permiso `costs` (403), empresa existente (404),
  activa (403), presente en el claim `company_ids` si el token lo trae (403) y con RAW listo (409).
- Errores internos devuelven 500 genérico, sin detalle de SQL ni credenciales; `statement_timeout` → 503.
- El navegador nunca habla con PostgreSQL ni recibe tokens Bsale.

Riesgos detectados y **no modificados** (requieren decisión):
- JWT sin `exp`; si `ENVIRONMENT` no está definido se usa el secreto de desarrollo. Verificar variables en producción.
- `GET /companies` es público (solo nombres).
- No existe tabla usuario↔empresa; V2 usa el claim opcional `company_ids`. Recomendado: tabla `app.user_companies` (migración futura revisada).

## Endpoints Control de Costos V2

Todos con `company_id` obligatorio (query) y `page_size ≤ 200`.

| Método y ruta | Descripción |
|---|---|
| `GET /analytics-v2/freshness` | Estado `sync_state` de recursos RAW relevantes |
| `GET /analytics-v2/costs/summary` | KPIs: SKUs, confiabilidad, anomalías, perfil tributario, movimientos |
| `GET /analytics-v2/costs/filters` | Catálogos para filtros (tipos, sucursales, enums) |
| `GET /analytics-v2/costs/products` | Tabla paginada; `q` (SKU/código/barra/nombre, sin acentos), `product_type_id`, `variant_state`, `reliability`, `tax_status`, `last_movement_class`, `anomaly_code`, `office_id`, `min_abs_change_pct`, `sort`, `page`, `page_size` |
| `GET /analytics-v2/costs/products/{variant_id}` | Ficha del SKU |
| `GET /analytics-v2/costs/products/{variant_id}/history` | Historial desde 2025 con costo comparable anterior, variación y anomalías por línea, más `series` para el gráfico |
| `GET /analytics-v2/costs/alerts` | Alertas por línea de recepción (por defecto últimos 90 días), con `reception_id` y `detail_id` |
| `GET /analytics-v2/costs/receptions/{reception_id}` | Cabecera, líneas y eventos del change log de la recepción original |

Toda respuesta incluye `meta`: `contract_version`, `data_source`, `company_id`, `company_name`,
`generated_at`, `history_start`, `net_cost_verified`.

## Contrato de ítem por SKU

```
variant_id, product_id, code, barcode, product_name, variant_name, product_type{id,name}, variant_state
bsale_average_cost {value, fetched_at, source:"bsale_variant_costs"}
last_reception        {reception_id, detail_id, date, office_id, office_name, document, document_number, movement_class, quantity, cost}
last_comparable       (misma forma; solo factura/guía con costo y cantidad > 0 y detalle completo)
previous_comparable   (misma forma)
change {amount, pct}  último comparable vs anterior comparable
tax_profile {status, tax_ids, taxes[], total_rate_pct, preliminary_allowed}
gross_cost_preliminary {value, status, base:"last_comparable"}
reliability {status, reason}
anomalies [{code, severity, message}]
counts {receptions, comparable}
```

## Clasificación de movimientos

Orden de evaluación (sin acentos, mayúsculas, espacios colapsados):

1. `internal_dispatch_id` distinto de NULL/0 → `transferencia_interna`
2. documento vacío → `no_determinado`
3. contiene `DESPACHO ANULADO` → `ajuste`
4. `NOTA DE CREDITO` → `nota_credito`
5. `AJUSTE` → `ajuste`
6. `FACTURA` → `factura`
7. `GUIA` → `guia_despacho`
8. `SIN DOCUMENTO` → `sin_documento`
9. otro → `no_determinado`

Solo `factura` y `guia_despacho` son **comparables** para costo de compra.
Pendiente: los folios de "GUÍA DE DESPACHO ELECTRÓNICA" son correlativos (25803–26793) y podrían
ser traspasos internos con `internal_dispatch_id = 0`. Validar con
`backend/sql/analytics_v2/checks/raw_cost_control_validation.sql` antes de confiar en ellos como compra.

## Anomalías y confiabilidad

Umbrales en `contracts.py`: alza ≥ 30 % (alta ≥ 100 %), baja ≤ −30 % (alta ≤ −60 %),
cantidad > 10 × mediana comparable, divergencia con promedio Bsale > 50 %, desactualizado > 180 días.

Confiabilidad por SKU: `sin_costo` → `solo_promedio_bsale` → `requiere_revision` → `desactualizado` → `consistente`.
Las alertas **no** se suprimen por promociones (regla para Etapas 3–5).

## Tres costos distintos

| Costo | Origen | Uso |
|---|---|---|
| Promedio Bsale | `bsale_raw.variant_costs.average_cost` | Referencia contable de Bsale |
| Costo de recepción | Último comparable en `stock_reception_details` | Señal de compra y anomalías |
| Costo comercial válido | Pendiente (Etapa 4) | Base de margen; se definirá con reglas explícitas y aprobación |

El costo comercial válido no se calcula aún: requiere la validación tributaria y del carácter neto del costo de recepción.
