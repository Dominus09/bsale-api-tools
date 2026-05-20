# Arquitectura: heurística «Probable facturada» (FASE 7.16)

## Objetivo

Cubrir **edge cases Bsale** donde la UI muestra la OC facturada pero la API pública no expone linkage (`relateddetailid`, `references` vacíos). La fuente oficial de facturación sigue siendo **`document_related`** (sync `relateddetailid`). Esta capa es **solo analítica**.

## Estados (`v_purchase_document_status`)

| Prioridad | `status` | `estado_real` (UI) | Fuente |
|-----------|----------|-------------------|--------|
| 1 | `FACTURADA_CONFIRMADA` | Facturada | `document_related` → tipo 1/6 |
| 2 | `PROBABLE_FACTURADA` | Probable facturada | `document_probable_matches` (mejor score ≥ 60) |
| 3 | `PENDIENTE` | Pendiente | Sin señal suficiente |

**Regla:** related real > probable > ninguno. Nunca se inserta en `document_related` ni se altera `relateddetailid`.

## Modelo de score (0–100)

Pesos máximos (cap total 100):

| Factor | Puntos | Notas |
|--------|--------|-------|
| Productos (`variant_id` + `quantity`) | 50 | `%` líneas OC encontradas en candidato |
| Mismo `client_id` | 15 | Obligatorio en búsqueda de candidatos |
| Proximidad fecha emisión | 12 | Mismo día = 12; ±N días lineal (env `PROBABLE_INVOICE_WINDOW_DAYS`, default 3) |
| Monto | 13 | Tolerancia `PROBABLE_INVOICE_AMOUNT_TOLERANCE_PCT` (default 15%) sobre total documento; si 100% líneas y total candidato ≥ OC → 75% del peso (boleta consolidada) |
| Vendedor (`seller_id` o `user_id`) | 5 | |
| Tracking idéntico (ambos no vacíos) | +20 bonus | |
| Superset de líneas OC en boleta | +10 bonus | |
| Misma dirección normalizada | +5 | |

### Clasificación persistida / UI

| Score | Tier |
|-------|------|
| ≥ 90 | `PROBABLE_FACTURADA_HIGH` |
| ≥ 75 | `PROBABLE_FACTURADA_MEDIUM` |
| ≥ 60 | `PROBABLE_FACTURADA_LOW` |
| < 60 | No se persiste |

## Búsqueda de candidatos

- OC tipo **33**, sin fila confirmada en `document_related` → 1/6.
- Boletas/facturas tipo **1/6**, `office_id = 1`, mismo `client_id`.
- Ventana emisión: **±3 días** (configurable).

## Persistencia

Tabla: `distribuidora.document_probable_matches`

- `UNIQUE (oc_document_id, candidate_document_id)`
- Campos de auditoría: `score`, `match_products_pct`, flags `same_*`, `tracking_match`, `created_at`

Vista: `distribuidora.v_purchase_document_status`  
Vista enriquecida actualizada: `v_orders_purchase_enriched` (expone `purchase_status`, `probable_*`).

## Job

```bash
python -m backend.jobs.build_probable_invoice_matches_may_2026
```

- **Read-only** respecto a API Bsale (usa datos ya sincronizados en PostgreSQL).
- Solo escribe `document_probable_matches`.
- Validación integrada:

```bash
python -m backend.jobs.build_probable_invoice_matches_may_2026 --validate-oc 66697 --validate-boleta 2616098
```

## Caso real validado: OC 66697 → Boleta 2616098

| Campo | OC 66697 | Boleta 2616098 |
|-------|----------|----------------|
| `document_id` | 3755778 | 3756913 |
| `client_id` | 1473 | 1473 |
| Emisión | 2026-05-17 | 2026-05-18 (Δ 1 día) |
| Total | $1.507.970 | $3.268.548 (boleta con línea extra) |
| Líneas OC | 4 variantes | Mismas 4 cantidades + línea 2880 u (variant 11053) |
| `relateddetailid` | Vacío | — |
| API linkage | No expuesto | — |

**Score esperado:** ≥ 90 (`PROBABLE_FACTURADA_HIGH`) por match 100% de líneas OC, mismo cliente, ventana fecha, bonus superset/consolidada y misma dirección.

## Edge cases

1. **Boleta consolidada** — Varias OC o líneas extra en boleta: match por subconjunto de líneas; monto total puede diferir > 15%.
2. **Mismo cliente, distintos pedidos mismo día** — Riesgo de falso positivo si productos similares; mitigar con score productos y tracking.
3. **Tracking solo en boleta** — Sin match de tracking no suma +20; no penaliza.
4. **Vendedor distinto** — Común en OC 66697 (user 85 vs 49); no bloquea si productos + cliente alinean.
5. **NC (tipo 9)** — Fuera de candidatos; no sustituye factura confirmada.

## Falsos positivos / límites

| Riesgo | Mitigación |
|--------|------------|
| Dos OCs mismo cliente/productos cercanos | Ventana ±3 días + match cantidades exactas; revisar tier MEDIUM manualmente |
| Montos muy distintos sin líneas alineadas | Score < 60, no persiste |
| UI Bsale sin equivalente operacional | Heurística no aplica; queda `PENDIENTE` |
| Datos desactualizados en BD | Job depende de sync documents/details previo |

**Límite explícito:** No reemplaza facturación confirmada ni modifica sync oficial.

## Frontend

- Verde: **Facturada** (`FACTURADA_CONFIRMADA`)
- Amarillo: **Probable facturada** + tooltip analítico
- Gris: **Pendiente**

Componente: `frontend/components/distribuidora/orders/PurchaseInvoiceStatusBadge.tsx`

## Variables de entorno

| Variable | Default |
|----------|---------|
| `PROBABLE_INVOICE_AMOUNT_TOLERANCE_PCT` | `15` |
| `PROBABLE_INVOICE_WINDOW_DAYS` | `3` |

## Métricas mayo 2026 (post-job)

Tras ejecutar el job, consultar:

```sql
SELECT
  COUNT(DISTINCT oc_document_id) AS ocs_con_probable,
  COUNT(*) AS filas_matches,
  COUNT(*) FILTER (WHERE score >= 90) AS high,
  COUNT(*) FILTER (WHERE score >= 75 AND score < 90) AS medium,
  COUNT(*) FILTER (WHERE score >= 60 AND score < 75) AS low
FROM distribuidora.document_probable_matches pm
INNER JOIN distribuidora.v_documents_latest d ON d.document_id = pm.oc_document_id
WHERE d.emission_date >= '2026-05-01'
  AND d.emission_date < '2026-06-01';
```

Validación puntual OC 66697:

```sql
SELECT pm.*, d.number AS oc_number, c.number AS boleta_number
FROM distribuidora.document_probable_matches pm
JOIN distribuidora.v_documents_latest d ON d.document_id = pm.oc_document_id
JOIN distribuidora.v_documents_latest c ON c.document_id = pm.candidate_document_id
WHERE d.number = 66697;
```

## Archivos principales

| Archivo | Rol |
|---------|-----|
| `backend/services/distribuidora/probable_invoice_service.py` | Score + job builder |
| `backend/repositories/distribuidora/probable_matches_repo.py` | Upsert SQL |
| `backend/sql/distribuidora/014_document_probable_matches.sql` | Tabla + vistas |
| `backend/jobs/build_probable_invoice_matches_may_2026.py` | CLI job |
| `backend/tests/test_probable_invoice_service.py` | Test score OC 66697 |
