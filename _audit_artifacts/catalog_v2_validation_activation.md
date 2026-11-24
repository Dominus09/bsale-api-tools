# Catálogo V2 — Validación final y activación controlada (local)

**Fecha consulta:** 2026-11-24 ~15:35–15:38 UTC  
**Empresa:** 3 · **Oficina catálogo:** 1 · **Campo stock público:** `quantityAvailable`  
**SHA:** (ver commit de esta entrega)  
**Restricciones respetadas:** sin push / redeploy / SQL prod / Coolify / crons / cambio picking-OC.

---

## 1. Stock vs Bsale

### Método
- Legacy: `bsale.stocks.quantity_available` (vista `catalog_view`; última sync **2026-10-28**).
- RAW: `bsale_raw.stocks` office 1 — columnas + **payload API** (`quantity`, `quantityAvailable`, `quantityReserved`).
- Live `GET /v1/stocks.json?variantid=&officeid=1`: **no ejecutado** — `BSALE_TOKEN` ausente en local. Script listo: `python -m backend.scripts.catalog_v2_bsale_point_check …`.

### Corrección de interpretación
El diff previo “Mariposa 227 vs **503**” mezclaba campos.  
`catalog_view` y el contrato público usan **`quantity_available`**, no `quantity`.  
V2 preview corregido a `stock = quantity_available`.

### Casos críticos

| SKU (barcode) | Producto | Legacy avail | RAW qty | RAW avail | Bsale API payload avail | Resultado | Causa |
|---|---|---:|---:|---:|---:|---|---|
| 7802626001163 | Harina Mariposa 25 kg (28216) | 227 | 503 | **446** | **446** (+ reserved 57) | RAW=API; legacy obsoleto | Legacy stocks 28-oct; reservas 57 |
| 7790119009881 | Sidra Real Blanca 750 (29094) | 4827 | 3392 | **3392** | **3392** | RAW=API; legacy obsoleto | Legacy 28-oct; sin reserva actual |
| 7802100003720 | Sol x18 (31199) | 324 | 243 | **243** | **243** | RAW=API; legacy obsoleto | Legacy 28-oct |

### Tabla extendida (≥20)

| variant_id | Producto | Legacy | RAW avail o1 | API payload | Resultado | Causa |
|---:|---|---:|---:|---:|---|---|
| 28216 | Harina Mariposa 25 kg | 227 | 446 | 446 | **diff legacy** | stale + reservas |
| 28258 | Harina TG 25 kg | 31 | 27 | 27 | **diff legacy** | stale |
| 30814 | Harina Rahue 25 kg | 22 | 2 | 2 | **diff legacy** | stale |
| 30293 | Harina Kunstmann Integral | 0 | 0 | 0 | match | — |
| 31510 | Harina Soft 25 (sin price_13) | 0 | 0 | 0 | match / sin precio lista | variante sin precio 13 |
| 31511 | Harina Fuerza 25 (sin price_13) | 0 | 0 | 0 | match / sin precio lista | idem |
| 29094 | Sidra Real Blanca 750 | 4827 | 3392 | 3392 | **diff legacy** | stale |
| 23312 | Sidra Real Blanca 910 | 0 | 0 | 0 | match | — |
| 29289 | Sidra Real Citrus 750 | 0 | 0 | 0 | match | — |
| 24709 | Sidra Etiqueta Negra 750 | 0 | 0 | 0 | match | — |
| 11184 | Sidra 910 (sin price_13) | 0 | 0 | — | sin fila stock RAW | inactiva/sin sync point |
| 31199 | Sol Pack x18 | 324 | 243 | 243 | **diff legacy** | stale |
| 19577 | Cerezas Ambrosoli SEC18 | 0 | 0 | 0 | match | IVA-like |
| 31339 | Halloween Ambrosoli | 90 | 0 | 0 | **diff legacy** | stale (agotado en RAW) |
| 30931 | Exportación Selecto 1.5 L | 114 | 114 | 114 | match | UPB missing PM |
| 31366 | Margarina Soprole | 0 | 0 | 0 | match | stock0 |
| 31371 | Manteca Llanquihue | 0 | 0 | 0 | match | stock0 |
| 31372 | Mentos | 0 | 0 | 0 | match | stock0 |
| 31373 | Hershey Dark | 0 | 0 | 0 | match | stock0 |

**Oficina 2:** en la muestra, `raw_avail_o2 = 0` (stock concentrado en oficina 1 para estos SKU).  
**Unidad:** misma variante Bsale (`variant_id`); no hay conversión de unidad en sync stock.

---

## 2. Precio e impuestos (lista 13)

| variant_id | price_13 / gross | price_net | ratio gross/net | Lectura |
|---:|---:|---:|---:|---|
| 28216 Mariposa | 20990 | 16022.90 | ~1.31 | IVA + otros (no solo 19%) |
| 29094 Sidra 750 | 4080 | 2924.73 | ~1.395 | patrón ILA + IVA |
| 31199 Sol x18 | 12800 | 9175.63 | ~1.395 | patrón ILA + IVA (cerveza) |
| 19577 Cerezas | 3914 | 3289.08 | ~1.19 | IVA ~19% |
| 30931 Selecto | 3590 | 2573.48 | ~1.395 | ILA-like |

- Contrato público sigue exponiendo **un** `price` = gross lista (factura/melinka).  
- `price_net` solo en metadatos `v2` (preview gerencia).  
- **Sin** costos/márgenes en contrato público.  
- **Sin** descuentos inventados; promociones no se alteran en esta etapa.

---

## 3. UPB faltantes

| Métrica | Valor |
|---|---:|
| PM activos sin UPB | ~998 |
| Con stock RAW o1 y sin UPB PM (prioridad) | **baja** (p.ej. 30931 con 114 u) |
| Sol x18 (31199) | **Sin fila PM**; variant.units_per_box NULL; descripción `(SEC 1)` |

**Política V2:** `upb_status` ∈ `verified` | `inherited_sec` | `missing`.  
**No** se asigna 18 desde “x18” del nombre.  
Sol x18 → prioridad ERP: crear/actualizar PM `units_per_box=18` manual.

Endpoint: `GET /api/catalog-v2/coverage/upb-missing` (gerencia).

---

## 4. Integridad RAW (separada)

| Familia | Estado | Notas |
|---|---|---|
| **stocks** | office1 SUCCESS reciente; office2 puede estar **RUNNING** durante ciclo | residual 401/timeout **stale** (error_at < success_at) |
| **variant_prices** | SUCCESS listas 13/16 hoy | residuales snapshot/timeout stale |
| **products / variants / taxes** | SUCCESS 24-nov | 401 del 23-nov residual |
| **variant_costs** | SUCCESS reciente | no bloquear catálogo |

Regla: no bloquear por residual histórico; no marcar saludable si `RUNNING` o `error_at >= success_at`.

---

## 5. Feature flag (local, default legacy)

| Variable | Efecto |
|---|---|
| `CATALOG_SOURCE_PUBLIC=legacy\|v2` | **Única** que cambia `GET /api/catalog` |
| `CATALOG_SOURCE` | fallback otros consumidores no hard-deny; **no** activa público solo |
| `CATALOG_SOURCE_ERP_PREVIEW` | preview `/api/catalog-v2/*` (default v2) |

**Hard-deny V2:** `picking`, `dispatch`, `purchase_orders`, `order_pricing` → siempre legacy.

Fallback: si V2 falla en `get_catalog`, vuelve a SQL legacy automáticamente.

---

## 6. Mapa de consumidores `GET /api/catalog`

| Consumidor | Cómo usa catálogo | ¿Flag público lo afecta? |
|---|---|---|
| **Catálogo web** `cat.quillotana.cl` | `GET /api/catalog?price_list=` | **Sí** (solo con `CATALOG_SOURCE_PUBLIC=v2`) |
| ERP Inteligencia `/analytics-v2/catalog` | endpoints analytics-v2 propios | No |
| `order_pricing` | `bsale.catalog_view` directo | No (hard legacy) |
| Picking / despacho / peso | PM + variants legacy | No |
| Góndolas | servicio propio stock | No |
| Preview gerencia | `/api/catalog-v2/*` | Independiente |

---

## 7. Pruebas

```
pytest backend/tests/test_catalog_v2_projection.py \
       backend/tests/test_catalog_v2_source_flag_and_contract.py
→ 17 passed
```

Cobertura añadida: sin UPB, sin foto, sin precio, stock0, available≠quantity, dos listas, permisos preview, contrato sin `v2`/cost, rollback flag, hard-deny picking, aislamiento público vs global.

---

## 8. Rollback

1. `CATALOG_SOURCE_PUBLIC=legacy` (o unset) → público vuelve a `catalog_view` inmediato.  
2. Si V2 error → fallback automático a legacy en el handler.  
3. Picking/OC no requieren rollback (nunca leyeron V2).  
4. No hay migraciones ni DROP.

---

## 9. Plan de activación gradual (pendiente aprobación)

1. Mantener preview gerencia + compare en staging/prod API **sin** `CATALOG_SOURCE_PUBLIC=v2`.  
2. Revalidar 20–50 SKU con `BSALE_TOKEN` puntual (script).  
3. Corregir Sol x18 UPB en PM (manual ERP).  
4. Canary: `CATALOG_SOURCE_PUBLIC=v2` en ventana corta; monitorear precios/stock.  
5. Si OK → mantener; si no → unset flag.  
6. Recién después: valorar lectura stock RAW en picking (ticket aparte).

---

## 10. Código tocado

- `backend/services/catalog_v2/source_flag.py` (nuevo)
- `backend/services/catalog_v2/projection.py` (available + UPB provenance + missing report)
- `backend/routers/catalog.py` (flag + fallback; default legacy)
- `backend/routers/catalog_v2_preview.py` (upb-missing, source-flag)
- `backend/scripts/catalog_v2_bsale_point_check.py`
- tests + `.env.example`
