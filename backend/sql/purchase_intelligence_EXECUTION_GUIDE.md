# Guía de ejecución — módulo compras inteligente

Archivo SQL: `purchase_intelligence_module.sql`

## Prerrequisitos

- Esquema `bsale` y tablas: `documents`, `document_details`, `variants`, `products`, `product_types`, `variant_cost`, `taxes`, `stocks`, `suppliers`.
- Si no existe `bsale.suppliers`, ejecutar antes `suppliers_schema.sql`.

---

## Etapa A — `variants` (ALTER + UPDATE)

1. Ejecutar el bloque **ETAPA A** del script (o el archivo completo en orden).
2. **Efecto:** columna `units_per_box`; relleno desde `description` con patrón **SEC** (p. ej. `(SEC 6)`, `(SEC 24)`), solo donde `units_per_box IS NULL`. No pisa valores ya cargados.

**Validación recomendada:**

```sql
SELECT
    description,
    units_per_box
FROM bsale.variants
WHERE description ILIKE '%SEC%'
ORDER BY description
LIMIT 100;
```

---

## Etapa B — Vistas de ventas y auxiliares

1. Ejecutar **ETAPA B** (vistas `vw_sales_base`, `vw_sales_7d`, `vw_sales_30d`, `vw_rotation`, `vw_costs`, `vw_stock`).

**Pruebas:**

```sql
SELECT COUNT(*) FROM bsale.vw_sales_30d;
SELECT COUNT(*) FROM bsale.vw_stock;
SELECT * FROM bsale.vw_costs LIMIT 5;
```

---

## Etapa C — `vw_purchase_analysis`

1. Ejecutar **ETAPA C** (recrea la vista).

**Verificar `status` (orden del `CASE`):**

1. `ventas_30_dias = 0` y `stock_actual > 0` → `NO_COMPRAR`
2. `unidades_a_comprar <= 0` → `NO_COMPRAR`
3. `unidades_a_comprar > 0` y `< units_per_box_eff` → `REVISAR`
4. `unidades_a_comprar >= units_per_box_eff` → `COMPRAR`
5. `ELSE` → `REVISAR` (**`status` no debería ser NULL**)

**Verificar columnas y ausencia de NULL en status:**

```sql
SELECT variant_id, units_per_box, units_per_box_eff, unidades_a_comprar, status
FROM bsale.vw_purchase_analysis
LIMIT 30;

SELECT COUNT(*) AS filas_status_null
FROM bsale.vw_purchase_analysis
WHERE status IS NULL;
-- Esperado: 0
```

---

## Etapa D — Tablas OC e ítems manuales

1. Ejecutar **ETAPA D** (`oc_document`, `oc_details`, `purchase_manual_items`, índices, `ALTER` idempotentes para `oc_id` / `consumed_at`).

**Nota:** `purchase_manual_items.oc_id` y `consumed_at` enlazan la fila manual con la OC al generar.

---

## Etapa E — Función `generate_purchase_order`

1. Ejecutar **ETAPA E**.

**Firma:**

| Parámetro           | Uso |
|---------------------|-----|
| `p_company_id`      | Obligatorio |
| `p_office_id`       | Obligatorio |
| `p_supplier_id`     | Obligatorio |
| `p_fecha_emision`   | Opcional; default `CURRENT_TIMESTAMP` |
| `p_fecha_entrega`   | Opcional |
| `p_forma_pago`      | Opcional |
| `p_responsable`     | Opcional |
| `p_observacion`     | Opcional |
| `p_manual_ids`      | Opcional; array de `id` de `purchase_manual_items` (misma empresa, sucursal y proveedor, `oc_id IS NULL`) |

**Ejemplo solo sugeridos automáticos (COMPRAR):**

```sql
SELECT bsale.generate_purchase_order(
    1,                    -- company_id
    1,                    -- office_id
    5,                    -- supplier_id
    NULL,                 -- fecha_emision → ahora
    DATE '2026-04-15',    -- fecha_entrega
    'Transferencia',      -- forma_pago
    'Juan Pérez',         -- responsable
    'Urgente',            -- observacion
    NULL                  -- sin manuales
);
```

**Ejemplo con manuales:**

```sql
SELECT bsale.generate_purchase_order(
    1, 1, 5,
    '2026-04-06 10:00:00+00'::timestamptz,
    DATE '2026-04-20',
    'Crédito',
    'Ana',
    NULL,
    ARRAY[10, 11]::bigint[]
);
```

**Comprobar snapshot en detalle:**

```sql
SELECT oc_id, product_type_name, product_name, variant_name, barcode,
       cantidad, units_per_box, cajas, costo_unitario, costo_total
FROM bsale.oc_details
WHERE oc_id = <oc_id_devuelto>;
```

**Ítems manuales consumidos:**

```sql
SELECT id, oc_id, consumed_at FROM bsale.purchase_manual_items WHERE oc_id IS NOT NULL;
```

---

## Orden resumido

1. Prerrequisitos (`suppliers`, datos Bsale sincronizados).
2. **A** → variants.
3. **B** → vistas base.
4. **C** → `vw_purchase_analysis`.
5. **D** → tablas + ALTER manuales.
6. **E** → función.
7. Pruebas SQL de las secciones anteriores.

---

## Actualizar solo la vista o la función

Tras cambios en el script, puede bastar con ejecutar desde **ETAPA C** hacia abajo (o `CREATE OR REPLACE` de la vista y función concretas).
