# estructura_postgres_bsale.md

## Descripción
Documento optimizado para desarrollo de módulo de compras y análisis.

---

## MODELO

documents → document_details → variants → products → product_types

+ costos (variant_cost + taxes)
+ stock

---

## TABLAS CLAVE

### documents
- company_id
- bsale_id
- emission_date
- document_type_id
- office_id

---

### document_details
- document_id
- variant_id
- quantity
- total_amount

---

### variants
- bsale_id
- product_id

---

### products
- bsale_id
- name
- product_type_id

---

### product_types
- id
- name

---

### variant_cost
- variant_id
- cost (NETO)

---

### taxes
- id
- rate

---

### stock
- variant_id
- office_id
- stock_actual

---

## REGLAS

- Usar SIEMPRE company_id en joins
- Usar SOLO document_type_id IN (1,6)
- Producto real = variant_id
- Costo real = cost * (1 + tax)

---

## QUERY BASE

SELECT
    dd.variant_id,
    SUM(dd.quantity) AS cantidad
FROM bsale.document_details dd
JOIN bsale.documents d 
  ON d.bsale_id = dd.document_id
 AND d.company_id = dd.company_id
WHERE d.document_type_id IN (1,6)
GROUP BY dd.variant_id;
