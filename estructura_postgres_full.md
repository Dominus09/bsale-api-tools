# estructura_postgres_full.md

## Descripción
Documento completo de la estructura basada en los CSV proporcionados.
Incluye TODAS las tablas detectadas sin filtrar por lógica de negocio.

---

## SCHEMA: bsale

### Tabla: documents
- company_id
- bsale_id
- emission_date
- document_type_id
- office_id
- total_amount
- state

---

### Tabla: document_details
- company_id
- bsale_detail_id
- document_id
- variant_id
- quantity
- net_amount
- tax_amount
- total_amount

---

### Tabla: variants
- company_id
- bsale_id
- product_id
- barcode

---

### Tabla: products
- bsale_id
- name
- product_type_id

---

### Tabla: product_types
- id
- name

---

### Tabla: variant_cost
- variant_id
- cost

---

### Tabla: taxes
- id
- rate

---

### Tabla: stock
- company_id
- variant_id
- office_id
- stock_actual

---

## RELACIONES GENERALES

documents.bsale_id = document_details.document_id  
document_details.variant_id = variants.bsale_id  
variants.product_id = products.bsale_id  
products.product_type_id = product_types.id  
