-- Vistas de negocio Distribuidora. Separador -- +go

CREATE OR REPLACE VIEW distribuidora.v_oc_attributes_flat AS
SELECT
    da.document_id,
    MAX(da.attribute_value) FILTER (
        WHERE upper(btrim(da.attribute_name)) = upper(btrim('TIPO DE DOCUMENTO A GENERAR'))
    ) AS tipo_documento_a_generar,
    MAX(da.attribute_value) FILTER (
        WHERE upper(btrim(da.attribute_name)) = upper(btrim('NOMBRE FANTASIA'))
    ) AS nombre_fantasia,
    MAX(da.attribute_value) FILTER (
        WHERE upper(btrim(da.attribute_name)) = upper(btrim('FORMA DE PAGO'))
    ) AS forma_pago,
    MAX(da.attribute_value) FILTER (
        WHERE upper(btrim(da.attribute_name)) = upper(btrim('OBSERVACIONES'))
    ) AS observaciones
FROM distribuidora.document_attributes da
INNER JOIN distribuidora.documents d ON d.document_id = da.document_id
WHERE d.company_id = 3
  AND d.office_id = 1
  AND d.document_type_id = 33
GROUP BY da.document_id
-- +go

CREATE OR REPLACE VIEW distribuidora.v_orders_purchase AS
SELECT
    d.document_id,
    d.number,
    d.client_id,
    d.user_id,
    d.emission_date,
    d.total_amount,
    d.municipality,
    d.city,
    d.address,
    a.tipo_documento_a_generar,
    a.nombre_fantasia,
    a.forma_pago,
    a.observaciones
FROM distribuidora.documents d
LEFT JOIN distribuidora.v_oc_attributes_flat a ON a.document_id = d.document_id
WHERE d.company_id = 3
  AND d.office_id = 1
  AND d.document_type_id = 33
-- +go

CREATE OR REPLACE VIEW distribuidora.v_orders_purchase_status AS
SELECT
    oc.document_id,
    oc.number,
    (inv.document_id IS NOT NULL) AS is_invoiced,
    inv.document_id AS invoicing_document_id,
    inv.document_type_id AS invoicing_document_type_id,
    inv.number AS invoicing_number,
    inv.emission_date AS invoicing_emission_date
FROM distribuidora.documents oc
LEFT JOIN LATERAL (
    SELECT d.document_id, d.document_type_id, d.number, d.emission_date
    FROM distribuidora.document_references dr
    INNER JOIN distribuidora.documents d
        ON d.document_id = dr.source_document_id
       AND d.document_type_id IN (1, 6)
       AND d.company_id = oc.company_id
       AND d.office_id = oc.office_id
    WHERE dr.reference_number IS NOT DISTINCT FROM oc.number
      AND (dr.reference_document_type_id IS NULL OR dr.reference_document_type_id = 33)
    ORDER BY d.emission_date DESC NULLS LAST, d.document_id DESC
    LIMIT 1
) inv ON TRUE
WHERE oc.company_id = 3
  AND oc.office_id = 1
  AND oc.document_type_id = 33
-- +go

CREATE OR REPLACE VIEW distribuidora.v_orders_purchase_enriched AS
SELECT
    p.document_id,
    p.number,
    p.client_id,
    p.user_id,
    p.emission_date,
    p.total_amount,
    p.municipality,
    p.city,
    p.address,
    p.tipo_documento_a_generar,
    p.nombre_fantasia,
    p.forma_pago,
    p.observaciones,
    s.is_invoiced,
    s.invoicing_document_id,
    s.invoicing_document_type_id,
    s.invoicing_number,
    s.invoicing_emission_date
FROM distribuidora.v_orders_purchase p
LEFT JOIN distribuidora.v_orders_purchase_status s ON s.document_id = p.document_id
-- +go
