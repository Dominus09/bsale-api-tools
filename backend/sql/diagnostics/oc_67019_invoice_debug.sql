-- Diagnóstico: ¿por qué una OC sigue pendiente si ya fue facturada?
-- Compatible con pgAdmin, DBeaver, Neon, etc. (SQL estándar, sin \set de psql).
--
-- Cambiar el número de OC solo en el CTE params de cada bloque (67019 → el que necesites).
-- Puede ejecutar el archivo completo o cada bloque por separado.
--
-- Tipos documento Bsale (sin join a bsale.document_types):
--   1 = Boleta, 6 = Factura, 33 = Orden de compra

-- 1) ¿Existe la OC en documents?
WITH params AS (SELECT 67019::bigint AS oc_number)
SELECT
    '1_oc_document' AS step,
    d.document_id,
    d.number,
    d.document_type_id,
    CASE d.document_type_id
        WHEN 1 THEN 'Boleta'
        WHEN 6 THEN 'Factura'
        WHEN 33 THEN 'Orden de compra'
        ELSE 'Tipo ' || d.document_type_id::text
    END AS document_type_name,
    d.emission_date,
    d.total_amount,
    d.client_id,
    d.state,
    d.updated_at
FROM params p
JOIN distribuidora.documents d
    ON d.document_type_id = 33
   AND d.number = p.oc_number
ORDER BY d.document_id DESC;

-- 2) Detalles de la OC (líneas)
WITH params AS (SELECT 67019::bigint AS oc_number)
SELECT
    '2_oc_details' AS step,
    dd.detail_id,
    dd.document_id,
    dd.line_number,
    dd.variant_id,
    dd.variant_code,
    dd.quantity,
    dd.net_unit_value,
    dd.total_amount,
    dd.related_detail_id
FROM params p
JOIN distribuidora.documents d
    ON d.document_type_id = 33
   AND d.number = p.oc_number
JOIN distribuidora.document_details dd ON dd.document_id = d.document_id
ORDER BY dd.line_number NULLS LAST, dd.detail_id;

-- 3) Relaciones confirmadas document_related (OC → boleta/factura 1/6)
WITH params AS (SELECT 67019::bigint AS oc_number)
SELECT
    '3_document_related' AS step,
    dr.id AS related_row_id,
    dr.detail_id,
    dr.related_document_id,
    dr.related_document_type,
    rd.number AS related_number,
    rd.document_type_id AS related_doc_type_id,
    CASE COALESCE(rd.document_type_id, dr.related_document_type)
        WHEN 1 THEN 'Boleta'
        WHEN 6 THEN 'Factura'
        WHEN 33 THEN 'Orden de compra'
        ELSE 'Tipo ' || COALESCE(rd.document_type_id, dr.related_document_type)::text
    END AS related_type_name,
    rd.emission_date AS related_emission,
    rd.total_amount AS related_total,
    dr.created_at
FROM params p
JOIN distribuidora.documents d
    ON d.document_type_id = 33
   AND d.number = p.oc_number
JOIN distribuidora.document_details dd ON dd.document_id = d.document_id
JOIN distribuidora.document_related dr ON dr.detail_id = dd.detail_id
LEFT JOIN distribuidora.documents rd ON rd.document_id = dr.related_document_id
ORDER BY dr.id;

-- 4) Posibles facturas/boletas del mismo cliente cerca en fecha (sin related)
WITH params AS (SELECT 67019::bigint AS oc_number),
oc AS (
    SELECT d.document_id, d.client_id, d.emission_date, d.total_amount
    FROM params p
    JOIN distribuidora.documents d
        ON d.document_type_id = 33
       AND d.number = p.oc_number
    LIMIT 1
)
SELECT
    '4_candidate_invoices' AS step,
    inv.document_id,
    inv.number,
    inv.document_type_id,
    inv.emission_date,
    inv.total_amount,
    ABS(inv.total_amount - oc.total_amount) AS amount_delta
FROM oc
JOIN distribuidora.documents inv
    ON inv.client_id = oc.client_id
   AND inv.document_type_id IN (1, 6)
   AND inv.emission_date >= oc.emission_date - interval '14 days'
   AND inv.emission_date <= oc.emission_date + interval '30 days'
ORDER BY amount_delta ASC, inv.emission_date DESC
LIMIT 20;

-- 5) Probable matches registrados
WITH params AS (SELECT 67019::bigint AS oc_number)
SELECT
    '5_probable_matches' AS step,
    pm.id,
    pm.oc_document_id,
    pm.candidate_document_id,
    pm.candidate_document_type,
    pm.score,
    pm.match_products_pct,
    pm.same_client,
    pm.same_seller,
    pm.same_day,
    pm.same_amount,
    pm.tracking_match,
    pm.created_at,
    cd.number AS candidate_number,
    cd.document_type_id AS candidate_doc_type_id
FROM params p
JOIN distribuidora.documents oc
    ON oc.document_type_id = 33
   AND oc.number = p.oc_number
JOIN distribuidora.document_probable_matches pm ON pm.oc_document_id = oc.document_id
LEFT JOIN distribuidora.documents cd ON cd.document_id = pm.candidate_document_id
ORDER BY pm.score DESC, pm.id DESC;

-- 6) Vista purchase status
WITH params AS (SELECT 67019::bigint AS oc_number)
SELECT
    '6_v_orders_purchase_status' AS step,
    v.*
FROM params p
JOIN distribuidora.documents d
    ON d.document_type_id = 33
   AND d.number = p.oc_number
JOIN distribuidora.v_orders_purchase_status v ON v.document_id = d.document_id;

-- 7) Si la OC está en algún plan de despacho (tabla: dispatch_plan, singular)
WITH params AS (SELECT 67019::bigint AS oc_number)
SELECT
    '7_dispatch_plan_orders' AS step,
    dp.id AS plan_id,
    dp.planning_code,
    dp.planning_name,
    dp.route_name,
    dp.status AS plan_status,
    dp.planning_date,
    dpo.oc_document_id,
    dpo.oc_number,
    dpo.route_order,
    dpo.oc_total_amount
FROM params p
JOIN distribuidora.documents d
    ON d.document_type_id = 33
   AND d.number = p.oc_number
JOIN distribuidora.dispatch_plan_orders dpo ON dpo.oc_document_id = d.document_id
JOIN distribuidora.dispatch_plan dp ON dp.id = dpo.dispatch_plan_id
ORDER BY dp.id DESC;

-- 8) Vista facturación en plan (si aplica)
WITH params AS (SELECT 67019::bigint AS oc_number)
SELECT
    '8_v_dispatch_plan_invoiced_documents' AS step,
    vid.*
FROM params p
JOIN distribuidora.documents d
    ON d.document_type_id = 33
   AND d.number = p.oc_number
JOIN distribuidora.v_dispatch_plan_invoiced_documents vid ON vid.oc_document_id = d.document_id;

-- 9) Cache purchase_document_status (si existe la tabla; PK = oc_document_id)
WITH params AS (SELECT 67019::bigint AS oc_number)
SELECT
    '9_purchase_status_cache' AS step,
    c.oc_document_id,
    c.oc_number,
    c.purchase_status,
    c.estado_real,
    c.is_invoiced_confirmed,
    c.invoicing_document_id,
    c.invoicing_number,
    c.probable_document_id,
    c.probable_number,
    c.probable_score,
    c.operational_status,
    c.relation_source,
    c.computed_at
FROM params p
JOIN distribuidora.documents d
    ON d.document_type_id = 33
   AND d.number = p.oc_number
JOIN distribuidora.purchase_document_status_cache c ON c.oc_document_id = d.document_id;

-- 10) Sync related: última corrida / estado (global, no depende del número OC)
SELECT
    '10_sync_status_related' AS step,
    ss.id,
    ss.sync_type,
    ss.last_run,
    ss.records_processed,
    ss.status,
    ss.created_at
FROM distribuidora.sync_status ss
WHERE ss.sync_type ILIKE '%related%'
ORDER BY ss.last_run DESC NULLS LAST
LIMIT 10;
