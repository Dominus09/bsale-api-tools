-- Diagnóstico OC 67019: por qué sigue pendiente si ya fue facturada.
-- Ejecutar en PostgreSQL (distribuidora + bsale). Ajustar :oc_number si aplica.

\set oc_number 67019

-- 1) ¿Existe la OC en documents?
SELECT
    '1_oc_document' AS step,
    d.document_id,
    d.number,
    d.document_type_id,
    dt.name AS document_type_name,
    d.emission_date,
    d.total_amount,
    d.client_id,
    d.state,
    d.updated_at
FROM distribuidora.documents d
LEFT JOIN distribuidora.document_types dt ON dt.id = d.document_type_id
WHERE d.document_type_id = 33
  AND d.number = :oc_number
ORDER BY d.document_id DESC;

-- 2) Detalles de la OC (líneas)
SELECT
    '2_oc_details' AS step,
    dd.id AS detail_id,
    dd.document_id,
    dd.line_number,
    dd.variant_id,
    dd.quantity,
    dd.net_unit_value,
    dd.total_amount
FROM distribuidora.document_details dd
JOIN distribuidora.documents d ON d.document_id = dd.document_id
WHERE d.document_type_id = 33
  AND d.number = :oc_number
ORDER BY dd.line_number;

-- 3) Relaciones confirmadas document_related (OC → boleta/factura 1/6)
SELECT
    '3_document_related' AS step,
    dr.id,
    dr.detail_id,
    dr.related_document_id,
    rd.number AS related_number,
    rd.document_type_id AS related_type_id,
    rdt.name AS related_type_name,
    rd.emission_date AS related_emission,
    rd.total_amount AS related_total,
    dr.created_at,
    dr.source
FROM distribuidora.document_related dr
JOIN distribuidora.document_details dd ON dd.id = dr.detail_id
JOIN distribuidora.documents d ON d.document_id = dd.document_id
LEFT JOIN distribuidora.documents rd ON rd.document_id = dr.related_document_id
LEFT JOIN distribuidora.document_types rdt ON rdt.id = rd.document_type_id
WHERE d.document_type_id = 33
  AND d.number = :oc_number
ORDER BY dr.id;

-- 4) Posibles facturas/boletas del mismo cliente cerca en fecha (sin related)
WITH oc AS (
    SELECT d.document_id, d.client_id, d.emission_date, d.total_amount
    FROM distribuidora.documents d
    WHERE d.document_type_id = 33 AND d.number = :oc_number
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
SELECT
    '5_probable_matches' AS step,
    pm.id,
    pm.oc_document_id,
    pm.candidate_document_id,
    pm.score,
    pm.match_reason,
    pm.created_at,
    cd.number AS candidate_number,
    cd.document_type_id AS candidate_type_id
FROM distribuidora.document_probable_matches pm
JOIN distribuidora.documents oc ON oc.document_id = pm.oc_document_id
LEFT JOIN distribuidora.documents cd ON cd.document_id = pm.candidate_document_id
WHERE oc.document_type_id = 33
  AND oc.number = :oc_number
ORDER BY pm.score DESC, pm.id DESC;

-- 6) Vista purchase status
SELECT
    '6_v_orders_purchase_status' AS step,
    v.*
FROM distribuidora.v_orders_purchase_status v
JOIN distribuidora.documents d ON d.document_id = v.document_id
WHERE d.document_type_id = 33
  AND d.number = :oc_number;

-- 7) Si la OC está en algún plan de despacho
SELECT
    '7_dispatch_plan_orders' AS step,
    dp.id AS plan_id,
    dp.planning_code,
    dp.status AS plan_status,
    dpo.oc_document_id,
    dpo.oc_number,
    dpo.route_order
FROM distribuidora.dispatch_plan_orders dpo
JOIN distribuidora.dispatch_plans dp ON dp.id = dpo.dispatch_plan_id
JOIN distribuidora.documents d ON d.document_id = dpo.oc_document_id
WHERE d.document_type_id = 33
  AND d.number = :oc_number
ORDER BY dp.id DESC;

-- 8) Vista facturación en plan (si aplica)
SELECT
    '8_v_dispatch_plan_invoiced_documents' AS step,
    vid.*
FROM distribuidora.v_dispatch_plan_invoiced_documents vid
JOIN distribuidora.documents d ON d.document_id = vid.oc_document_id
WHERE d.document_type_id = 33
  AND d.number = :oc_number;

-- 9) Cache purchase_document_status (si existe)
SELECT
    '9_purchase_status_cache' AS step,
    c.*
FROM distribuidora.purchase_document_status_cache c
JOIN distribuidora.documents d ON d.document_id = c.document_id
WHERE d.document_type_id = 33
  AND d.number = :oc_number;

-- 10) Sync related: última corrida / estado (si hay tabla sync)
SELECT
    '10_sync_status_related' AS step,
    ss.*
FROM distribuidora.sync_status ss
WHERE ss.branch ILIKE '%related%'
   OR ss.job_name ILIKE '%related%'
ORDER BY ss.last_run_at DESC NULLS LAST
LIMIT 5;
