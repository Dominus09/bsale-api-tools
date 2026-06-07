/* Trazabilidad OC 67019
   OC: document_id 3773583, number 67019
   Boleta: document_id 3778188, number 2634708

   Ejecutar TODO el archivo en pgAdmin (F5), desde la linea 1.
   Cambiar IDs en el SELECT ... INTO TEMP mas abajo si diagnostica otra OC.

   El NOTICE "table does not exist, skipping" en la 1ra corrida es normal
   (DROP IF EXISTS). Se suprime con client_min_messages abajo.
*/

SET client_min_messages TO WARNING;

DROP TABLE IF EXISTS _oc_trace_params;

SELECT
    3773583::bigint AS oc_document_id,
    67019::bigint AS oc_number,
    3778188::bigint AS candidate_document_id,
    2634708::bigint AS candidate_number,
    3::int AS default_window_days
INTO TEMP _oc_trace_params;

SET client_min_messages TO NOTICE;

-- A) Cabecera OC
SELECT 'A_oc_header' AS step, d.*
FROM _oc_trace_params p
JOIN distribuidora.documents d ON d.document_id = p.oc_document_id;

-- A) Cabecera boleta candidata
SELECT 'A_candidate_header' AS step, d.*
FROM _oc_trace_params p
JOIN distribuidora.documents d ON d.document_id = p.candidate_document_id;

-- B) Lineas OC (detail_id es distinto de document_id)
SELECT
    'B_oc_detail_ids' AS step,
    dd.detail_id,
    dd.document_id,
    dd.line_number,
    dd.variant_id,
    dd.variant_code,
    dd.quantity,
    dd.total_amount,
    dd.related_detail_id
FROM _oc_trace_params p
JOIN distribuidora.document_details dd ON dd.document_id = p.oc_document_id
ORDER BY dd.line_number NULLS LAST, dd.detail_id;

-- C) document_related actual
SELECT
    'C_document_related' AS step,
    dr.id AS related_row_id,
    dr.detail_id,
    dd.document_id AS oc_document_id,
    dr.related_document_id,
    dr.related_document_type,
    rd.number AS related_number,
    rd.document_type_id AS related_doc_type_id,
    dr.created_at
FROM _oc_trace_params p
JOIN distribuidora.document_details dd ON dd.document_id = p.oc_document_id
LEFT JOIN distribuidora.document_related dr ON dr.detail_id = dd.detail_id
LEFT JOIN distribuidora.documents rd ON rd.document_id = dr.related_document_id
ORDER BY dr.id NULLS LAST;

-- D) Candidato 3778188 enlazado a alguna OC?
SELECT
    'D_candidate_in_related' AS step,
    dr.*
FROM _oc_trace_params p
JOIN distribuidora.document_related dr ON dr.related_document_id = p.candidate_document_id;

-- E) Ventana matcher default (+/- 3 dias)
SELECT
    'E_candidate_in_default_window' AS step,
    p.default_window_days AS window_days,
    oc.emission_date AS oc_emission,
    cand.emission_date AS candidate_emission,
    (cand.emission_date::date - oc.emission_date::date) AS delta_days,
    (
        cand.emission_date::date >= (oc.emission_date::date - p.default_window_days)
        AND cand.emission_date::date <= (oc.emission_date::date + p.default_window_days)
    ) AS in_default_window,
    (
        cand.emission_date::date >= (oc.emission_date::date - 7)
        AND cand.emission_date::date <= (oc.emission_date::date + 7)
    ) AS in_window_7d
FROM _oc_trace_params p
JOIN distribuidora.documents oc ON oc.document_id = p.oc_document_id
JOIN distribuidora.documents cand ON cand.document_id = p.candidate_document_id;

-- F) Candidatos que el matcher evalua (ventana default)
SELECT
    'F_matcher_candidates_default_window' AS step,
    d.document_id,
    d.number,
    d.document_type_id,
    d.emission_date,
    d.total_amount,
    ABS(d.total_amount - oc.total_amount) AS amount_delta
FROM _oc_trace_params p
JOIN distribuidora.documents oc ON oc.document_id = p.oc_document_id
JOIN distribuidora.documents d
    ON d.company_id = oc.company_id
   AND d.office_id = oc.office_id
   AND d.document_type_id IN (1, 6)
   AND d.client_id = oc.client_id
   AND d.document_id <> oc.document_id
   AND d.emission_date::date >= (oc.emission_date::date - p.default_window_days)
   AND d.emission_date::date <= (oc.emission_date::date + p.default_window_days)
ORDER BY d.emission_date DESC, d.document_id DESC;

-- G) Probable matches persistidos para la OC
SELECT
    'G_probable_matches' AS step,
    pm.*,
    cd.number AS candidate_number
FROM _oc_trace_params p
JOIN distribuidora.document_probable_matches pm ON pm.oc_document_id = p.oc_document_id
LEFT JOIN distribuidora.documents cd ON cd.document_id = pm.candidate_document_id
ORDER BY pm.score DESC;

-- H) Match especifico OC -> 3778188
SELECT
    'H_probable_match_target' AS step,
    pm.*
FROM _oc_trace_params p
LEFT JOIN distribuidora.document_probable_matches pm
    ON pm.oc_document_id = p.oc_document_id
   AND pm.candidate_document_id = p.candidate_document_id;

-- I) Lineas OC
SELECT
    'I_line_compare_oc' AS step,
    dd.variant_id,
    dd.quantity,
    dd.variant_code
FROM _oc_trace_params p
JOIN distribuidora.document_details dd ON dd.document_id = p.oc_document_id
ORDER BY dd.line_number NULLS LAST;

-- I) Lineas boleta candidata
SELECT
    'I_line_compare_candidate' AS step,
    dd.variant_id,
    dd.quantity,
    dd.variant_code
FROM _oc_trace_params p
JOIN distribuidora.document_details dd ON dd.document_id = p.candidate_document_id
ORDER BY dd.line_number NULLS LAST;

-- J) Estado confirmado (solo document_related)
SELECT
    'J_v_orders_purchase_status' AS step,
    v.*
FROM _oc_trace_params p
JOIN distribuidora.v_orders_purchase_status v ON v.document_id = p.oc_document_id;

-- K) Estado full (confirmada + probable)
SELECT
    'K_v_purchase_document_status_full' AS step,
    v.oc_document_id,
    v.oc_number,
    v.status,
    v.estado_real,
    v.is_invoiced_confirmed,
    v.invoicing_document_id,
    v.invoicing_number,
    v.candidate_document_id,
    v.candidate_number,
    v.score,
    v.match_products_pct,
    v.same_client,
    v.same_amount,
    v.same_day
FROM _oc_trace_params p
JOIN distribuidora.v_purchase_document_status_full v ON v.oc_document_id = p.oc_document_id;

-- L) Cache denormalizada
SELECT
    'L_purchase_status_cache' AS step,
    c.*
FROM _oc_trace_params p
LEFT JOIN distribuidora.purchase_document_status_cache c ON c.oc_document_id = p.oc_document_id;

-- M) Ultimo sync related
SELECT
    'M_sync_status_related' AS step,
    ss.sync_type,
    ss.last_run,
    ss.records_processed,
    ss.status
FROM distribuidora.sync_status ss
WHERE ss.sync_type ILIKE '%related%'
ORDER BY ss.last_run DESC NULLS LAST
LIMIT 5;
