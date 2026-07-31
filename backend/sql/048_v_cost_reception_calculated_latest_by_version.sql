-- PROPUESTA — NO ejecutar automáticamente.
--
-- Problema (auditoría Etapa E.5):
--   analytics.v_cost_reception_calculated_latest usa
--     DISTINCT ON (history_id) ORDER BY history_id, calculated_at DESC, id DESC
--   Es "última corrida temporal", NO "última por calculation_version".
--
-- Riesgo:
--   Si existe cost-v2.1.0 más reciente para el mismo history_id, la vista
--   oculta cost-v2.0.0. Filtrar la vista con calculation_version='cost-v2.0.0'
--   devolvería 0 filas para ese history_id.
--
-- Semántica correcta para API V2 pinneada a una versión:
--   UNIQUE (history_id, calculation_version) en la tabla ⇒ a lo más una fila
--   por history_id + versión. La API E.5 consulta la TABLA con filtro de versión.
--
-- Esta vista propuesta permitiría "latest por versión" si en el futuro se
-- relajara el UNIQUE o se versionara por batch sin upsert.

CREATE OR REPLACE VIEW analytics.v_cost_reception_calculated_latest_by_version AS
SELECT
    c.history_id,
    c.calculation_version,
    c.calculation_batch_id,
    c.company_id,
    c.office_id,
    c.variant_id,
    c.admission_date,
    c.stored_cost_net,
    c.stored_quantity,
    c.stored_iva_amount,
    c.stored_other_taxes,
    c.stored_gross_cost,
    c.reception_tax_ids_json,
    c.catalog_tax_ids_json,
    c.resolved_tax_ids_json,
    c.iva_tax_id,
    c.iva_rate,
    c.calculated_iva_amount,
    c.additional_taxes_json,
    c.additional_tax_rate_total,
    c.additional_tax_amount_total,
    c.total_tax_rate,
    c.corrected_gross_cost,
    c.gross_difference_amount,
    c.tax_rate_on_net_pct,
    c.gross_understatement_vs_corrected_pct,
    c.tax_context_source,
    c.tax_ids_source,
    c.tax_rates_source,
    c.tax_context_as_of,
    c.tax_context_is_historical,
    c.tax_context_fingerprint,
    c.tax_resolution_quality,
    c.effective_quality_status,
    c.warnings_json,
    c.source_history_created_at,
    c.source_history_fingerprint,
    c.calculation_result_fingerprint,
    c.calculated_at,
    h.document_number,
    h.document,
    h.reception_id,
    h.barcode AS history_barcode,
    h.product_name AS history_product_name,
    h.variant_name AS history_variant_name,
    h.created_at AS history_created_at
FROM (
    SELECT DISTINCT ON (history_id, calculation_version)
        *
    FROM analytics.cost_reception_calculated
    ORDER BY history_id, calculation_version, calculated_at DESC, id DESC
) c
INNER JOIN analytics.cost_reception_history h
    ON h.id = c.history_id;

COMMENT ON VIEW analytics.v_cost_reception_calculated_latest_by_version IS
    'Latest por (history_id, calculation_version). Distinto de v_cost_reception_calculated_latest '
    '(latest temporal global). Propuesta E.5 — no aplicada automáticamente.';
