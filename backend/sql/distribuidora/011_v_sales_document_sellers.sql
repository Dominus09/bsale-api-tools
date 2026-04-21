-- ``v_sales``: incorpora vendedor desde ``document_sellers`` (primera fila por ``id``) con fallback a ``documents``.
-- Requiere ``010_document_sellers.sql``. Misma semántica que ``009_v_sales_with_credit_notes.sql``.

DROP VIEW IF EXISTS distribuidora.v_sales CASCADE;
-- +go

CREATE OR REPLACE VIEW distribuidora.v_sales AS
SELECT
    b.document_id,
    b.number,
    b.emission_date,
    b.document_type_id,
    b.state,
    b.client_id,
    b.total_amount_net,
    b.total_amount_sales,
    b.is_sale,
    b.total_amount_net AS total_amount,
    b.client_name,
    b.municipality,
    b.seller_name,
    b.seller_id
FROM (
    SELECT
        d.document_id,
        d.number,
        d.emission_date,
        d.document_type_id,
        d.state,
        d.client_id,
        CASE
            WHEN d.document_type_id = 9 THEN -ABS(COALESCE(d.total_amount, 0::numeric))
            ELSE COALESCE(d.total_amount, 0::numeric)
        END AS total_amount_net,
        CASE
            WHEN d.document_type_id IN (1, 6) THEN COALESCE(d.total_amount, 0::numeric)
            ELSE 0::numeric
        END AS total_amount_sales,
        CASE
            WHEN d.document_type_id IN (1, 6) THEN 1
            ELSE 0
        END::integer AS is_sale,
        COALESCE(
            NULLIF(
                TRIM(
                    CONCAT_WS(
                        ' ',
                        NULLIF(TRIM(c.company), ''),
                        NULLIF(TRIM(c.first_name), ''),
                        NULLIF(TRIM(c.last_name), '')
                    )
                ),
                ''
            ),
            'Cliente ' || COALESCE(d.client_id::text, '0')
        ) AS client_name,
        COALESCE(
            NULLIF(TRIM(d.municipality), ''),
            NULLIF(TRIM(c.municipality), '')
        ) AS municipality,
        COALESCE(
            NULLIF(TRIM(ds.seller_name), ''),
            NULLIF(TRIM(d.seller_name), ''),
            'Sin vendedor'
        ) AS seller_name,
        COALESCE(ds.seller_id, d.seller_id) AS seller_id
    FROM distribuidora.v_documents_latest d
    LEFT JOIN bsale.clients c
        ON c.company_id = d.company_id
       AND c.bsale_id = d.client_id
    LEFT JOIN LATERAL (
        SELECT s.seller_id, s.seller_name
        FROM distribuidora.document_sellers s
        WHERE s.document_id = d.document_id
        ORDER BY s.id ASC
        LIMIT 1
    ) ds ON TRUE
    WHERE d.company_id = 3
      AND d.office_id = 1
      AND d.document_type_id IN (1, 6, 9)
      AND COALESCE(d.state, 0) = 0
) b;
-- +go
