-- =============================================================================
-- bsale_raw 009 — seed de sources (paso separado, idempotente)
--
-- cpnId observados en fase 2 (credential.bsale.io). token_env = NOMBRE de la variable de
-- entorno (igual a bsale.companies.bsale_token); los tokens reales NO se guardan.
-- Re-ejecutable: no sobrescribe filas existentes.
-- =============================================================================

BEGIN;

INSERT INTO bsale_raw.sources (company_id, cpn_id, name, token_env, active)
VALUES
    (1, 96674, 'Minimarkets La Quillotana', 'BSALE_TOKEN_Mini', true),
    (2, 5807, 'Carlos Romero', 'BSALE_TOKEN_Romero', true),
    (3, 21884, 'La Quillotana SPA', 'BSALE_TOKEN_SPA', true)
ON CONFLICT (company_id) DO NOTHING;

COMMIT;
