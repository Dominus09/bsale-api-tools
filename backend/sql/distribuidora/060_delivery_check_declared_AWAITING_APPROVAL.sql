-- 060 — Forma de pago: cheque declarado (check_declared)
-- ESTADO: AWAITING APPROVAL — NO aplicar en producción sin autorización.
--
-- Objetivo:
--   Ampliar CHECK de distribuidora.delivery_collections.method para aceptar
--   `check_declared` (Cheque recibido, declaración del chofer).
--
-- NO significa:
--   - cheque validado / compensado
--   - crédito aprobado
--   - autorización comercial automática
--
-- Conserva métodos previos (incl. cash_and_transfer de 059).
-- Idempotente. No toca Bsale, picking ni visitas cerradas.

BEGIN;

ALTER TABLE distribuidora.delivery_collections
    DROP CONSTRAINT IF EXISTS delivery_collections_method_check;

ALTER TABLE distribuidora.delivery_collections
    ADD CONSTRAINT delivery_collections_method_check
    CHECK (method IN (
        'cash',
        'transfer_declared',
        'transfer_verified',
        'credit',
        'partial',
        'pending',
        'not_applicable',
        'cash_and_transfer',
        'check_declared'
    ));

COMMENT ON COLUMN distribuidora.delivery_collections.method IS
    'Forma de pago declarada por el chofer. check_declared = cheque recibido (declaración; no validación).';

COMMIT;
