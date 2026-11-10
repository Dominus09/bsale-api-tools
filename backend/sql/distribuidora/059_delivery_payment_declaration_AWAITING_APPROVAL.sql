-- 059 — Forma de pago DECLARATIVA (sin montos) para PWA Operativa 2.0
-- ESTADO: AWAITING APPROVAL — NO aplicar en producción sin autorización.
--
-- Objetivo:
--   Permitir método `cash_and_transfer` (Efectivo + transferencia) en
--   distribuidora.delivery_collections.method.
--   Los montos (declared_amount / pending_balance) quedan NULL en el flujo
--   nuevo; las filas históricas con montos se conservan intactas.
--
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
        'cash_and_transfer'
    ));

COMMENT ON COLUMN distribuidora.delivery_collections.declared_amount IS
    'Monto declarado (legado). En flujo Operativa 2.0 declarativo permanece NULL.';

COMMENT ON COLUMN distribuidora.delivery_collections.method IS
    'Forma de pago declarada por el chofer. cash_and_transfer = efectivo+transf. informada.';

COMMIT;
