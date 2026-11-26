-- PROPOSAL. Do not apply automatically.
-- Target analytics; verify actual ERP/API DB and obtain approval before execution.
BEGIN;

ALTER TABLE bsale.products_master ADD COLUMN IF NOT EXISTS boxes_per_pallet integer;
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'bsale.products_master'::regclass
      AND conname = 'products_master_boxes_per_pallet_positive'
  ) THEN
    ALTER TABLE bsale.products_master ADD CONSTRAINT products_master_boxes_per_pallet_positive
      CHECK (boxes_per_pallet IS NULL OR boxes_per_pallet > 0);
  END IF;
END $$;
COMMENT ON COLUMN bsale.products_master.boxes_per_pallet IS
  'Verified positive count of boxes per pallet; NULL if unknown. Manual overlay, not Bsale sync.';

CREATE SCHEMA IF NOT EXISTS app;
CREATE TABLE IF NOT EXISTS app.product_master_pallet_audit (
  id bigserial PRIMARY KEY,
  actor_user_id bigint NOT NULL,
  products_master_id bigint NOT NULL,
  variant_id bigint,
  barcode text NOT NULL,
  before_boxes integer,
  after_boxes integer,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS product_master_pallet_audit_product_idx
  ON app.product_master_pallet_audit (products_master_id, created_at DESC);

CREATE TABLE IF NOT EXISTS app.catalog_photo_audit (
  audit_id text PRIMARY KEY CHECK (audit_id ~ '^[0-9a-f]{32}$'),
  actor_user_id bigint NOT NULL,
  products_master_id bigint NOT NULL,
  barcode text NOT NULL,
  catalog_variant_id bigint NOT NULL,
  previous_sha text,
  prepared_sha256 text NOT NULL,
  new_sha text,
  commit_sha text,
  status text NOT NULL CHECK (status IN ('started','published','needs_review')),
  error_status integer,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS catalog_photo_audit_product_idx
  ON app.catalog_photo_audit (products_master_id, created_at DESC);
COMMIT;
