-- PROPUESTA — NO EJECUTAR automáticamente.
-- BD objetivo: analytics (única).
-- Gestión de revisiones humanas de costos (distinto de alertas/detección automática).
-- Numeración 067: 064–066 reservados al diseño PWA (docs/auth/USERS_MULTIAPP_DESIGN.md).
-- No escribe en bsale_raw ni modifica costos de origen Bsale.
--
-- Historial: ON DELETE RESTRICT en events → no se puede borrar un caso con eventos.
-- La API no expone DELETE de revisiones ni UPDATE/DELETE de eventos (solo INSERT vía operaciones).
-- Inmutabilidad absoluta del historial NO está garantizada mientras la app se conecte a
-- PostgreSQL con un rol privilegiado/superusuario: un operador con SQL directo podría alterar filas.
-- Preferir desactivar cuentas de usuario (no DELETE físico) porque assignee_user_id /
-- created_by_user_id / actor_user_id usan ON DELETE SET NULL.

CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.cost_reviews (
  id                    bigserial PRIMARY KEY,
  company_id            integer NOT NULL REFERENCES bsale.companies (company_id),
  -- Nombre de empresa al crear (fuente backend); no depende de sesión del navegador.
  company_name          text NOT NULL,
  variant_id            integer NOT NULL,
  anomaly_code          text NOT NULL,
  reason_label          text NOT NULL,
  priority              text NOT NULL DEFAULT 'media'
                          CHECK (priority IN ('alta', 'media', 'baja')),
  status                text NOT NULL DEFAULT 'pendiente'
                          CHECK (status IN ('pendiente', 'en_revision', 'resuelto', 'descartado')),
  -- Instantánea al crear (sobrevive a re-análisis / sync Bsale).
  product_name          text NULL,
  sku_code              text NULL,
  barcode               text NULL,
  current_cost_ref      numeric NULL,
  previous_cost_ref     numeric NULL,
  change_pct_ref        numeric NULL,
  -- Asignación: solo al iniciar revisión (nunca desde el body del cliente).
  -- ON DELETE SET NULL: si se borra el usuario, se pierde el id; assignee_email conserva rastro.
  -- Preferir desactivación de cuentas sobre borrado físico.
  assignee_user_id      integer NULL REFERENCES bsale.users (id) ON DELETE SET NULL,
  assignee_email        text NULL,
  started_at            timestamptz NULL,
  resolved_at           timestamptz NULL,
  observations          text NULL,
  conclusion            text NULL,
  source                text NOT NULL DEFAULT 'reviews_grid'
                          CHECK (source IN (
                            'dashboard_alert',
                            'reception_analysis',
                            'reviews_grid',
                            'product_sheet'
                          )),
  source_ref            jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_by_user_id    integer NULL REFERENCES bsale.users (id) ON DELETE SET NULL,
  created_at            timestamptz NOT NULL DEFAULT now(),
  updated_at            timestamptz NOT NULL DEFAULT now(),
  -- Coherencia status ↔ assignee/started/resolved/conclusion (flujo real del servicio).
  -- en_revision: started_at obligatorio; assignee_user_id puede ser NULL solo tras borrado de usuario.
  -- Cierre: conclusión con al menos 5 caracteres no blancos.
  CONSTRAINT cost_reviews_status_coherence CHECK (
    (status = 'pendiente'
      AND assignee_user_id IS NULL
      AND started_at IS NULL
      AND resolved_at IS NULL
      AND conclusion IS NULL)
    OR (status = 'en_revision'
      AND started_at IS NOT NULL
      AND resolved_at IS NULL
      AND conclusion IS NULL)
    OR (status IN ('resuelto', 'descartado')
      AND resolved_at IS NOT NULL
      AND conclusion IS NOT NULL
      AND length(trim(conclusion)) >= 5)
  )
);

-- Una sola revisión ABIERTA por empresa + variante + tipo de anomalía.
CREATE UNIQUE INDEX IF NOT EXISTS uq_cost_reviews_open_case
  ON app.cost_reviews (company_id, variant_id, anomaly_code)
  WHERE status IN ('pendiente', 'en_revision');

CREATE INDEX IF NOT EXISTS idx_cost_reviews_company_status
  ON app.cost_reviews (company_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_cost_reviews_company_priority
  ON app.cost_reviews (company_id, priority, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_cost_reviews_company_variant
  ON app.cost_reviews (company_id, variant_id);

CREATE INDEX IF NOT EXISTS idx_cost_reviews_assignee
  ON app.cost_reviews (company_id, assignee_user_id, status)
  WHERE assignee_user_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS app.cost_review_events (
  id                 bigserial PRIMARY KEY,
  -- RESTRICT: no borrar físicamente una revisión que ya tiene historial.
  review_id          bigint NOT NULL REFERENCES app.cost_reviews (id) ON DELETE RESTRICT,
  actor_user_id      integer NULL REFERENCES bsale.users (id) ON DELETE SET NULL,
  actor_email        text NULL,
  action             text NOT NULL,
  status_from        text NULL,
  status_to          text NULL,
  note               text NULL,
  detail             jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_cost_review_events_review
  ON app.cost_review_events (review_id, created_at ASC, id ASC);

COMMENT ON TABLE app.cost_reviews IS
  'Casos de revisión humana de costos. Instantánea al crear; no altera Bsale. Sin DELETE en API.';
COMMENT ON TABLE app.cost_review_events IS
  'Historial append-only vía API (solo INSERT en TX). ON DELETE RESTRICT. Inmutabilidad absoluta no garantizada con rol DB privilegiado.';
COMMENT ON COLUMN app.cost_reviews.company_name IS
  'Nombre de empresa resuelto en backend al crear el caso.';
COMMENT ON COLUMN app.cost_reviews.current_cost_ref IS
  'Fotografía del costo comparable Costos V2 al crear; no se actualiza con syncs posteriores.';
COMMENT ON COLUMN app.cost_reviews.assignee_user_id IS
  'Asignado solo en start (JWT). Preferir desactivar usuarios; DELETE físico deja NULL por ON DELETE SET NULL.';
COMMENT ON COLUMN app.cost_reviews.conclusion IS
  'Obligatoria al cerrar; mínimo 5 caracteres no blancos (CHECK + FastAPI).';
