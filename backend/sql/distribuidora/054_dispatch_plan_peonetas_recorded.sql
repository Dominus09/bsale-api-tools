-- Aditivo — peonetas informadas vs valor por defecto histórico.
-- Reutiliza assistant_count (INTEGER NOT NULL DEFAULT 0, CHECK 0..10 en 021).
-- peonetas_recorded=FALSE + assistant_count=0 ⇒ "No informado" (histórico / ORS sin dato).
-- peonetas_recorded=TRUE  + assistant_count=N ⇒ cantidad explícita (incl. 0).
-- Aplicar SOLO vía apply_distribuidora_schema o psql manual. No tocar analytics aquí.

ALTER TABLE distribuidora.dispatch_plan
    ADD COLUMN IF NOT EXISTS peonetas_recorded BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN distribuidora.dispatch_plan.peonetas_recorded IS
  'TRUE cuando la cantidad de peonetas (assistant_count) fue registrada explícitamente '
  'al confirmar (p. ej. planificación simplificada). FALSE conserva el semántica histórica: '
  'assistant_count=0 no implica "cero peonetas informadas".';
