-- Fase 2B. Aplicación MANUAL con respaldo y autorización expresa.
-- Sólo amplía el tipo de evento de auditoría de pallets. No toca registros ni Bsale.
BEGIN;
SET LOCAL lock_timeout = '5s';
DO $guard$
DECLARE def text;
BEGIN
  IF current_database() <> 'analytics' AND NOT (
    left(current_database(),length('warehouse_pallets_test')) = 'warehouse_pallets_test'
    AND COALESCE(current_setting('warehouse.pallets_lab',true),'off')='on'
  ) THEN
    RAISE EXCEPTION 'Destino no autorizado para warehouse 009: %', current_database();
  END IF;
  IF to_regclass('warehouse.pallets') IS NULL OR to_regclass('warehouse.pallet_lines') IS NULL
     OR to_regclass('warehouse.pallet_events') IS NULL THEN
    RAISE EXCEPTION 'Falta migración warehouse 008';
  END IF;
  SELECT pg_get_constraintdef(oid) INTO def FROM pg_constraint
  WHERE conrelid='warehouse.pallet_events'::regclass
    AND contype='c' AND conname='pallet_events_action_check';
  IF def IS NULL OR strpos(def,'registered')=0 OR strpos(def,'location_observed')=0 OR strpos(def,'voided')=0 THEN
    RAISE EXCEPTION 'Restricción de eventos no esperada. Verificar manualmente.';
  END IF;
  IF strpos(def,'details_updated')=0 THEN
    ALTER TABLE warehouse.pallet_events DROP CONSTRAINT pallet_events_action_check;
    ALTER TABLE warehouse.pallet_events ADD CONSTRAINT pallet_events_action_check
      CHECK (action IN ('registered','location_observed','voided','details_updated'));
  END IF;
END
$guard$;
COMMIT;
