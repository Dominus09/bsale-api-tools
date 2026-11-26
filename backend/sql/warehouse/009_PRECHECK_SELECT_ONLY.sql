SELECT current_database() AS database_name, current_user AS db_user,
       to_regclass('warehouse.pallets') AS pallets,
       to_regclass('warehouse.pallet_lines') AS pallet_lines,
       to_regclass('warehouse.pallet_events') AS pallet_events;
SELECT conname,pg_get_constraintdef(oid) AS definition
FROM pg_constraint
WHERE conrelid=to_regclass('warehouse.pallet_events')
  AND conname='pallet_events_action_check';
SELECT 'pallets' AS tabla,count(*) FROM warehouse.pallets
UNION ALL SELECT 'pallet_lines',count(*) FROM warehouse.pallet_lines
UNION ALL SELECT 'pallet_events',count(*) FROM warehouse.pallet_events;
