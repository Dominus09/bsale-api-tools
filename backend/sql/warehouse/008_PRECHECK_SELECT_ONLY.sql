-- Ejecutar ANTES de 008, solo lectura. No aplicar migracion si hay faltantes.
SELECT current_database() AS database_name, current_user AS db_user,
       current_setting('server_version') AS pg_version,
       has_schema_privilege(current_user,'warehouse','CREATE') AS warehouse_create;
SELECT to_regclass('warehouse.physical_elements') AS physical_elements,
       to_regclass('warehouse.layout_revisions') AS layout_revisions,
       to_regclass('warehouse.pallets') AS pallets,
       to_regclass('warehouse.pallet_lines') AS pallet_lines,
       to_regclass('warehouse.pallet_events') AS pallet_events;
SELECT table_name,column_name,data_type
FROM information_schema.columns
WHERE table_schema='bsale' AND (
  (table_name='variants' AND column_name IN ('company_id','bsale_id','product_id','description','bar_code','code','units_per_box'))
  OR (table_name='products' AND column_name IN ('company_id','bsale_id','name','product_type_id'))
  OR (table_name='product_types' AND column_name IN ('company_id','bsale_id','name'))
)
ORDER BY table_name,column_name;
SELECT f.code AS facility, fl.code AS floor, r.id AS revision_id,r.status,r.row_version,
       count(*) FILTER (WHERE l.element_type_snap='position') AS position_refs
FROM warehouse.facilities f
JOIN warehouse.floors fl ON fl.facility_id=f.id
JOIN warehouse.layout_revisions r ON r.floor_id=fl.id
LEFT JOIN warehouse.layout_elements l ON l.revision_id=r.id
WHERE f.company_id=3 AND f.code='BC' AND fl.code='PISO1' AND r.status IN ('draft','published')
GROUP BY f.code,fl.code,r.id,r.status,r.row_version
ORDER BY r.id;
