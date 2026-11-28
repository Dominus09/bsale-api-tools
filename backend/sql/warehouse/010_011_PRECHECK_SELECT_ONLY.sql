-- SELECT ONLY — revisar destino y prerequisitos. No aplica cambios.
SELECT current_database() AS database_name, current_user AS actor;
SELECT to_regclass('warehouse.layout_elements') AS layout_elements,
       to_regclass('warehouse.pallets') AS pallets,
       to_regclass('warehouse.storage_plans') AS existing_plans;
SELECT f.code, fl.code AS floor_code, r.id, r.row_version, r.status,
       count(le.id) FILTER (WHERE le.element_type_snap='position') AS positions
FROM warehouse.facilities f
JOIN warehouse.floors fl ON fl.facility_id=f.id
LEFT JOIN warehouse.layout_revisions r ON r.floor_id=fl.id AND r.status IN ('draft','published')
LEFT JOIN warehouse.layout_elements le ON le.revision_id=r.id
WHERE f.company_id=3 AND f.code IN ('BC','BL')
GROUP BY f.code,fl.code,r.id,r.row_version,r.status ORDER BY f.code,r.id;
