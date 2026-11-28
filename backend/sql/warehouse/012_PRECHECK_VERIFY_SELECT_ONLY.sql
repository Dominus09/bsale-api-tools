-- SELECT ONLY. Antes y después de 012.
SELECT current_database() AS database_name, current_user AS actor;
SELECT f.code,fl.id AS floor_id,r.id AS revision_id,r.status,r.row_version,
       count(*) FILTER(WHERE le.code_snap ~ '^BL-F(0[1-9]|1[0-2])-0[1-5]$') AS legacy_cells,
       count(*) FILTER(WHERE le.code_snap ~ '^BL(0[1-9]|1[0-2])[A-E]$') AS short_cells,
       count(*) FILTER(WHERE le.element_type_snap='position') AS positions,
       count(*) FILTER(WHERE le.code_snap IN ('BL-F01-01','BL01A') AND le.element_type_snap='unusable') AS dead_space,
       count(*) FILTER(WHERE le.code_snap IN ('BL-F01-05','BL01E') AND le.element_type_snap='position') AS furniture_position
FROM warehouse.facilities f JOIN warehouse.floors fl ON fl.facility_id=f.id
JOIN warehouse.layout_revisions r ON r.floor_id=fl.id
JOIN warehouse.layout_elements le ON le.revision_id=r.id
WHERE f.company_id=3 AND f.code='BL' AND fl.code='PISO1' AND r.status IN('draft','published')
GROUP BY f.code,fl.id,r.id,r.status,r.row_version ORDER BY r.id;
SELECT a.created_at,a.detail_json FROM warehouse.layout_audit a
JOIN warehouse.facilities f ON f.id=a.facility_id
WHERE f.company_id=3 AND f.code='BL' AND a.action='bl_short_codes_012'
ORDER BY a.created_at DESC;
