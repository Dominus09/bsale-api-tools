-- =============================================================================
-- warehouse / 005 — VERIFY SELECT ONLY (post-migración)
-- No modifica datos. Ejecutar tras 001–004 en destino autorizado.
-- =============================================================================

SELECT current_database() AS db, current_user AS db_user, now() AS checked_at;

SELECT nspname
FROM pg_namespace
WHERE nspname = 'warehouse';

SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'warehouse'
ORDER BY table_name;

-- Índices de unicidad draft/published
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'warehouse'
  AND indexname IN (
    'uq_warehouse_one_draft_per_floor',
    'uq_warehouse_one_published_per_floor'
  )
ORDER BY indexname;

-- Columnas de publicación
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'warehouse'
  AND table_name = 'layout_revisions'
  AND column_name IN (
    'source_revision_id', 'archived_at', 'archived_by', 'publish_meta_json',
    'published_at', 'published_by', 'row_version', 'status'
  )
ORDER BY column_name;

-- Facility BC + oficina Bsale 1 (columna lógica; sin FK a bsale)
SELECT id, code, name, company_id, bsale_office_id, status
FROM warehouse.facilities
WHERE code = 'BC' AND company_id = 3;
-- Esperado: 1 fila; bsale_office_id = 1; status = active

-- Piso PISO1
SELECT fl.id, fl.code, fl.name, fl.level_index, fl.length_m, fl.width_m, fl.status
FROM warehouse.floors fl
JOIN warehouse.facilities f ON f.id = fl.facility_id
WHERE f.code = 'BC' AND fl.code = 'PISO1';
-- Esperado: 1 fila; length_m ≈ 23.10; width_m ≈ 13.70; status = active

-- Revisiones: draft presente; sin published automático del seed
SELECT f.code AS facility, fl.code AS floor, r.id AS revision_id, r.status, r.row_version, r.label
FROM warehouse.facilities f
JOIN warehouse.floors fl ON fl.facility_id = f.id
LEFT JOIN warehouse.layout_revisions r ON r.floor_id = fl.id
WHERE f.code = 'BC'
ORDER BY fl.code, r.status, r.id;
-- Esperado: al menos 1 revision status = 'draft'; COUNT published para BC = 0

SELECT pe.code, pe.element_type, pe.status
FROM warehouse.physical_elements pe
JOIN warehouse.floors fl ON fl.id = pe.floor_id
JOIN warehouse.facilities f ON f.id = fl.facility_id
WHERE f.code = 'BC'
  AND pe.code IN (
    'IZ', 'CIRC', 'DER', 'P1', 'P2', 'P3', 'P4', 'P5', 'P6', 'P7',
    'ESC', 'PORTON', 'OFC', 'BC-BND', 'IZ-SU', 'BAE-01', 'BAE-02'
  )
ORDER BY pe.code;
-- Esperado: las 17 filas presentes; IZ-SU element_type = loose_storage;
-- BAE-* element_type = position; status = active

-- Geometría en el draft (snaps)
SELECT COUNT(*) AS layout_elements_in_draft
FROM warehouse.layout_elements le
JOIN warehouse.layout_revisions r ON r.id = le.revision_id
JOIN warehouse.floors fl ON fl.id = r.floor_id
JOIN warehouse.facilities f ON f.id = fl.facility_id
WHERE f.code = 'BC' AND r.status = 'draft';
-- Esperado: >= 17 (seed 002+003)

-- Sin inventario ficticio (no hay tablas de stock en warehouse)
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'warehouse'
  AND table_name ILIKE ANY (ARRAY['%stock%', '%pallet%', '%inventory%', '%occupancy%']);
-- Esperado: 0 filas

-- Contar revisiones por estado (no >1 draft ni >1 published por piso)
SELECT floor_id, status, COUNT(*) AS n
FROM warehouse.layout_revisions
GROUP BY floor_id, status
ORDER BY floor_id, status;
-- Esperado: n = 1 para cada (floor_id, status) presente

-- Schemas ajenos: solo lectura de existencia (no tocar)
SELECT nspname FROM pg_namespace
WHERE nspname IN ('bsale', 'bsale_raw', 'distribuidora', 'app')
ORDER BY 1;
