-- =============================================================================
-- warehouse / 003 — Zonas especiales BC (IZ-SU, BAE-01/02) — aditivo
-- AWAITING_APPROVAL — NO producción sin autorización.
--
-- Solo inserta si el código activo aún no existe.
-- Geometrías PROVISIONALES (pending_validation). No sobrescribe borradores
-- que ya tengan esos physical_element_id en layout_elements.
-- =============================================================================

BEGIN;

DO $$
DECLARE
    v_facility_id BIGINT;
    v_floor_id BIGINT;
    v_rev_id BIGINT;
    v_actor TEXT := 'seed:warehouse_003';
    v_iz BIGINT;
    v_esc BIGINT;
    v_bnd BIGINT;
    v_su BIGINT;
    v_bae1 BIGINT;
    v_bae2 BIGINT;
    v_has INTEGER;
BEGIN
    SELECT id INTO v_facility_id
    FROM warehouse.facilities
    WHERE company_id = 3 AND code = 'BC'
    LIMIT 1;
    IF v_facility_id IS NULL THEN
        RAISE NOTICE '003: facility BC no existe — ejecutar 002 primero';
        RETURN;
    END IF;

    SELECT id INTO v_floor_id
    FROM warehouse.floors
    WHERE facility_id = v_facility_id AND code = 'PISO1'
    LIMIT 1;
    IF v_floor_id IS NULL THEN
        RAISE NOTICE '003: floor PISO1 no existe';
        RETURN;
    END IF;

    SELECT id INTO v_rev_id
    FROM warehouse.layout_revisions
    WHERE floor_id = v_floor_id AND status = 'draft'
    LIMIT 1;
    IF v_rev_id IS NULL THEN
        RAISE NOTICE '003: draft no existe';
        RETURN;
    END IF;

    SELECT id INTO v_bnd FROM warehouse.physical_elements
    WHERE floor_id = v_floor_id AND status = 'active' AND code = 'BC-BND';
    SELECT id INTO v_iz FROM warehouse.physical_elements
    WHERE floor_id = v_floor_id AND status = 'active' AND code = 'IZ';
    SELECT id INTO v_esc FROM warehouse.physical_elements
    WHERE floor_id = v_floor_id AND status = 'active' AND code = 'ESC';

    -- IZ-SU: zona sueltos ~2,67 m² (≈ 2,52 × 1,06). Extremo oeste de IZ (provisional).
    SELECT id INTO v_su FROM warehouse.physical_elements
    WHERE floor_id = v_floor_id AND status = 'active' AND code = 'IZ-SU';
    IF v_su IS NULL THEN
        INSERT INTO warehouse.physical_elements (
            facility_id, floor_id, parent_id, element_type, code, name, meta_json,
            created_by, updated_by
        ) VALUES (
            v_facility_id, v_floor_id, v_iz, 'loose_storage', 'IZ-SU',
            'Zona productos sueltos (IZ)',
            jsonb_build_object(
                'storage_kind', 'loose_no_pallet',
                'approx_area_m2', 2.67,
                'not_pallet_positions', true,
                'nomenclature', 'IZ-SU = sector izquierdo, productos sueltos'
            ),
            v_actor, v_actor
        )
        RETURNING id INTO v_su;
    END IF;

    SELECT COUNT(*) INTO v_has FROM warehouse.layout_elements
    WHERE revision_id = v_rev_id AND physical_element_id = v_su;
    IF v_has = 0 THEN
        INSERT INTO warehouse.layout_elements (
            revision_id, physical_element_id,
            code_snap, name_snap, element_type_snap, parent_physical_id_snap, physical_meta_snap,
            x_m, y_m, elevation_m, width_m, depth_m, height_clear_m, rotation_deg,
            geom_type, validation_status, constraints_json, observations, meta_json
        ) VALUES (
            v_rev_id, v_su,
            'IZ-SU', 'Zona productos sueltos (IZ)', 'loose_storage', v_iz,
            jsonb_build_object('storage_kind', 'loose_no_pallet', 'approx_area_m2', 2.67),
            0, 0, 0, 2.52, 1.06, NULL, 0, 'rect', 'pending_validation',
            jsonb_build_object('storage_kind', 'loose_no_pallet', 'stacking_authorized', false),
            'Área ≈2,67 m² sin pallet. Forma exacta pendiente de validación. No son dos posiciones de pallet.',
            jsonb_build_object('provisional', true, 'storage_kind', 'loose_no_pallet')
        );
    END IF;

    -- BAE-01 / BAE-02: candidatos bajo escalera (junto ESC). Altura variable.
    SELECT id INTO v_bae1 FROM warehouse.physical_elements
    WHERE floor_id = v_floor_id AND status = 'active' AND code = 'BAE-01';
    IF v_bae1 IS NULL THEN
        INSERT INTO warehouse.physical_elements (
            facility_id, floor_id, parent_id, element_type, code, name, meta_json,
            created_by, updated_by
        ) VALUES (
            v_facility_id, v_floor_id, COALESCE(v_esc, v_bnd), 'position', 'BAE-01',
            'Bajo escalera 1 (candidato)',
            jsonb_build_object(
                'storage_kind', 'special_restricted',
                'under_stair', true,
                'safe_storage_authorized', false,
                'nomenclature', 'BAE = bajo escalera'
            ),
            v_actor, v_actor
        )
        RETURNING id INTO v_bae1;
    END IF;

    SELECT id INTO v_bae2 FROM warehouse.physical_elements
    WHERE floor_id = v_floor_id AND status = 'active' AND code = 'BAE-02';
    IF v_bae2 IS NULL THEN
        INSERT INTO warehouse.physical_elements (
            facility_id, floor_id, parent_id, element_type, code, name, meta_json,
            created_by, updated_by
        ) VALUES (
            v_facility_id, v_floor_id, COALESCE(v_esc, v_bnd), 'position', 'BAE-02',
            'Bajo escalera 2 (candidato)',
            jsonb_build_object(
                'storage_kind', 'special_restricted',
                'under_stair', true,
                'safe_storage_authorized', false,
                'nomenclature', 'BAE = bajo escalera'
            ),
            v_actor, v_actor
        )
        RETURNING id INTO v_bae2;
    END IF;

    SELECT COUNT(*) INTO v_has FROM warehouse.layout_elements
    WHERE revision_id = v_rev_id AND physical_element_id = v_bae1;
    IF v_has = 0 THEN
        INSERT INTO warehouse.layout_elements (
            revision_id, physical_element_id,
            code_snap, name_snap, element_type_snap, parent_physical_id_snap, physical_meta_snap,
            x_m, y_m, elevation_m, width_m, depth_m, height_clear_m, rotation_deg,
            geom_type, validation_status, constraints_json, observations, meta_json
        ) VALUES (
            v_rev_id, v_bae1,
            'BAE-01', 'Bajo escalera 1 (candidato)', 'position', COALESCE(v_esc, v_bnd),
            jsonb_build_object('storage_kind', 'special_restricted', 'under_stair', true),
            21.20, 7.50, 0, 1.26, 1.06, NULL, 0, 'rect', 'pending_validation',
            jsonb_build_object(
                'storage_kind', 'special_restricted',
                'safe_storage_authorized', false,
                'keep_stair_access_clear', true
            ),
            'Candidato bajo ESC. Altura variable por inclinación. NO habilitado como almacenamiento seguro. Mantener acceso despejado.',
            jsonb_build_object('provisional', true, 'under_stair', true)
        );
    END IF;

    SELECT COUNT(*) INTO v_has FROM warehouse.layout_elements
    WHERE revision_id = v_rev_id AND physical_element_id = v_bae2;
    IF v_has = 0 THEN
        INSERT INTO warehouse.layout_elements (
            revision_id, physical_element_id,
            code_snap, name_snap, element_type_snap, parent_physical_id_snap, physical_meta_snap,
            x_m, y_m, elevation_m, width_m, depth_m, height_clear_m, rotation_deg,
            geom_type, validation_status, constraints_json, observations, meta_json
        ) VALUES (
            v_rev_id, v_bae2,
            'BAE-02', 'Bajo escalera 2 (candidato)', 'position', COALESCE(v_esc, v_bnd),
            jsonb_build_object('storage_kind', 'special_restricted', 'under_stair', true),
            21.20, 8.70, 0, 1.26, 1.06, NULL, 0, 'rect', 'pending_validation',
            jsonb_build_object(
                'storage_kind', 'special_restricted',
                'safe_storage_authorized', false,
                'keep_stair_access_clear', true
            ),
            'Candidato bajo ESC. Altura variable. NO almacenamiento seguro autorizado. No interferir evacuación/circulación.',
            jsonb_build_object('provisional', true, 'under_stair', true)
        );
    END IF;

    INSERT INTO warehouse.layout_audit (
        revision_id, facility_id, floor_id, action, actor_label, detail_json
    ) VALUES (
        v_rev_id, v_facility_id, v_floor_id, 'seed_bc_special_zones_003', v_actor,
        jsonb_build_object('codes', jsonb_build_array('IZ-SU', 'BAE-01', 'BAE-02'), 'provisional', true)
    );
END $$;

COMMIT;
