-- =============================================================================
-- warehouse / 002 — Seed Bodega Central (solo DRAFT, idempotente)
-- AWAITING_APPROVAL — NO producción sin autorización.
--
-- No duplica facility/floor si ya existen.
-- No sobrescribe un draft que ya tenga elementos (modificado por usuarios).
-- No crea posiciones individuales de almacenamiento ni inventario.
-- Coordenadas PROVISIONALES → pending_validation.
-- =============================================================================

BEGIN;

-- Pallet estándar y convención en meta de facility (si se crea).
DO $$
DECLARE
    v_facility_id BIGINT;
    v_floor_id BIGINT;
    v_rev_id BIGINT;
    v_actor TEXT := 'seed:warehouse_002';
    v_elem_count INTEGER;
    -- physical ids
    v_bnd BIGINT; v_gate BIGINT; v_iz BIGINT; v_circ BIGINT; v_der BIGINT;
    v_ofc BIGINT; v_esc BIGINT;
    v_p1 BIGINT; v_p2 BIGINT; v_p3 BIGINT; v_p4 BIGINT; v_p5 BIGINT; v_p6 BIGINT; v_p7 BIGINT;
BEGIN
    SELECT id INTO v_facility_id
    FROM warehouse.facilities
    WHERE company_id = 3 AND code = 'BC'
    LIMIT 1;

    IF v_facility_id IS NULL THEN
        INSERT INTO warehouse.facilities (
            code, name, company_id, bsale_office_id, status, meta_json, created_by, updated_by
        ) VALUES (
            'BC',
            'Bodega Central',
            3,
            1,
            'active',
            jsonb_build_object(
                'coordinate_convention', jsonb_build_object(
                    'origin', 'southwest_corner_of_facility_boundary',
                    'x', 'length_meters_along_building',
                    'y', 'width_meters_left_to_right_from_entrance_view',
                    'rotation', 'degrees_ccw_about_rect_center',
                    'units', 'meters',
                    'elevation_m', 'building_floor_slab_reference',
                    'stack_levels_max', 'future_pallet_stacking_authorized_levels_independent'
                ),
                'standard_pallet_m', jsonb_build_object(
                    'length', 1.26,
                    'width', 1.06,
                    'orientation_configurable', true
                ),
                'symbology_notes', jsonb_build_object(
                    'BC', 'Bodega Central',
                    'IZ', 'Sector izquierdo',
                    'DER', 'Sector derecho',
                    'CIRC', 'Circulación central',
                    'Pn', 'Pasillo n',
                    'D', 'Delante (futuro en códigos de posición)',
                    'A', 'Atrás (futuro en códigos de posición)',
                    'OFC', 'Oficina',
                    'ESC', 'Escalera',
                    'PORTON', 'Portón principal',
                    'PAL', 'Identificador de pallet (fase futura)'
                ),
                'seed', '002_bodega_central_draft'
            ),
            v_actor,
            v_actor
        )
        RETURNING id INTO v_facility_id;
    END IF;

    SELECT id INTO v_floor_id
    FROM warehouse.floors
    WHERE facility_id = v_facility_id AND code = 'PISO1'
    LIMIT 1;

    IF v_floor_id IS NULL THEN
        INSERT INTO warehouse.floors (
            facility_id, code, name, level_index, length_m, width_m, status, meta_json,
            created_by, updated_by
        ) VALUES (
            v_facility_id,
            'PISO1',
            'Piso 1',
            1,
            23.10,
            13.70,
            'active',
            jsonb_build_object(
                'gate_width_m', 4.35,
                'second_floor_above_right_sector', true,
                'dims_note', 'Referencias confirmadas; no levantamiento arquitectónico certificado'
            ),
            v_actor,
            v_actor
        )
        RETURNING id INTO v_floor_id;
    END IF;

    SELECT id INTO v_rev_id
    FROM warehouse.layout_revisions
    WHERE floor_id = v_floor_id AND status = 'draft'
    LIMIT 1;

    IF v_rev_id IS NULL THEN
        INSERT INTO warehouse.layout_revisions (
            floor_id, status, label, row_version, notes, created_by, updated_by
        ) VALUES (
            v_floor_id,
            'draft',
            'BC Piso1 — seed inicial',
            1,
            'Borrador seed 002. Coordenadas provisionales pending_validation. No generar posiciones individuales.',
            v_actor,
            v_actor
        )
        RETURNING id INTO v_rev_id;
    END IF;

    SELECT COUNT(*) INTO v_elem_count
    FROM warehouse.layout_elements
    WHERE revision_id = v_rev_id;

    IF v_elem_count > 0 THEN
        -- Draft ya tiene geometría (posiblemente editada): no sobrescribir.
        INSERT INTO warehouse.layout_audit (
            revision_id, facility_id, floor_id, action, actor_label, detail_json
        ) VALUES (
            v_rev_id, v_facility_id, v_floor_id, 'seed_skipped_existing_draft', v_actor,
            jsonb_build_object('layout_element_count', v_elem_count)
        );
        RETURN;
    END IF;

    -- Select-or-insert physicals (idempotent; unique parcial floor+code activos)
    SELECT id INTO v_bnd FROM warehouse.physical_elements
    WHERE floor_id = v_floor_id AND status = 'active' AND code = 'BC-BND';
    IF v_bnd IS NULL THEN
        INSERT INTO warehouse.physical_elements (
            facility_id, floor_id, element_type, code, name, created_by, updated_by
        ) VALUES (v_facility_id, v_floor_id, 'facility_boundary', 'BC-BND', 'Límite Bodega Central', v_actor, v_actor)
        RETURNING id INTO v_bnd;
    END IF;

    SELECT id INTO v_gate FROM warehouse.physical_elements
    WHERE floor_id = v_floor_id AND status = 'active' AND code = 'PORTON';
    IF v_gate IS NULL THEN
        INSERT INTO warehouse.physical_elements (
            facility_id, floor_id, parent_id, element_type, code, name, created_by, updated_by
        ) VALUES (v_facility_id, v_floor_id, v_bnd, 'gate', 'PORTON', 'Portón principal 4,35 m', v_actor, v_actor)
        RETURNING id INTO v_gate;
    END IF;

    SELECT id INTO v_iz FROM warehouse.physical_elements
    WHERE floor_id = v_floor_id AND status = 'active' AND code = 'IZ';
    IF v_iz IS NULL THEN
        INSERT INTO warehouse.physical_elements (
            facility_id, floor_id, parent_id, element_type, code, name, meta_json, created_by, updated_by
        ) VALUES (
            v_facility_id, v_floor_id, v_bnd, 'sector', 'IZ', 'Sector izquierdo',
            jsonb_build_object(
                'storage_rows', jsonb_build_array('D', 'A'),
                'excel_refs_note', 'Excel con 20 referencias (incluye especiales); no todas son posiciones estándar'
            ),
            v_actor, v_actor
        )
        RETURNING id INTO v_iz;
    END IF;

    SELECT id INTO v_circ FROM warehouse.physical_elements
    WHERE floor_id = v_floor_id AND status = 'active' AND code = 'CIRC';
    IF v_circ IS NULL THEN
        INSERT INTO warehouse.physical_elements (
            facility_id, floor_id, parent_id, element_type, code, name, meta_json, created_by, updated_by
        ) VALUES (
            v_facility_id, v_floor_id, v_bnd, 'circulation', 'CIRC', 'Circulación central (Yale)',
            jsonb_build_object('width_range_m', jsonb_build_array(4.60, 4.80), 'not_storage', true),
            v_actor, v_actor
        )
        RETURNING id INTO v_circ;
    END IF;

    SELECT id INTO v_der FROM warehouse.physical_elements
    WHERE floor_id = v_floor_id AND status = 'active' AND code = 'DER';
    IF v_der IS NULL THEN
        INSERT INTO warehouse.physical_elements (
            facility_id, floor_id, parent_id, element_type, code, name, meta_json, created_by, updated_by
        ) VALUES (
            v_facility_id, v_floor_id, v_bnd, 'sector', 'DER', 'Sector derecho (pasillos)',
            jsonb_build_object(
                'second_floor_above', true,
                'observed_pallet_overhang_m', 0.60,
                'overhang_note', 'Observado; NO extensión autorizada de capacidad'
            ),
            v_actor, v_actor
        )
        RETURNING id INTO v_der;
    END IF;

    SELECT id INTO v_ofc FROM warehouse.physical_elements
    WHERE floor_id = v_floor_id AND status = 'active' AND code = 'OFC';
    IF v_ofc IS NULL THEN
        INSERT INTO warehouse.physical_elements (
            facility_id, floor_id, parent_id, element_type, code, name, created_by, updated_by
        ) VALUES (v_facility_id, v_floor_id, v_der, 'office', 'OFC', 'Oficina (referencia)', v_actor, v_actor)
        RETURNING id INTO v_ofc;
    END IF;

    -- Pasillos P1..P7
    SELECT id INTO v_p1 FROM warehouse.physical_elements WHERE floor_id = v_floor_id AND status='active' AND code='P1';
    IF v_p1 IS NULL THEN
        INSERT INTO warehouse.physical_elements (facility_id, floor_id, parent_id, element_type, code, name, created_by, updated_by)
        VALUES (v_facility_id, v_floor_id, v_der, 'aisle', 'P1', 'Pasillo 1 (cerca oficina)', v_actor, v_actor)
        RETURNING id INTO v_p1;
    END IF;
    SELECT id INTO v_p2 FROM warehouse.physical_elements WHERE floor_id = v_floor_id AND status='active' AND code='P2';
    IF v_p2 IS NULL THEN
        INSERT INTO warehouse.physical_elements (facility_id, floor_id, parent_id, element_type, code, name, created_by, updated_by)
        VALUES (v_facility_id, v_floor_id, v_der, 'aisle', 'P2', 'Pasillo 2', v_actor, v_actor)
        RETURNING id INTO v_p2;
    END IF;
    SELECT id INTO v_p3 FROM warehouse.physical_elements WHERE floor_id = v_floor_id AND status='active' AND code='P3';
    IF v_p3 IS NULL THEN
        INSERT INTO warehouse.physical_elements (facility_id, floor_id, parent_id, element_type, code, name, created_by, updated_by)
        VALUES (v_facility_id, v_floor_id, v_der, 'aisle', 'P3', 'Pasillo 3', v_actor, v_actor)
        RETURNING id INTO v_p3;
    END IF;
    SELECT id INTO v_p4 FROM warehouse.physical_elements WHERE floor_id = v_floor_id AND status='active' AND code='P4';
    IF v_p4 IS NULL THEN
        INSERT INTO warehouse.physical_elements (facility_id, floor_id, parent_id, element_type, code, name, created_by, updated_by)
        VALUES (v_facility_id, v_floor_id, v_der, 'aisle', 'P4', 'Pasillo 4', v_actor, v_actor)
        RETURNING id INTO v_p4;
    END IF;
    SELECT id INTO v_p5 FROM warehouse.physical_elements WHERE floor_id = v_floor_id AND status='active' AND code='P5';
    IF v_p5 IS NULL THEN
        INSERT INTO warehouse.physical_elements (facility_id, floor_id, parent_id, element_type, code, name, created_by, updated_by)
        VALUES (v_facility_id, v_floor_id, v_der, 'aisle', 'P5', 'Pasillo 5', v_actor, v_actor)
        RETURNING id INTO v_p5;
    END IF;
    SELECT id INTO v_p6 FROM warehouse.physical_elements WHERE floor_id = v_floor_id AND status='active' AND code='P6';
    IF v_p6 IS NULL THEN
        INSERT INTO warehouse.physical_elements (facility_id, floor_id, parent_id, element_type, code, name, created_by, updated_by)
        VALUES (v_facility_id, v_floor_id, v_der, 'aisle', 'P6', 'Pasillo 6', v_actor, v_actor)
        RETURNING id INTO v_p6;
    END IF;
    SELECT id INTO v_p7 FROM warehouse.physical_elements WHERE floor_id = v_floor_id AND status='active' AND code='P7';
    IF v_p7 IS NULL THEN
        INSERT INTO warehouse.physical_elements (facility_id, floor_id, parent_id, element_type, code, name, created_by, updated_by)
        VALUES (v_facility_id, v_floor_id, v_der, 'aisle', 'P7', 'Pasillo 7 (fondo / escalera)', v_actor, v_actor)
        RETURNING id INTO v_p7;
    END IF;

    SELECT id INTO v_esc FROM warehouse.physical_elements
    WHERE floor_id = v_floor_id AND status = 'active' AND code = 'ESC';
    IF v_esc IS NULL THEN
        INSERT INTO warehouse.physical_elements (
            facility_id, floor_id, parent_id, element_type, code, name, created_by, updated_by
        ) VALUES (v_facility_id, v_floor_id, v_der, 'stair', 'ESC', 'Escalera (referencia)', v_actor, v_actor)
        RETURNING id INTO v_esc;
    END IF;

    -- Geometrías + identity snaps (metros). Origen SW. Ver observations.
    -- IZ: franja Y 0..2.65; CIRC: 2.65..7.35 (4.70 mid de 4.6–4.8); DER: 7.35..13.35 (6.00).
    INSERT INTO warehouse.layout_elements (
        revision_id, physical_element_id,
        code_snap, name_snap, element_type_snap, parent_physical_id_snap, physical_meta_snap,
        x_m, y_m, elevation_m, width_m, depth_m, height_clear_m, rotation_deg,
        geom_type, validation_status, constraints_json, observations, meta_json
    ) VALUES
    (v_rev_id, v_bnd, 'BC-BND', 'Límite Bodega Central', 'facility_boundary', NULL, '{}'::jsonb,
     0, 0, 0, 23.10, 13.70, NULL, 0, 'rect', 'pending_validation',
     '{}'::jsonb,
     'Límite 23,10 × 13,70 m confirmado como referencia; no certificado arquitectónico.',
     jsonb_build_object('provisional', true)),
    (v_rev_id, v_gate, 'PORTON', 'Portón principal 4,35 m', 'gate', v_bnd, '{}'::jsonb,
     9.375, 0, 0, 4.35, 0.40, NULL, 0, 'rect', 'pending_validation',
     '{}'::jsonb,
     'Portón 4,35 m centrado de forma provisional en muro sur (y=0).',
     jsonb_build_object('provisional', true)),
    (v_rev_id, v_iz, 'IZ', 'Sector izquierdo', 'sector', v_bnd,
     jsonb_build_object('storage_rows', jsonb_build_array('D', 'A')),
     0, 0, 0, 23.10, 2.65, 4.00, 0, 'rect', 'pending_validation',
     jsonb_build_object('height_clear_m_authorized', false),
     'Profundidad ~2,65 m. Altura 4,00 m REFERENCIAL — no máximo seguro autorizado. Filas D/A futuras.',
     jsonb_build_object('provisional', true, 'storage_rows', jsonb_build_array('D','A'))),
    (v_rev_id, v_circ, 'CIRC', 'Circulación central (Yale)', 'circulation', v_bnd,
     jsonb_build_object('width_range_m', jsonb_build_array(4.60, 4.80), 'not_storage', true),
     0, 2.65, 0, 23.10, 4.70, NULL, 0, 'rect', 'pending_validation',
     jsonb_build_object('width_range_m', jsonb_build_array(4.60, 4.80), 'storage_forbidden_assumed', true),
     'Ancho mid 4,70 m dentro del rango observado 4,60–4,80 (pallets IZ ↔ viga DER). No asumir almacenamiento.',
     jsonb_build_object('provisional', true)),
    (v_rev_id, v_der, 'DER', 'Sector derecho (pasillos)', 'sector', v_bnd,
     jsonb_build_object('second_floor_above', true, 'observed_pallet_overhang_m', 0.60),
     0, 7.35, 0, 23.10, 6.00, 2.75, 0, 'rect', 'pending_validation',
     jsonb_build_object(
        'height_clear_m_authorized', false,
        'observed_overhang_m', 0.60,
        'overhang_authorized', false
     ),
     'Sector derecho 6,00 × alto libre 2,75 m referenciales. Sobresaliente 0,60 m observado ≠ autorizado.',
     jsonb_build_object('provisional', true)),
    (v_rev_id, v_ofc, 'OFC', 'Oficina (referencia)', 'office', v_der, '{}'::jsonb,
     0, 7.35, 0, 1.50, 6.00, 2.75, 0, 'rect', 'pending_validation',
     '{}'::jsonb,
     'Oficina: geometría placeholder junto a P1. Validar en terreno.',
     jsonb_build_object('provisional', true)),
    (v_rev_id, v_p1, 'P1', 'Pasillo 1 (cerca oficina)', 'aisle', v_der, '{}'::jsonb,
     1.50, 7.35, 0, 2.80, 6.00, 2.75, 0, 'rect', 'pending_validation',
     jsonb_build_object('height_clear_m_authorized', false),
     'Pasillo 1: 2,80 × 6,00 m referencial. Sin posiciones individuales en seed.',
     jsonb_build_object('provisional', true)),
    (v_rev_id, v_p2, 'P2', 'Pasillo 2', 'aisle', v_der, '{}'::jsonb,
     4.30, 7.35, 0, 2.80, 6.00, 2.75, 0, 'rect', 'pending_validation',
     jsonb_build_object('height_clear_m_authorized', false),
     'Pasillo 2: geometría provisional.',
     jsonb_build_object('provisional', true)),
    (v_rev_id, v_p3, 'P3', 'Pasillo 3', 'aisle', v_der, '{}'::jsonb,
     7.10, 7.35, 0, 2.80, 6.00, 2.75, 0, 'rect', 'pending_validation',
     jsonb_build_object('height_clear_m_authorized', false),
     'Pasillo 3: geometría provisional.',
     jsonb_build_object('provisional', true)),
    (v_rev_id, v_p4, 'P4', 'Pasillo 4', 'aisle', v_der, '{}'::jsonb,
     9.90, 7.35, 0, 2.80, 6.00, 2.75, 0, 'rect', 'pending_validation',
     jsonb_build_object('height_clear_m_authorized', false),
     'Pasillo 4: geometría provisional.',
     jsonb_build_object('provisional', true)),
    (v_rev_id, v_p5, 'P5', 'Pasillo 5', 'aisle', v_der, '{}'::jsonb,
     12.70, 7.35, 0, 2.80, 6.00, 2.75, 0, 'rect', 'pending_validation',
     jsonb_build_object('height_clear_m_authorized', false),
     'Pasillo 5: geometría provisional.',
     jsonb_build_object('provisional', true)),
    (v_rev_id, v_p6, 'P6', 'Pasillo 6', 'aisle', v_der, '{}'::jsonb,
     15.50, 7.35, 0, 2.80, 6.00, 2.75, 0, 'rect', 'pending_validation',
     jsonb_build_object('height_clear_m_authorized', false),
     'Pasillo 6: geometría provisional.',
     jsonb_build_object('provisional', true)),
    (v_rev_id, v_p7, 'P7', 'Pasillo 7 (fondo / escalera)', 'aisle', v_der, '{}'::jsonb,
     18.30, 7.35, 0, 2.80, 6.00, 2.75, 0, 'rect', 'pending_validation',
     jsonb_build_object('height_clear_m_authorized', false),
     'Pasillo 7: cerca escalera. Sin posiciones en seed.',
     jsonb_build_object('provisional', true)),
    (v_rev_id, v_esc, 'ESC', 'Escalera (referencia)', 'stair', v_der, '{}'::jsonb,
     21.10, 7.35, 0, 2.00, 6.00, 2.75, 0, 'rect', 'pending_validation',
     '{}'::jsonb,
     'Escalera: placeholder al fondo del sector derecho. Validar posición real.',
     jsonb_build_object('provisional', true))
    ON CONFLICT (revision_id, physical_element_id) DO NOTHING;

    INSERT INTO warehouse.layout_audit (
        revision_id, facility_id, floor_id, action, actor_label, detail_json
    ) VALUES (
        v_rev_id, v_facility_id, v_floor_id, 'seed_bc_draft_applied', v_actor,
        jsonb_build_object(
            'facility_code', 'BC',
            'floor_code', 'PISO1',
            'bsale_office_id', 1,
            'positions_created', 0,
            'provisional_geometry', true
        )
    );
END $$;

COMMIT;
