-- =============================================================================
-- warehouse / 006 — Corrección geometría borrador BC/PISO1 (productivo analytics)
-- AWAITING APPROVAL — NO ejecutar automáticamente.
--
-- Destino: SOLO draft existente revision_id=1 / row_version=1 en BD analytics.
-- No re-ejecuta seeds 002/003. No DROP SCHEMA. No publica. Preserva physical IDs.
--
-- Convención SoT:
--   +X = largo longitudinal (entrada x≈0 → fondo)
--   +Y = ancho transversal (IZ y≈0 → DER)
--
-- Coordenadas OFC/P1–P7: referencia preliminar consecutiva (tramos 2,80 m);
--   residual 0,70 m al fondo (x 22,40–23,10). Todo pending_validation.
-- =============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- PRECHECK lectura (informativo; los guards duros están en el DO)
-- ---------------------------------------------------------------------------
SELECT current_database() AS db, current_user AS db_user, now() AS checked_at;

SELECT f.code AS facility, fl.code AS floor, r.id AS revision_id, r.status,
       r.row_version, COUNT(le.id) AS layout_elements
FROM warehouse.facilities f
JOIN warehouse.floors fl ON fl.facility_id = f.id
JOIN warehouse.layout_revisions r ON r.floor_id = fl.id
LEFT JOIN warehouse.layout_elements le ON le.revision_id = r.id
WHERE f.code = 'BC' AND fl.code = 'PISO1' AND r.status = 'draft'
GROUP BY f.code, fl.code, r.id, r.status, r.row_version;

SELECT le.code_snap, le.x_m, le.y_m, le.width_m, le.depth_m
FROM warehouse.layout_elements le
WHERE le.revision_id = 1
ORDER BY le.code_snap;

DO $$
DECLARE
    v_facility_id BIGINT;
    v_floor_id BIGINT;
    v_rev_id BIGINT;
    v_row_version INT;
    v_status TEXT;
    v_facility_code TEXT;
    v_floor_code TEXT;
    v_elem_count INT;
    v_actor TEXT := 'fix:warehouse_006_layout_orientation';
    v_updated INT;
    r_porton RECORD;
    r_ofc RECORD;
    r_already BOOLEAN;
BEGIN
    -- Guard: base de datos productiva esperada.
    -- Lab aislado: SET LOCAL warehouse.fix_006_lab = 'on'; (solo tests; no usar en prod)
    IF current_database() IS DISTINCT FROM 'analytics'
       AND COALESCE(current_setting('warehouse.fix_006_lab', true), '') IS DISTINCT FROM 'on' THEN
        RAISE EXCEPTION '006 abort: current_database()=% (esperado analytics)', current_database();
    END IF;

    SELECT f.id, fl.id, r.id, r.row_version, r.status, f.code, fl.code
      INTO v_facility_id, v_floor_id, v_rev_id, v_row_version, v_status,
           v_facility_code, v_floor_code
    FROM warehouse.facilities f
    JOIN warehouse.floors fl ON fl.facility_id = f.id
    JOIN warehouse.layout_revisions r ON r.floor_id = fl.id
    WHERE f.code = 'BC' AND fl.code = 'PISO1' AND r.status = 'draft'
    FOR UPDATE OF r;

    IF v_rev_id IS NULL THEN
        RAISE EXCEPTION '006 abort: no hay draft BC/PISO1';
    END IF;

    IF v_facility_code IS DISTINCT FROM 'BC' OR v_floor_code IS DISTINCT FROM 'PISO1' THEN
        RAISE EXCEPTION '006 abort: identidad inesperada facility=% floor=%',
            v_facility_code, v_floor_code;
    END IF;

    IF v_rev_id IS DISTINCT FROM 1 THEN
        RAISE EXCEPTION '006 abort: revision_id=% (esperado 1)', v_rev_id;
    END IF;

    IF v_status IS DISTINCT FROM 'draft' THEN
        RAISE EXCEPTION '006 abort: status=% (esperado draft)', v_status;
    END IF;

    IF v_row_version IS DISTINCT FROM 1 THEN
        RAISE EXCEPTION '006 abort: row_version=% (esperado 1). ¿Ya aplicado o editado?',
            v_row_version;
    END IF;

    SELECT COUNT(*) INTO v_elem_count
    FROM warehouse.layout_elements
    WHERE revision_id = v_rev_id;

    IF v_elem_count IS DISTINCT FROM 17 THEN
        RAISE EXCEPTION '006 abort: layout_elements=% (esperado 17)', v_elem_count;
    END IF;

    -- Idempotencia: no reaplicar si ya quedó marca 006
    SELECT EXISTS (
        SELECT 1 FROM warehouse.layout_elements
        WHERE revision_id = v_rev_id
          AND (meta_json->>'corrected_by') = '006'
    ) INTO r_already;
    IF r_already THEN
        RAISE EXCEPTION '006 abort: meta corrected_by=006 ya presente (no reejecutar)';
    END IF;

    -- Verificar geometrías ORIGINALES del seed que se van a tocar
    SELECT x_m, y_m, width_m, depth_m INTO r_porton
    FROM warehouse.layout_elements
    WHERE revision_id = v_rev_id AND code_snap = 'PORTON';
    IF r_porton IS NULL THEN
        RAISE EXCEPTION '006 abort: falta PORTON en draft';
    END IF;
    IF r_porton.x_m IS DISTINCT FROM 9.375
       OR r_porton.y_m IS DISTINCT FROM 0
       OR r_porton.width_m IS DISTINCT FROM 4.35
       OR r_porton.depth_m IS DISTINCT FROM 0.40 THEN
        RAISE EXCEPTION
            '006 abort: PORTON no coincide con seed original (%,%,%,%)',
            r_porton.x_m, r_porton.y_m, r_porton.width_m, r_porton.depth_m;
    END IF;

    SELECT x_m, y_m, width_m, depth_m INTO r_ofc
    FROM warehouse.layout_elements
    WHERE revision_id = v_rev_id AND code_snap = 'OFC';
    IF r_ofc IS NULL THEN
        RAISE EXCEPTION '006 abort: falta OFC en draft';
    END IF;
    IF r_ofc.x_m IS DISTINCT FROM 0
       OR r_ofc.y_m IS DISTINCT FROM 7.35
       OR r_ofc.width_m IS DISTINCT FROM 1.50
       OR r_ofc.depth_m IS DISTINCT FROM 6.00 THEN
        RAISE EXCEPTION
            '006 abort: OFC no coincide con seed original (%,%,%,%)',
            r_ofc.x_m, r_ofc.y_m, r_ofc.width_m, r_ofc.depth_m;
    END IF;

    -- -------------------------------------------------------------------------
    -- Updates (17 physicals intactos; solo layout_elements del draft)
    -- -------------------------------------------------------------------------

    -- PORTON: muro corto frontal (x≈0), 4,35 m a lo largo de Y
    UPDATE warehouse.layout_elements
    SET x_m = 0, y_m = 4.675, width_m = 0.40, depth_m = 4.35, rotation_deg = 0,
        validation_status = 'pending_validation',
        observations = 'Portón 4,35 m en muro corto de frente (x≈0). Y centrada provisional; validar en terreno.',
        meta_json = COALESCE(meta_json, '{}'::jsonb) || jsonb_build_object(
            'provisional', true, 'corrected_by', '006', 'placement', 'front_short_wall'),
        updated_at = now()
    WHERE revision_id = v_rev_id AND code_snap = 'PORTON';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN RAISE EXCEPTION '006 abort: PORTON rows=%', v_updated; END IF;

    -- OFC: 2,80 m longitudinales (X 0–2,80). Profundidad Y provisional = franja DER.
    UPDATE warehouse.layout_elements
    SET x_m = 0, y_m = 7.35, width_m = 2.80, depth_m = 6.00, rotation_deg = 0,
        validation_status = 'pending_validation',
        observations = 'Oficina: 2,80 m longitudinales (X 0–2,80). depth_m transversal provisional (= franja DER 6,00); pendiente validación en terreno.',
        meta_json = COALESCE(meta_json, '{}'::jsonb) || jsonb_build_object(
            'provisional', true, 'corrected_by', '006'),
        updated_at = now()
    WHERE revision_id = v_rev_id AND code_snap = 'OFC';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN RAISE EXCEPTION '006 abort: OFC rows=%', v_updated; END IF;

    -- P1–P7: tramos consecutivos 2,80 m sin solaparse con OFC (ref. preliminar)
    UPDATE warehouse.layout_elements
    SET x_m = 2.80, y_m = 7.35, width_m = 2.80, depth_m = 6.00, rotation_deg = 0,
        validation_status = 'pending_validation',
        observations = 'Pasillo 1: X 2,80–5,60 (preliminar consecutivo). Pendiente validación. Sin posiciones en seed.',
        meta_json = COALESCE(meta_json, '{}'::jsonb) || jsonb_build_object(
            'provisional', true, 'corrected_by', '006'),
        updated_at = now()
    WHERE revision_id = v_rev_id AND code_snap = 'P1';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN RAISE EXCEPTION '006 abort: P1 rows=%', v_updated; END IF;

    UPDATE warehouse.layout_elements
    SET x_m = 5.60, y_m = 7.35, width_m = 2.80, depth_m = 6.00, rotation_deg = 0,
        validation_status = 'pending_validation',
        observations = 'Pasillo 2: X 5,60–8,40 (preliminar). Pendiente validación.',
        meta_json = COALESCE(meta_json, '{}'::jsonb) || jsonb_build_object(
            'provisional', true, 'corrected_by', '006'),
        updated_at = now()
    WHERE revision_id = v_rev_id AND code_snap = 'P2';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN RAISE EXCEPTION '006 abort: P2 rows=%', v_updated; END IF;

    UPDATE warehouse.layout_elements
    SET x_m = 8.40, y_m = 7.35, width_m = 2.80, depth_m = 6.00, rotation_deg = 0,
        validation_status = 'pending_validation',
        observations = 'Pasillo 3: X 8,40–11,20 (preliminar). Pendiente validación.',
        meta_json = COALESCE(meta_json, '{}'::jsonb) || jsonb_build_object(
            'provisional', true, 'corrected_by', '006'),
        updated_at = now()
    WHERE revision_id = v_rev_id AND code_snap = 'P3';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN RAISE EXCEPTION '006 abort: P3 rows=%', v_updated; END IF;

    UPDATE warehouse.layout_elements
    SET x_m = 11.20, y_m = 7.35, width_m = 2.80, depth_m = 6.00, rotation_deg = 0,
        validation_status = 'pending_validation',
        observations = 'Pasillo 4: X 11,20–14,00 (preliminar). Pendiente validación.',
        meta_json = COALESCE(meta_json, '{}'::jsonb) || jsonb_build_object(
            'provisional', true, 'corrected_by', '006'),
        updated_at = now()
    WHERE revision_id = v_rev_id AND code_snap = 'P4';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN RAISE EXCEPTION '006 abort: P4 rows=%', v_updated; END IF;

    UPDATE warehouse.layout_elements
    SET x_m = 14.00, y_m = 7.35, width_m = 2.80, depth_m = 6.00, rotation_deg = 0,
        validation_status = 'pending_validation',
        observations = 'Pasillo 5: X 14,00–16,80 (preliminar). Pendiente validación.',
        meta_json = COALESCE(meta_json, '{}'::jsonb) || jsonb_build_object(
            'provisional', true, 'corrected_by', '006'),
        updated_at = now()
    WHERE revision_id = v_rev_id AND code_snap = 'P5';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN RAISE EXCEPTION '006 abort: P5 rows=%', v_updated; END IF;

    UPDATE warehouse.layout_elements
    SET x_m = 16.80, y_m = 7.35, width_m = 2.80, depth_m = 6.00, rotation_deg = 0,
        validation_status = 'pending_validation',
        observations = 'Pasillo 6: X 16,80–19,60 (preliminar). Pendiente validación.',
        meta_json = COALESCE(meta_json, '{}'::jsonb) || jsonb_build_object(
            'provisional', true, 'corrected_by', '006'),
        updated_at = now()
    WHERE revision_id = v_rev_id AND code_snap = 'P6';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN RAISE EXCEPTION '006 abort: P6 rows=%', v_updated; END IF;

    UPDATE warehouse.layout_elements
    SET x_m = 19.60, y_m = 7.35, width_m = 2.80, depth_m = 6.00, rotation_deg = 0,
        validation_status = 'pending_validation',
        observations = 'Pasillo 7: X 19,60–22,40 (preliminar). Junto a escalera/fondo. Residual 0,70 m (22,40–23,10) fuera de este tramo. Pendiente validación.',
        meta_json = COALESCE(meta_json, '{}'::jsonb) || jsonb_build_object(
            'provisional', true, 'corrected_by', '006'),
        updated_at = now()
    WHERE revision_id = v_rev_id AND code_snap = 'P7';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN RAISE EXCEPTION '006 abort: P7 rows=%', v_updated; END IF;

    -- ESC: transversal contra pared de fondo; huella provisional cubre BAE (no dims definitivas)
    -- X 21,84–23,10 (1,26 m) dentro del residual + solape preliminar con extremo P7; pending.
    UPDATE warehouse.layout_elements
    SET x_m = 21.84, y_m = 7.35, width_m = 1.26, depth_m = 6.00, rotation_deg = 0,
        validation_status = 'pending_validation',
        observations = 'Escalera transversal contra pared de fondo, junto a P7. Huella provisional X 21,84–23,10 × franja DER para contener BAE; dims/extensión NO definitivas. Pendiente validación. Puede solapar extremo P7 en X.',
        meta_json = COALESCE(meta_json, '{}'::jsonb) || jsonb_build_object(
            'provisional', true, 'corrected_by', '006'),
        updated_at = now()
    WHERE revision_id = v_rev_id AND code_snap = 'ESC';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN RAISE EXCEPTION '006 abort: ESC rows=%', v_updated; END IF;

    -- IZ-SU: extremo IZ del fondo ≈2,67 m²; sin posiciones pallet
    UPDATE warehouse.layout_elements
    SET x_m = 20.58, y_m = 0.10, width_m = 2.52, depth_m = 1.06, rotation_deg = 0,
        validation_status = 'pending_validation',
        observations = 'Área ≈2,67 m² sin pallet en extremo IZ del fondo (cerca escalera). No crear posiciones pallet. Forma exacta pendiente.',
        meta_json = COALESCE(meta_json, '{}'::jsonb) || jsonb_build_object(
            'provisional', true, 'storage_kind', 'loose_no_pallet', 'corrected_by', '006'),
        constraints_json = COALESCE(constraints_json, '{}'::jsonb) || jsonb_build_object(
            'storage_kind', 'loose_no_pallet', 'stacking_authorized', false),
        updated_at = now()
    WHERE revision_id = v_rev_id AND code_snap = 'IZ-SU';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN RAISE EXCEPTION '006 abort: IZ-SU rows=%', v_updated; END IF;

    -- BAE-01/02: dentro de huella ESC; NO autorizados
    UPDATE warehouse.layout_elements
    SET x_m = 21.84, y_m = 7.50, width_m = 1.26, depth_m = 1.06, rotation_deg = 0,
        validation_status = 'pending_validation',
        observations = 'Candidato bajo ESC (dentro de huella provisional). NO almacenamiento seguro autorizado. Mantener acceso despejado.',
        meta_json = COALESCE(meta_json, '{}'::jsonb) || jsonb_build_object(
            'provisional', true, 'under_stair', true, 'corrected_by', '006'),
        constraints_json = COALESCE(constraints_json, '{}'::jsonb) || jsonb_build_object(
            'storage_kind', 'special_restricted',
            'safe_storage_authorized', false,
            'keep_stair_access_clear', true),
        updated_at = now()
    WHERE revision_id = v_rev_id AND code_snap = 'BAE-01';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN RAISE EXCEPTION '006 abort: BAE-01 rows=%', v_updated; END IF;

    UPDATE warehouse.layout_elements
    SET x_m = 21.84, y_m = 8.70, width_m = 1.26, depth_m = 1.06, rotation_deg = 0,
        validation_status = 'pending_validation',
        observations = 'Candidato bajo ESC (dentro de huella provisional). NO almacenamiento seguro autorizado.',
        meta_json = COALESCE(meta_json, '{}'::jsonb) || jsonb_build_object(
            'provisional', true, 'under_stair', true, 'corrected_by', '006'),
        constraints_json = COALESCE(constraints_json, '{}'::jsonb) || jsonb_build_object(
            'storage_kind', 'special_restricted',
            'safe_storage_authorized', false,
            'keep_stair_access_clear', true),
        updated_at = now()
    WHERE revision_id = v_rev_id AND code_snap = 'BAE-02';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN RAISE EXCEPTION '006 abort: BAE-02 rows=%', v_updated; END IF;

    -- BC-BND: solo nota de ejes (geometría 0,0,23.10×13.70 se mantiene)
    UPDATE warehouse.layout_elements
    SET observations = 'Límite 23,10 (largo) × 13,70 (ancho) m. Entrada en muro corto x≈0. No certificado arquitectónico.',
        meta_json = COALESCE(meta_json, '{}'::jsonb) || jsonb_build_object(
            'provisional', true, 'corrected_by', '006',
            'axis_note', 'x=length_entrance_to_back;y=width_iz_to_der'),
        updated_at = now()
    WHERE revision_id = v_rev_id AND code_snap = 'BC-BND';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN RAISE EXCEPTION '006 abort: BC-BND rows=%', v_updated; END IF;

    -- IZ / CIRC / DER: marca corrected_by sin cambiar geometría (auditoría de pase)
    UPDATE warehouse.layout_elements
    SET meta_json = COALESCE(meta_json, '{}'::jsonb) || jsonb_build_object(
            'corrected_by', '006', 'geometry_unchanged', true),
        updated_at = now()
    WHERE revision_id = v_rev_id AND code_snap IN ('IZ', 'CIRC', 'DER');
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 3 THEN RAISE EXCEPTION '006 abort: IZ/CIRC/DER rows=%', v_updated; END IF;

    UPDATE warehouse.layout_revisions
    SET row_version = row_version + 1,
        updated_at = now(),
        updated_by = v_actor,
        notes = COALESCE(notes, '') || E'\n[006] Corrección layout OFC/P1-P7/PORTON/ESC/IZ-SU/BAE (pending_validation).'
    WHERE id = v_rev_id AND row_version = 1;
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN
        RAISE EXCEPTION '006 abort: bump row_version falló (concurrencia?)';
    END IF;

    INSERT INTO warehouse.layout_audit (
        revision_id, facility_id, floor_id, action, actor_label, detail_json
    ) VALUES (
        v_rev_id, v_facility_id, v_floor_id, 'fix_bc_draft_layout_006', v_actor,
        jsonb_build_object(
            'revision_id', v_rev_id,
            'previous_row_version', 1,
            'new_row_version', 2,
            'updated_codes', jsonb_build_array(
                'PORTON','OFC','P1','P2','P3','P4','P5','P6','P7',
                'ESC','IZ-SU','BAE-01','BAE-02','BC-BND'),
            'unchanged_geometry_codes', jsonb_build_array('IZ','CIRC','DER'),
            'publish', false,
            'residual_length_m', 0.70
        )
    );

    -- Post-check: BAE dentro de ESC (AABB)
    IF EXISTS (
        SELECT 1
        FROM warehouse.layout_elements bae
        JOIN warehouse.layout_elements esc
          ON esc.revision_id = bae.revision_id AND esc.code_snap = 'ESC'
        WHERE bae.revision_id = v_rev_id
          AND bae.code_snap IN ('BAE-01', 'BAE-02')
          AND (
            bae.x_m < esc.x_m - 1e-9
            OR bae.y_m < esc.y_m - 1e-9
            OR bae.x_m + bae.width_m > esc.x_m + esc.width_m + 1e-9
            OR bae.y_m + bae.depth_m > esc.y_m + esc.depth_m + 1e-9
          )
    ) THEN
        RAISE EXCEPTION '006 abort: BAE fuera de huella ESC';
    END IF;

    -- OFC no solapa P1 en X
    IF EXISTS (
        SELECT 1
        FROM warehouse.layout_elements ofc
        JOIN warehouse.layout_elements p1
          ON p1.revision_id = ofc.revision_id AND p1.code_snap = 'P1'
        WHERE ofc.revision_id = v_rev_id AND ofc.code_snap = 'OFC'
          AND ofc.x_m + ofc.width_m > p1.x_m + 1e-9
    ) THEN
        RAISE EXCEPTION '006 abort: OFC solapa P1 en X';
    END IF;
END $$;

-- VERIFY
SELECT r.id, r.status, r.row_version,
       (SELECT COUNT(*) FROM warehouse.layout_elements le WHERE le.revision_id = r.id) AS n_elem
FROM warehouse.layout_revisions r WHERE r.id = 1;
-- Esperado: draft, row_version=2, n_elem=17

SELECT code_snap, x_m, y_m, width_m, depth_m
FROM warehouse.layout_elements
WHERE revision_id = 1
ORDER BY code_snap;

SELECT COUNT(*) AS published_bc
FROM warehouse.layout_revisions r
JOIN warehouse.floors fl ON fl.id = r.floor_id
JOIN warehouse.facilities f ON f.id = fl.facility_id
WHERE f.code = 'BC' AND r.status = 'published';
-- Esperado: 0

COMMIT;
