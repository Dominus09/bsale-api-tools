-- =============================================================================
-- warehouse / 007 — IZ-SU compacta + ESC/BAE en franja CIRC (post-006)
-- AWAITING APPROVAL — NO ejecutar automáticamente.
--
-- Tras 006: revision_id=1, row_version=2, draft, 17 elementos.
-- Solo toca IZ-SU, ESC, BAE-01, BAE-02 (+ parent físico ESC → CIRC).
-- NO modifica P1–P7 ni el resto. No publica. Preserva physical IDs.
--
-- Ubicación ESC (corrección):
--   Dentro de CIRC (Y ≈ 2,65–7,35), contra pared de fondo (X alto),
--   frente a P7. NO invade DER ni reduce P7.
-- =============================================================================

BEGIN;

SELECT current_database() AS db, now() AS checked_at;
-- Nota: no declarar variable PL/pgSQL llamada "r" (evita ambigüedad con alias de tabla).
SELECT rev.id, rev.status, rev.row_version,
       (SELECT COUNT(*) FROM warehouse.layout_elements le WHERE le.revision_id = rev.id) AS n
FROM warehouse.layout_revisions rev WHERE rev.id = 1;

SELECT code_snap, x_m, y_m, width_m, depth_m, parent_physical_id_snap
FROM warehouse.layout_elements
WHERE revision_id = 1 AND code_snap IN ('IZ-SU','ESC','BAE-01','BAE-02','CIRC','P7','DER')
ORDER BY 1;

DO $$
DECLARE
    v_facility_id BIGINT;
    v_floor_id BIGINT;
    v_rev_id BIGINT;
    v_row_version INT;
    v_status TEXT;
    v_elem_count INT;
    v_circ_id BIGINT;
    v_actor TEXT := 'fix:warehouse_007_esc_izsu_bae';
    v_updated INT;
BEGIN
    IF current_database() IS DISTINCT FROM 'analytics'
       AND COALESCE(current_setting('warehouse.fix_007_lab', true), '') IS DISTINCT FROM 'on' THEN
        RAISE EXCEPTION '007 abort: current_database()=% (esperado analytics)', current_database();
    END IF;

    SELECT f.id, fl.id, rev.id, rev.row_version, rev.status
      INTO v_facility_id, v_floor_id, v_rev_id, v_row_version, v_status
    FROM warehouse.facilities f
    JOIN warehouse.floors fl ON fl.facility_id = f.id
    JOIN warehouse.layout_revisions rev ON rev.floor_id = fl.id
    WHERE f.code = 'BC' AND fl.code = 'PISO1' AND rev.status = 'draft'
    FOR UPDATE OF rev;

    IF v_rev_id IS DISTINCT FROM 1 THEN
        RAISE EXCEPTION '007 abort: revision_id=% (esperado 1)', v_rev_id;
    END IF;
    IF v_status IS DISTINCT FROM 'draft' THEN
        RAISE EXCEPTION '007 abort: status=%', v_status;
    END IF;
    IF v_row_version IS DISTINCT FROM 2 THEN
        RAISE EXCEPTION '007 abort: row_version=% (esperado 2 post-006)', v_row_version;
    END IF;

    SELECT COUNT(*) INTO v_elem_count FROM warehouse.layout_elements WHERE revision_id = v_rev_id;
    IF v_elem_count IS DISTINCT FROM 17 THEN
        RAISE EXCEPTION '007 abort: elements=% (esperado 17)', v_elem_count;
    END IF;

    IF EXISTS (
        SELECT 1 FROM warehouse.layout_elements
        WHERE revision_id = v_rev_id AND (meta_json->>'corrected_by') = '007'
    ) THEN
        RAISE EXCEPTION '007 abort: already applied (corrected_by=007)';
    END IF;

    -- Guard: P7 intacto (2,80 × 6,00 en franja DER) — no debe haber sido alterado por este script
    IF NOT EXISTS (
        SELECT 1 FROM warehouse.layout_elements
        WHERE revision_id = v_rev_id AND code_snap = 'P7'
          AND x_m = 19.60 AND y_m = 7.35 AND width_m = 2.80 AND depth_m = 6.00
    ) THEN
        RAISE EXCEPTION '007 abort: P7 no coincide con geometría post-006 esperada (no modificar P7)';
    END IF;

    SELECT id INTO v_circ_id
    FROM warehouse.physical_elements
    WHERE floor_id = v_floor_id AND status = 'active' AND code = 'CIRC';
    IF v_circ_id IS NULL THEN
        RAISE EXCEPTION '007 abort: physical CIRC no encontrado';
    END IF;

    -- Reparent ESC → CIRC (ya no bajo DER)
    UPDATE warehouse.physical_elements
    SET parent_id = v_circ_id, updated_by = v_actor, updated_at = now()
    WHERE floor_id = v_floor_id AND status = 'active' AND code = 'ESC';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN RAISE EXCEPTION '007 abort: physical ESC parent update rows=%', v_updated; END IF;

    -- IZ-SU: compacta ≈2 pallets en fondo izquierdo (sin tocar si el usuario ya la dejó así)
    UPDATE warehouse.layout_elements
    SET x_m = 21.84, y_m = 0.10, width_m = 1.26, depth_m = 2.12, rotation_deg = 0,
        validation_status = 'pending_validation',
        observations = 'Zona sueltos ≈2,67 m² (~2 pallets) en extremo IZ del fondo. Compacta; no posiciones pallet. Pendiente validación. Editable en editor.',
        meta_json = COALESCE(meta_json, '{}'::jsonb) || jsonb_build_object(
            'provisional', true,
            'storage_kind', 'loose_no_pallet',
            'approx_area_m2', 2.67,
            'corrected_by', '007'),
        constraints_json = COALESCE(constraints_json, '{}'::jsonb) || jsonb_build_object(
            'storage_kind', 'loose_no_pallet', 'stacking_authorized', false),
        updated_at = now()
    WHERE revision_id = v_rev_id AND code_snap = 'IZ-SU';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN RAISE EXCEPTION '007 abort: IZ-SU rows=%', v_updated; END IF;

    -- ESC: en CIRC (Y 2,65–7,35), contra fondo, frente a P7. No invade DER (y+depth ≤ 7,35).
    -- Dims provisionales; editable en editor.
    UPDATE warehouse.layout_elements
    SET x_m = 21.58, y_m = 4.05, width_m = 1.52, depth_m = 3.20, rotation_deg = 0,
        parent_physical_id_snap = v_circ_id,
        validation_status = 'pending_validation',
        observations = 'Escalera en extremo posterior de CIRC, contra pared de fondo, frente a P7. Sube a 2° piso. NO ocupa DER ni P7. Dims provisionales; editable. Contiene BAE candidatos.',
        meta_json = COALESCE(meta_json, '{}'::jsonb) || jsonb_build_object(
            'provisional', true,
            'to_second_floor', true,
            'in_circulation', true,
            'corrected_by', '007'),
        updated_at = now()
    WHERE revision_id = v_rev_id AND code_snap = 'ESC';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN RAISE EXCEPTION '007 abort: ESC rows=%', v_updated; END IF;

    -- BAE bajo ESC (dentro de huella CIRC/ESC); pending altura/acceso/seguridad
    UPDATE warehouse.layout_elements
    SET x_m = 21.68, y_m = 4.20, width_m = 1.26, depth_m = 1.06, rotation_deg = 0,
        validation_status = 'pending_validation',
        observations = 'Candidato bajo ESC (BAE-01) en CIRC. Pendiente validación de altura, acceso y seguridad. NO autorizado. Editable.',
        meta_json = COALESCE(meta_json, '{}'::jsonb) || jsonb_build_object(
            'provisional', true, 'under_stair', true, 'corrected_by', '007'),
        constraints_json = COALESCE(constraints_json, '{}'::jsonb) || jsonb_build_object(
            'storage_kind', 'special_restricted',
            'safe_storage_authorized', false,
            'keep_stair_access_clear', true),
        updated_at = now()
    WHERE revision_id = v_rev_id AND code_snap = 'BAE-01';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN RAISE EXCEPTION '007 abort: BAE-01 rows=%', v_updated; END IF;

    UPDATE warehouse.layout_elements
    SET x_m = 21.68, y_m = 5.40, width_m = 1.26, depth_m = 1.06, rotation_deg = 0,
        validation_status = 'pending_validation',
        observations = 'Candidato bajo ESC (BAE-02) en CIRC. Pendiente validación de altura, acceso y seguridad. NO autorizado. Editable.',
        meta_json = COALESCE(meta_json, '{}'::jsonb) || jsonb_build_object(
            'provisional', true, 'under_stair', true, 'corrected_by', '007'),
        constraints_json = COALESCE(constraints_json, '{}'::jsonb) || jsonb_build_object(
            'storage_kind', 'special_restricted',
            'safe_storage_authorized', false,
            'keep_stair_access_clear', true),
        updated_at = now()
    WHERE revision_id = v_rev_id AND code_snap = 'BAE-02';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN RAISE EXCEPTION '007 abort: BAE-02 rows=%', v_updated; END IF;

    -- ESC no invade DER
    IF EXISTS (
        SELECT 1 FROM warehouse.layout_elements
        WHERE revision_id = v_rev_id AND code_snap = 'ESC'
          AND (y_m < 2.65 - 1e-9 OR y_m + depth_m > 7.35 + 1e-9)
    ) THEN
        RAISE EXCEPTION '007 abort: ESC fuera de franja CIRC (Y 2,65–7,35)';
    END IF;

    -- Consistencia parent físico ESC ↔ snapshot versionado
    IF EXISTS (
        SELECT 1
        FROM warehouse.layout_elements le
        JOIN warehouse.physical_elements pe ON pe.id = le.physical_element_id
        WHERE le.revision_id = v_rev_id AND le.code_snap = 'ESC'
          AND (
            pe.parent_id IS DISTINCT FROM v_circ_id
            OR le.parent_physical_id_snap IS DISTINCT FROM v_circ_id
            OR pe.parent_id IS DISTINCT FROM le.parent_physical_id_snap
          )
    ) THEN
        RAISE EXCEPTION '007 abort: ESC parent físico / snap inconsistente con CIRC';
    END IF;

    -- P7 intacto tras updates
    IF NOT EXISTS (
        SELECT 1 FROM warehouse.layout_elements
        WHERE revision_id = v_rev_id AND code_snap = 'P7'
          AND x_m = 19.60 AND width_m = 2.80 AND depth_m = 6.00 AND y_m = 7.35
    ) THEN
        RAISE EXCEPTION '007 abort: P7 fue alterado (no permitido)';
    END IF;

    UPDATE warehouse.layout_revisions
    SET row_version = row_version + 1,
        updated_at = now(),
        updated_by = v_actor,
        notes = COALESCE(notes, '') || E'\n[007] ESC/BAE en CIRC (frente a P7); IZ-SU compacta. P1–P7 intactos.'
    WHERE id = v_rev_id AND row_version = 2;
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN
        RAISE EXCEPTION '007 abort: bump row_version falló';
    END IF;

    INSERT INTO warehouse.layout_audit (
        revision_id, facility_id, floor_id, action, actor_label, detail_json
    ) VALUES (
        v_rev_id, v_facility_id, v_floor_id, 'fix_bc_draft_esc_izsu_bae_007', v_actor,
        jsonb_build_object(
            'codes', jsonb_build_array('IZ-SU','ESC','BAE-01','BAE-02'),
            'esc_zone', 'CIRC',
            'previous_row_version', 2,
            'new_row_version', 3,
            'publish', false,
            'p1_p7_unchanged', true
        )
    );

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
        RAISE EXCEPTION '007 abort: BAE fuera de huella ESC';
    END IF;
END $$;

SELECT id, status, row_version FROM warehouse.layout_revisions WHERE id = 1;
-- Esperado: draft, row_version=3

SELECT code_snap, x_m, y_m, width_m, depth_m
FROM warehouse.layout_elements
WHERE revision_id = 1 AND code_snap IN ('IZ-SU','ESC','BAE-01','BAE-02','P7')
ORDER BY 1;
-- IZ-SU: 21.84, 0.10, 1.26, 2.12
-- ESC:   21.58, 4.05, 1.52, 3.20  (Y 4,05–7,25 ⊂ CIRC)
-- BAE-*: dentro de ESC
-- P7:    19.60, 7.35, 2.80, 6.00  (sin cambio)

COMMIT;
