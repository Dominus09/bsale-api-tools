-- =============================================================================
-- Overlay layout BC corregido — SOLO instalaciones nuevas (post 001–004)
-- AWAITING APPROVAL — No usar en analytics productivo (usar 006 allí).
-- No hardcodea revision_id ni exige BD analytics.
-- =============================================================================

BEGIN;

DO $$
DECLARE
    v_facility_id BIGINT;
    v_floor_id BIGINT;
    v_rev_id BIGINT;
    v_row_version INT;
    v_elem_count INT;
    v_actor TEXT := 'fix:warehouse_layout_new_install';
    v_updated INT;
    r_porton RECORD;
    r_already BOOLEAN;
BEGIN
    SELECT f.id, fl.id, r.id, r.row_version
      INTO v_facility_id, v_floor_id, v_rev_id, v_row_version
    FROM warehouse.facilities f
    JOIN warehouse.floors fl ON fl.facility_id = f.id
    JOIN warehouse.layout_revisions r ON r.floor_id = fl.id
    WHERE f.code = 'BC' AND fl.code = 'PISO1' AND r.status = 'draft'
    FOR UPDATE OF r;

    IF v_rev_id IS NULL THEN
        RAISE EXCEPTION 'new-install overlay abort: no draft BC/PISO1';
    END IF;

    SELECT COUNT(*) INTO v_elem_count FROM warehouse.layout_elements WHERE revision_id = v_rev_id;
    IF v_elem_count IS DISTINCT FROM 17 THEN
        RAISE EXCEPTION 'new-install overlay abort: elements=% (esperado 17)', v_elem_count;
    END IF;

    SELECT EXISTS (
        SELECT 1 FROM warehouse.layout_elements
        WHERE revision_id = v_rev_id AND (meta_json->>'corrected_by') IN ('006', 'new_install_overlay')
    ) INTO r_already;
    IF r_already THEN
        RAISE EXCEPTION 'new-install overlay abort: layout ya corregido (corrected_by presente)';
    END IF;

    SELECT x_m, y_m, width_m, depth_m INTO r_porton
    FROM warehouse.layout_elements
    WHERE revision_id = v_rev_id AND code_snap = 'PORTON';
    IF r_porton.x_m IS DISTINCT FROM 9.375 OR r_porton.width_m IS DISTINCT FROM 4.35 THEN
        RAISE EXCEPTION 'new-install overlay abort: PORTON no es geometría seed original';
    END IF;

    UPDATE warehouse.layout_elements SET
        x_m=0, y_m=4.675, width_m=0.40, depth_m=4.35, rotation_deg=0,
        validation_status='pending_validation',
        observations='Portón 4,35 m en muro corto de frente (x≈0). Y centrada provisional.',
        meta_json = COALESCE(meta_json,'{}'::jsonb) || jsonb_build_object('provisional',true,'corrected_by','new_install_overlay'),
        updated_at=now()
    WHERE revision_id=v_rev_id AND code_snap='PORTON';

    UPDATE warehouse.layout_elements SET
        x_m=0, y_m=7.35, width_m=2.80, depth_m=6.00, rotation_deg=0,
        validation_status='pending_validation',
        observations='Oficina: 2,80 m longitudinales (X 0–2,80). depth transversal provisional (=DER).',
        meta_json = COALESCE(meta_json,'{}'::jsonb) || jsonb_build_object('provisional',true,'corrected_by','new_install_overlay'),
        updated_at=now()
    WHERE revision_id=v_rev_id AND code_snap='OFC';

    UPDATE warehouse.layout_elements SET x_m=2.80, y_m=7.35, width_m=2.80, depth_m=6.00, rotation_deg=0,
        validation_status='pending_validation',
        observations='Pasillo 1: X 2,80–5,60 (preliminar).',
        meta_json = COALESCE(meta_json,'{}'::jsonb) || jsonb_build_object('provisional',true,'corrected_by','new_install_overlay'),
        updated_at=now()
    WHERE revision_id=v_rev_id AND code_snap='P1';
    UPDATE warehouse.layout_elements SET x_m=5.60, y_m=7.35, width_m=2.80, depth_m=6.00, rotation_deg=0,
        validation_status='pending_validation',
        observations='Pasillo 2: X 5,60–8,40 (preliminar).',
        meta_json = COALESCE(meta_json,'{}'::jsonb) || jsonb_build_object('provisional',true,'corrected_by','new_install_overlay'),
        updated_at=now()
    WHERE revision_id=v_rev_id AND code_snap='P2';
    UPDATE warehouse.layout_elements SET x_m=8.40, y_m=7.35, width_m=2.80, depth_m=6.00, rotation_deg=0,
        validation_status='pending_validation',
        observations='Pasillo 3: X 8,40–11,20 (preliminar).',
        meta_json = COALESCE(meta_json,'{}'::jsonb) || jsonb_build_object('provisional',true,'corrected_by','new_install_overlay'),
        updated_at=now()
    WHERE revision_id=v_rev_id AND code_snap='P3';
    UPDATE warehouse.layout_elements SET x_m=11.20, y_m=7.35, width_m=2.80, depth_m=6.00, rotation_deg=0,
        validation_status='pending_validation',
        observations='Pasillo 4: X 11,20–14,00 (preliminar).',
        meta_json = COALESCE(meta_json,'{}'::jsonb) || jsonb_build_object('provisional',true,'corrected_by','new_install_overlay'),
        updated_at=now()
    WHERE revision_id=v_rev_id AND code_snap='P4';
    UPDATE warehouse.layout_elements SET x_m=14.00, y_m=7.35, width_m=2.80, depth_m=6.00, rotation_deg=0,
        validation_status='pending_validation',
        observations='Pasillo 5: X 14,00–16,80 (preliminar).',
        meta_json = COALESCE(meta_json,'{}'::jsonb) || jsonb_build_object('provisional',true,'corrected_by','new_install_overlay'),
        updated_at=now()
    WHERE revision_id=v_rev_id AND code_snap='P5';
    UPDATE warehouse.layout_elements SET x_m=16.80, y_m=7.35, width_m=2.80, depth_m=6.00, rotation_deg=0,
        validation_status='pending_validation',
        observations='Pasillo 6: X 16,80–19,60 (preliminar).',
        meta_json = COALESCE(meta_json,'{}'::jsonb) || jsonb_build_object('provisional',true,'corrected_by','new_install_overlay'),
        updated_at=now()
    WHERE revision_id=v_rev_id AND code_snap='P6';
    UPDATE warehouse.layout_elements SET x_m=19.60, y_m=7.35, width_m=2.80, depth_m=6.00, rotation_deg=0,
        validation_status='pending_validation',
        observations='Pasillo 7: X 19,60–22,40 (preliminar). Residual 0,70 m al fondo.',
        meta_json = COALESCE(meta_json,'{}'::jsonb) || jsonb_build_object('provisional',true,'corrected_by','new_install_overlay'),
        updated_at=now()
    WHERE revision_id=v_rev_id AND code_snap='P7';

    -- ESC / IZ-SU / BAE: ESC en CIRC (frente a P7), no en DER
    UPDATE warehouse.physical_elements pe
    SET parent_id = (
            SELECT id FROM warehouse.physical_elements
            WHERE floor_id = pe.floor_id AND status='active' AND code='CIRC'
        ),
        updated_at=now()
    WHERE pe.status='active' AND pe.code='ESC'
      AND pe.floor_id IN (
          SELECT fl.id FROM warehouse.floors fl
          JOIN warehouse.facilities f ON f.id=fl.facility_id
          WHERE f.code='BC' AND fl.code='PISO1'
      );

    UPDATE warehouse.layout_elements SET
        x_m=21.58, y_m=4.05, width_m=1.52, depth_m=3.20, rotation_deg=0,
        parent_physical_id_snap = (
            SELECT pe.id FROM warehouse.physical_elements pe
            JOIN warehouse.floors fl ON fl.id=pe.floor_id
            JOIN warehouse.facilities f ON f.id=fl.facility_id
            WHERE f.code='BC' AND fl.code='PISO1' AND pe.code='CIRC' AND pe.status='active'
            LIMIT 1
        ),
        validation_status='pending_validation',
        observations='Escalera en extremo posterior de CIRC, frente a P7. No ocupa DER/P7. Dims provisionales.',
        meta_json = COALESCE(meta_json,'{}'::jsonb) || jsonb_build_object('provisional',true,'to_second_floor',true,'in_circulation',true,'corrected_by','new_install_overlay'),
        updated_at=now()
    WHERE revision_id=v_rev_id AND code_snap='ESC';

    UPDATE warehouse.layout_elements SET
        x_m=21.84, y_m=0.10, width_m=1.26, depth_m=2.12, rotation_deg=0,
        validation_status='pending_validation',
        observations='IZ-SU compacta ≈2,67 m² (~2 pallets) en extremo IZ del fondo. Sin posiciones pallet.',
        meta_json = COALESCE(meta_json,'{}'::jsonb) || jsonb_build_object('provisional',true,'storage_kind','loose_no_pallet','approx_area_m2',2.67,'corrected_by','new_install_overlay'),
        updated_at=now()
    WHERE revision_id=v_rev_id AND code_snap='IZ-SU';

    UPDATE warehouse.layout_elements SET
        x_m=21.68, y_m=4.20, width_m=1.26, depth_m=1.06, rotation_deg=0,
        validation_status='pending_validation',
        observations='BAE-01 bajo ESC en CIRC. NO almacenamiento seguro autorizado.',
        meta_json = COALESCE(meta_json,'{}'::jsonb) || jsonb_build_object('provisional',true,'under_stair',true,'corrected_by','new_install_overlay'),
        constraints_json = COALESCE(constraints_json,'{}'::jsonb) || jsonb_build_object(
            'storage_kind','special_restricted','safe_storage_authorized',false,'keep_stair_access_clear',true),
        updated_at=now()
    WHERE revision_id=v_rev_id AND code_snap='BAE-01';

    UPDATE warehouse.layout_elements SET
        x_m=21.68, y_m=5.40, width_m=1.26, depth_m=1.06, rotation_deg=0,
        validation_status='pending_validation',
        observations='BAE-02 bajo ESC en CIRC. NO almacenamiento seguro autorizado.',
        meta_json = COALESCE(meta_json,'{}'::jsonb) || jsonb_build_object('provisional',true,'under_stair',true,'corrected_by','new_install_overlay'),
        constraints_json = COALESCE(constraints_json,'{}'::jsonb) || jsonb_build_object(
            'storage_kind','special_restricted','safe_storage_authorized',false,'keep_stair_access_clear',true),
        updated_at=now()
    WHERE revision_id=v_rev_id AND code_snap='BAE-02';

    UPDATE warehouse.layout_elements SET
        observations='Límite 23,10 × 13,70 m. Entrada muro corto x≈0.',
        meta_json = COALESCE(meta_json,'{}'::jsonb) || jsonb_build_object('provisional',true,'corrected_by','new_install_overlay'),
        updated_at=now()
    WHERE revision_id=v_rev_id AND code_snap='BC-BND';

    UPDATE warehouse.layout_elements SET
        meta_json = COALESCE(meta_json,'{}'::jsonb) || jsonb_build_object('corrected_by','new_install_overlay','geometry_unchanged',true),
        updated_at=now()
    WHERE revision_id=v_rev_id AND code_snap IN ('IZ','CIRC','DER');

    UPDATE warehouse.layout_revisions
    SET row_version = row_version + 1, updated_at=now(), updated_by=v_actor
    WHERE id = v_rev_id;
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN RAISE EXCEPTION 'new-install overlay abort: bump row_version'; END IF;

    INSERT INTO warehouse.layout_audit (revision_id, facility_id, floor_id, action, actor_label, detail_json)
    VALUES (v_rev_id, v_facility_id, v_floor_id, 'apply_corrected_bc_layout_new_install', v_actor,
            jsonb_build_object('overlay', true, 'publish', false));

    IF EXISTS (
        SELECT 1 FROM warehouse.layout_elements bae
        JOIN warehouse.layout_elements esc ON esc.revision_id=bae.revision_id AND esc.code_snap='ESC'
        WHERE bae.revision_id=v_rev_id AND bae.code_snap IN ('BAE-01','BAE-02')
          AND (bae.x_m < esc.x_m OR bae.y_m < esc.y_m
               OR bae.x_m+bae.width_m > esc.x_m+esc.width_m
               OR bae.y_m+bae.depth_m > esc.y_m+esc.depth_m)
    ) THEN
        RAISE EXCEPTION 'new-install overlay abort: BAE fuera de ESC';
    END IF;
END $$;

SELECT code_snap, x_m, y_m, width_m, depth_m
FROM warehouse.layout_elements le
JOIN warehouse.layout_revisions r ON r.id = le.revision_id
JOIN warehouse.floors fl ON fl.id = r.floor_id
JOIN warehouse.facilities f ON f.id = fl.facility_id
WHERE f.code='BC' AND fl.code='PISO1' AND r.status='draft'
ORDER BY 1;

COMMIT;
