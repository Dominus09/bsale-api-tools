-- AWAITING_APPROVAL. BL/PISO1: nomenclatura BL01A ... BL12E.
-- Conserva IDs, geometría, histórico de pallets y revisiones archivadas.
-- BL01A sigue inutilizable. BL01E se habilita SOLO como destino previsto;
-- retirar el mueble y verificar físicamente antes de ocuparlo.
-- Aplicar con editor/planificador cerrados. No publicar el plano.
BEGIN;
SET LOCAL lock_timeout='5s';
DO $migration$
DECLARE
    fac bigint; fl bigint; rev bigint; ver integer; changed integer; plans integer;
    actor text := 'fix:warehouse_012_bl_short_codes';
BEGIN
    IF current_database() <> 'analytics' AND NOT (
        left(current_database(),length('warehouse_pallets_test'))='warehouse_pallets_test'
        AND COALESCE(current_setting('warehouse.pallets_lab',true),'off')='on'
    ) THEN
        RAISE EXCEPTION 'Destino no autorizado para warehouse 012: %', current_database();
    END IF;
    IF to_regclass('warehouse.storage_plans') IS NULL OR to_regclass('warehouse.pallets') IS NULL THEN
        RAISE EXCEPTION 'Aplicar primero warehouse 008, 010 y 011.';
    END IF;
    SELECT f.id, p.id INTO STRICT fac,fl
    FROM warehouse.facilities f JOIN warehouse.floors p ON p.facility_id=f.id
    WHERE f.company_id=3 AND f.code='BL' AND f.status='active'
      AND p.code='PISO1' AND p.status='active';

    SELECT id,row_version INTO STRICT rev,ver
    FROM warehouse.layout_revisions WHERE floor_id=fl AND status='draft' FOR UPDATE;

    IF EXISTS(SELECT 1 FROM warehouse.layout_audit a WHERE a.floor_id=fl AND a.action='bl_short_codes_012') THEN
        RAISE NOTICE '012 ya aplicada; no se modifican ediciones posteriores.';
        RETURN;
    END IF;
    IF EXISTS(SELECT 1 FROM warehouse.layout_revisions WHERE floor_id=fl AND status='published') THEN
        RAISE EXCEPTION 'BL tiene plano publicado; revisar migración manualmente.';
    END IF;

    -- No inferir identidad de nombres parciales. Exigir las 60 celdas originales.
    CREATE TEMP TABLE bl_012_mapping ON COMMIT DROP AS
    SELECT pe.id, pe.code AS old_code,
           'BL'||lpad(f::text,2,'0')||substr('ABCDE',d,1) AS new_code,
           f AS row_index,d AS depth_index,
           pe.element_type AS old_type
    FROM (SELECT f,d FROM generate_series(1,12) AS fs(f) CROSS JOIN generate_series(1,5) AS ds(d)) AS grid
    JOIN warehouse.physical_elements pe ON pe.floor_id=fl AND pe.facility_id=fac
        AND pe.status='active' AND pe.code='BL-F'||lpad(f::text,2,'0')||'-'||lpad(d::text,2,'0')
    JOIN warehouse.layout_elements le ON le.physical_element_id=pe.id
        AND le.revision_id=rev AND le.code_snap=pe.code
        AND le.element_type_snap=pe.element_type
        AND le.parent_physical_id_snap=pe.parent_id
    WHERE pe.element_type=CASE WHEN f=1 AND d IN(1,5) THEN 'unusable' ELSE 'position' END;
    -- The mapping must be exhaustive and must not collide with any other active element.
    IF (SELECT count(*) FROM bl_012_mapping)<>60 THEN
        RAISE EXCEPTION 'No se encontraron las 60 celdas originales coherentes; revisar BL antes de 012.';
    END IF;
    IF EXISTS(SELECT 1 FROM warehouse.physical_elements pe JOIN bl_012_mapping m ON lower(pe.code)=lower(m.new_code)
              WHERE pe.floor_id=fl AND pe.status='active' AND pe.id<>m.id) THEN
        RAISE EXCEPTION 'Colisión con códigos cortos existentes.';
    END IF;

    UPDATE warehouse.physical_elements pe
    SET code=m.new_code,
        name=CASE WHEN m.new_code='BL01E' THEN m.new_code WHEN pe.name=m.old_code THEN m.new_code ELSE pe.name END,
        element_type=CASE WHEN m.new_code='BL01E' THEN 'position' ELSE pe.element_type END,
        meta_json=CASE WHEN m.new_code='BL01E' THEN pe.meta_json||'{"requires_furniture_removal":true}'::jsonb ELSE pe.meta_json END,
        updated_at=now(),updated_by=actor
    FROM bl_012_mapping m WHERE pe.id=m.id;
    GET DIAGNOSTICS changed=ROW_COUNT;
    IF changed<>60 THEN RAISE EXCEPTION 'Cambio incompleto de identidades: %',changed; END IF;

    UPDATE warehouse.layout_elements le
    SET code_snap=m.new_code,
        name_snap=CASE WHEN m.new_code='BL01E' THEN m.new_code WHEN le.name_snap=m.old_code THEN m.new_code ELSE le.name_snap END,
        element_type_snap=CASE WHEN m.new_code='BL01E' THEN 'position' ELSE le.element_type_snap END,
        physical_meta_snap=CASE WHEN m.new_code='BL01E' THEN le.physical_meta_snap||'{"requires_furniture_removal":true}'::jsonb ELSE le.physical_meta_snap END,
        validation_status=CASE WHEN m.new_code='BL01E' THEN 'pending_validation' ELSE le.validation_status END,
        observations=CASE WHEN m.new_code='BL01E' THEN 'Destino previsto: retirar mueble y verificar antes de ocupar. No autoriza apilamiento.' ELSE le.observations END,
        updated_at=now()
    FROM bl_012_mapping m WHERE le.revision_id=rev AND le.physical_element_id=m.id;
    GET DIAGNOSTICS changed=ROW_COUNT;
    IF changed<>60 THEN RAISE EXCEPTION 'Cambio incompleto de snapshots draft: %',changed; END IF;

    UPDATE warehouse.layout_revisions SET row_version=ver+1,updated_at=now(),updated_by=actor
    WHERE id=rev AND row_version=ver;
    IF NOT FOUND THEN RAISE EXCEPTION 'Conflicto de versión BL'; END IF;

    -- Solo rebase de referencias vigentes de las 58 posiciones previamente utilizables.
    -- No cambia el conteo, SKU, nivel, posición ni método; no revive planes obsoletos.
    UPDATE warehouse.storage_plans sp
    SET document=jsonb_set(sp.document,'{slots}',(
        SELECT jsonb_agg(CASE WHEN (s->>'floor_id')::bigint=fl
              AND (s->>'revision_id')::bigint=rev AND (s->>'revision_version')::integer=ver
              AND EXISTS(SELECT 1 FROM bl_012_mapping m WHERE m.id=(s->>'position_id')::bigint AND m.old_type='position')
            THEN jsonb_set(s,'{revision_version}',to_jsonb(ver+1),false) ELSE s END ORDER BY ord)
        FROM jsonb_array_elements(sp.document->'slots') WITH ORDINALITY AS sl(s,ord)
    ),false),row_version=sp.row_version+1,updated_at=now(),updated_by=actor
    WHERE sp.company_id=3 AND EXISTS(
        SELECT 1 FROM jsonb_array_elements(sp.document->'slots') AS sl(s)
        JOIN bl_012_mapping m ON m.id=(s->>'position_id')::bigint AND m.old_type='position'
        WHERE (s->>'floor_id')::bigint=fl AND (s->>'revision_id')::bigint=rev
          AND (s->>'revision_version')::integer=ver
    );
    GET DIAGNOSTICS plans=ROW_COUNT;

    INSERT INTO warehouse.layout_audit(revision_id,facility_id,floor_id,action,actor_label,detail_json)
    VALUES(rev,fac,fl,'bl_short_codes_012',actor,jsonb_build_object(
        'before_version',ver,'after_version',ver+1,'cells_renamed',60,
        'positions_provisional',59,'restricted','BL01A','furniture_pending','BL01E',
        'plans_rebased',plans,'mapping',(SELECT jsonb_object_agg(old_code,new_code) FROM bl_012_mapping)));
    RAISE NOTICE 'BL 012: 60 códigos, 59 destinos provisionales, % escenarios actualizados, revisión % -> %',plans,ver,ver+1;
END $migration$;
COMMIT;
