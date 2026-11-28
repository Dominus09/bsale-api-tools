-- AWAITING_APPROVAL. Galpon de Lata, draft geometrico APROXIMADO.
-- Fuente: croquis 4 pasillos x 3 filas x 5 fondos, medidas del usuario.
-- No stock, no pallets observados, no publicacion ni autorizacion de apilamiento.
-- X: profundidad desde patio hacia fondo; Y: izquierda a derecha mirando desde patio.
-- Diferencia de medidas: 3x1.26 + separaciones entre postes no reproduce todos los
-- 70 cm declarados. Separacion F3/F4 modelada ~0.55 m, pendiente de verificar.
-- Banda sensores 0.50 m en frente es una HIPOTESIS, verificar en terreno.
-- F1-01 espacio muerto y F1-05 mueble pendiente retiro: excluidos del conteo.
BEGIN;
DO $$
DECLARE
    fac bigint; fl bigint; rev bigint; bnd bigint; parent bigint; pid bigint;
    actor text := 'seed:warehouse_011';
    starts numeric[] := ARRAY[0.00,4.00,8.85,13.70];
    spans numeric[] := ARRAY[4.00,4.85,4.85,3.90];
    ys numeric[] := ARRAY[0.02,1.28,2.54,4.35,5.61,7.57,9.08,10.94,12.20,13.76,15.02,16.28];
    m integer; f integer; d integer; k integer;
    c text; n text; typ text; xx numeric; yy numeric; ww numeric; dd numeric;
    entry record; meta jsonb;
BEGIN
    -- Never overwrite existing BL or its user-edited layout.
    IF EXISTS (SELECT 1 FROM warehouse.facilities WHERE company_id=3 AND code='BL') THEN
        RAISE NOTICE 'BL ya existe; no se modifica.'; RETURN;
    END IF;
    INSERT INTO warehouse.facilities(code,name,company_id,bsale_office_id,meta_json,created_by,updated_by)
    VALUES('BL','Bodega de Lata',3,1,'{"provisional":true,"same_commercial_office_as_bc":true}'::jsonb,actor,actor) RETURNING id INTO fac;
    INSERT INTO warehouse.floors(facility_id,code,name,level_index,length_m,width_m,meta_json,created_by,updated_by)
    VALUES(fac,'PISO1','Piso 1',1,6.0,17.6,'{"usable_depth_m":5.5,"orientation":"fila 1 a la izquierda mirando desde patio","gate":"corredizo doble, medidas por confirmar","provisional":true}'::jsonb,actor,actor) RETURNING id INTO fl;
    INSERT INTO warehouse.layout_revisions(floor_id,status,label,notes,created_by,updated_by)
    VALUES(fl,'draft','BL — croquis métrico preliminar','Medidas aproximadas; revisar sensores, postes, accesos y espacios muertos. No autoriza apilamiento.',actor,actor) RETURNING id INTO rev;
    INSERT INTO warehouse.physical_elements(facility_id,floor_id,element_type,code,name,created_by,updated_by)
    VALUES(fac,fl,'facility_boundary','BL-BND','Límite BL',actor,actor) RETURNING id INTO bnd;
    INSERT INTO warehouse.layout_elements(revision_id,physical_element_id,code_snap,name_snap,element_type_snap,x_m,y_m,width_m,depth_m,validation_status,observations)
    VALUES(rev,bnd,'BL-BND','Límite BL','facility_boundary',0,0,6,17.6,'pending_validation','Dimensiones aproximadas del usuario.');
    FOR entry IN SELECT * FROM (VALUES
        ('BL-SENS','Banda sensores (hipótesis)','unusable',0.0,0.0,0.5,17.6),
        ('BL-POSTE1','Poste 1 (referencia)','pillar',0.05,3.95,0.1,0.1),
        ('BL-POSTE2','Poste 2 (referencia)','pillar',0.05,8.80,0.1,0.1),
        ('BL-POSTE3','Poste 3 (referencia)','pillar',0.05,13.65,0.1,0.1)
    ) AS v(code,name,typ,x,y,w,h) LOOP
        INSERT INTO warehouse.physical_elements(facility_id,floor_id,parent_id,element_type,code,name,created_by,updated_by)
        VALUES(fac,fl,bnd,entry.typ,entry.code,entry.name,actor,actor) RETURNING id INTO pid;
        INSERT INTO warehouse.layout_elements(revision_id,physical_element_id,code_snap,name_snap,element_type_snap,parent_physical_id_snap,x_m,y_m,width_m,depth_m,validation_status,observations)
        VALUES(rev,pid,entry.code,entry.name,entry.typ,bnd,entry.x,entry.y,entry.w,entry.h,'pending_validation','Ubicación y dimensiones exactas pendientes.');
    END LOOP;
    FOR m IN 1..4 LOOP
        c := 'BL-P'||m; n := 'Pasillo '||m;
        INSERT INTO warehouse.physical_elements(facility_id,floor_id,parent_id,element_type,code,name,created_by,updated_by)
        VALUES(fac,fl,bnd,'aisle',c,n,actor,actor) RETURNING id INTO parent;
        INSERT INTO warehouse.layout_elements(revision_id,physical_element_id,code_snap,name_snap,element_type_snap,parent_physical_id_snap,x_m,y_m,width_m,depth_m,validation_status,observations)
        VALUES(rev,parent,c,n,'aisle',bnd,0.5,starts[m],5.5,spans[m],'pending_validation','Módulo entre postes, no pasillo de tránsito de grúa.');
        FOR f IN 1..3 LOOP
            k := (m-1)*3+f;
            FOR d IN 1..5 LOOP
                c := 'BL-F'||lpad(k::text,2,'0')||'-'||lpad(d::text,2,'0');
                typ := 'position'; n := c;
                IF k=1 AND d IN (1,5) THEN
                    typ := 'unusable'; n := CASE WHEN d=1 THEN 'Espacio muerto' ELSE 'Mueble pendiente retiro' END;
                END IF;
                xx := 0.52+(d-1)*1.10; yy := ys[k]; ww := 1.06; dd := 1.26;
                meta := jsonb_build_object('provisional',true,'global_row',k,'depth_index',d,'front_access',true);
                INSERT INTO warehouse.physical_elements(facility_id,floor_id,parent_id,element_type,code,name,meta_json,created_by,updated_by)
                VALUES(fac,fl,parent,typ,c,n,meta,actor,actor) RETURNING id INTO pid;
                INSERT INTO warehouse.layout_elements(revision_id,physical_element_id,code_snap,name_snap,element_type_snap,parent_physical_id_snap,physical_meta_snap,x_m,y_m,width_m,depth_m,validation_status,meta_json,observations)
                VALUES(rev,pid,c,n,typ,parent,meta,xx,yy,ww,dd,'pending_validation',meta,'Referencia de croquis; no capacidad certificada.');
            END LOOP;
        END LOOP;
    END LOOP;
    INSERT INTO warehouse.layout_audit(revision_id,facility_id,floor_id,action,actor_label,detail_json)
    VALUES(rev,fac,fl,'seed_bl_draft_applied',actor,'{"positions":58,"excluded":2,"provisional":true}'::jsonb);
END $$;
COMMIT;
