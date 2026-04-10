-- Poblar y sincronizar bsale.rutero desde bsale.clients (empresa 3, vendedores de ruta).
-- No elimina filas. No modifica filas en bsale.clients.
-- Requiere UNIQUE (company_id, bsale_id) en bsale.rutero (rutero_schema.sql).
--
-- Si la tabla sigue vacía tras ejecutar:
--   1) Revisa mensajes de error: cualquier error en el bloque hace ROLLBACK y 0 filas.
--   2) Comprueba que haya filas que cumplan el filtro (ejecutar aparte):
--        SELECT COUNT(*) FROM bsale.clients WHERE company_id = 3;
--        SELECT DISTINCT TRIM(vendedor::text) AS v FROM bsale.clients WHERE company_id = 3;
--   3) Los valores de vendedor deben coincidir exactamente (mayúsculas/espacios) o usa el filtro
--      LOWER(TRIM(...)) del script.
--
-- dia_extra en rutero: si bsale.clients tiene columna dia_extra, cambia NULL::text por c.dia_extra
-- y en el CASE añade: OR LOWER(COALESCE(c.dia_extra, '')) = 'telefonico'.

BEGIN;

INSERT INTO bsale.rutero (
    company_id,
    bsale_id,
    first_name,
    last_name,
    code,
    phone,
    company,
    facebook,
    city,
    municipality,
    address,
    created,
    updated,
    dia_atencion,
    dia_extra,
    nombre_fantasia,
    vendedor,
    rut_clean,
    lat,
    lon,
    tipo_atencion,
    activo
)
SELECT
    c.company_id,
    c.bsale_id,
    c.first_name,
    c.last_name,
    c.code,
    c.phone,
    c.company,
    c.facebook,
    c.city,
    c.municipality,
    c.address,
    c.created,
    c.updated,
    c.dia_atencion,
    NULL::text AS dia_extra,
    c.nombre_fantasia,
    c.vendedor,
    c.rut_clean,
    c.lat,
    c.lon,
    CASE
        WHEN LOWER(COALESCE(c.dia_atencion, '')) = 'telefonico'
        THEN 'telefonico'
        ELSE 'terreno'
    END,
    TRUE
FROM bsale.clients AS c
WHERE c.company_id = 3
  AND LOWER(TRIM(COALESCE(c.vendedor::text, ''))) IN (
      'vendedor_1',
      'vendedor_2',
      'vendedor_3',
      'vendedor_4'
  )
ON CONFLICT (company_id, bsale_id) DO UPDATE SET
    company_id = EXCLUDED.company_id,
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name,
    code = EXCLUDED.code,
    phone = EXCLUDED.phone,
    company = EXCLUDED.company,
    facebook = EXCLUDED.facebook,
    city = EXCLUDED.city,
    municipality = EXCLUDED.municipality,
    address = EXCLUDED.address,
    created = EXCLUDED.created,
    updated = EXCLUDED.updated,
    dia_atencion = EXCLUDED.dia_atencion,
    dia_extra = EXCLUDED.dia_extra,
    nombre_fantasia = EXCLUDED.nombre_fantasia,
    vendedor = EXCLUDED.vendedor,
    rut_clean = EXCLUDED.rut_clean,
    lat = EXCLUDED.lat,
    lon = EXCLUDED.lon,
    tipo_atencion = EXCLUDED.tipo_atencion,
    activo = TRUE;

UPDATE bsale.rutero AS r
SET activo = FALSE
WHERE r.company_id = 3
  AND NOT EXISTS (
      SELECT 1
      FROM bsale.clients AS c
      WHERE c.company_id = 3
        AND c.bsale_id = r.bsale_id
        AND LOWER(TRIM(COALESCE(c.vendedor::text, ''))) IN (
            'vendedor_1',
            'vendedor_2',
            'vendedor_3',
            'vendedor_4'
        )
  );

COMMIT;
