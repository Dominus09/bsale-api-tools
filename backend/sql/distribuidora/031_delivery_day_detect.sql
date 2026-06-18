-- Funciones opcionales de detección de día de entrega (pre-despacho).
-- La lógica activa está embebida en orders_service.py vía sql_resolve_delivery_day().

CREATE OR REPLACE FUNCTION distribuidora.detect_delivery_day_from_text(src text)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT (regexp_match(
    translate(lower(coalesce(src, '')), 'áéíóúü', 'aeiouu'),
    '.*\m(lunes|martes|miercoles|jueves|viernes|sabado|domingo)\M'
  ))[1];
$$;

CREATE OR REPLACE FUNCTION distribuidora.resolve_delivery_day(
    observaciones text,
    comments text,
    dia_atencion text
)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $$
  SELECT COALESCE(
    distribuidora.detect_delivery_day_from_text(NULLIF(BTRIM(observaciones), '')),
    CASE
        WHEN NULLIF(BTRIM(observaciones), '') IS NULL
            THEN distribuidora.detect_delivery_day_from_text(NULLIF(BTRIM(comments), ''))
        ELSE NULL
    END,
    distribuidora.detect_delivery_day_from_text(NULLIF(BTRIM(dia_atencion), ''))
  );
$$;
