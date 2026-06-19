-- Actualiza precio diesel legado 1200 → 1500 (solo si sigue en el valor por defecto antiguo).

UPDATE distribuidora.system_config
SET value_json = '{"clp": 1500}'::jsonb,
    updated_at = NOW()
WHERE key = 'diesel_price_per_liter'
  AND (
    (value_json->>'clp')::numeric = 1200
    OR value_json::text = '1200'
  );
