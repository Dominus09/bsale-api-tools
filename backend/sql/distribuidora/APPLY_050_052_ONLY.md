# Aplicar solo 050–052 (+ auth 069) — sin runner completo

## Por qué no usar el runner completo en go-live

`python -m backend.jobs.apply_distribuidora_schema` **re-ejecuta**
`DISTRIBUIDORA_SCHEMA_FILES` = `001` … `052` en una sola transacción.

Aunque muchos scripts usan `IF NOT EXISTS` / `CREATE OR REPLACE`:

- Reaplica `CREATE OR REPLACE VIEW`, `ALTER TABLE` y al menos un `UPDATE`
  histórico (`033_diesel_price_default_1500.sql`, y el `UPDATE` de defaults en `050`).
- Esos `ALTER` / refresh de vistas piden locks fuertes y pueden encolar
  `planning-rows` / predespacho ORS durante la ventana.
- **No** es “solo 050–052”: el runner no tiene filtro por rango.

**Recomendación para publicar 3B:** aplicar **solo** los SQL nuevos, con `psql`,
fuera de punta, tras respaldo.

## Orden definitivo

```bash
# 0) Respaldo / snapshot analytics (ops)

# 1) Auth capability (NO es runner distribuidora)
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f backend/sql/069_user_erp_capabilities.sql

# 2) Solo DDL 3B.3 / 3B.3.2
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f backend/sql/distribuidora/050_dispatch_plan_planning_origin.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f backend/sql/distribuidora/051_simple_planning_sessions.sql
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f backend/sql/distribuidora/052_dispatch_plan_driver_user.sql
```

## Prechequeo (READ-ONLY)

```sql
SELECT current_database(), now();

SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'distribuidora' AND table_name = 'dispatch_plan'
  AND column_name IN ('planning_origin', 'ors_metrics_calculated', 'driver_user_id')
ORDER BY 1;

SELECT to_regclass('distribuidora.simple_planning_sessions');
SELECT to_regclass('app.user_erp_capabilities');

-- FK chofer: bsale.users debe existir
SELECT to_regclass('bsale.users');
SELECT to_regclass('distribuidora.trucks');  -- 051 referencia trucks(id)
```

Esperado **antes** del go-live 3B: las tres columnas ausentes o parciales;
`simple_planning_sessions` y `user_erp_capabilities` NULL.

## Postchequeo

```sql
SELECT column_name, data_type, column_default
FROM information_schema.columns
WHERE table_schema = 'distribuidora' AND table_name = 'dispatch_plan'
  AND column_name IN ('planning_origin', 'ors_metrics_calculated', 'driver_user_id')
ORDER BY 1;

SELECT conname FROM pg_constraint
WHERE conrelid = 'distribuidora.dispatch_plan'::regclass
  AND conname = 'dispatch_plan_planning_origin_check';

SELECT to_regclass('distribuidora.simple_planning_sessions'),
       to_regclass('app.user_erp_capabilities');

-- Defaults de 050 no deben romper planes ORS existentes
SELECT planning_origin, ors_metrics_calculated, count(*)
FROM distribuidora.dispatch_plan
GROUP BY 1, 2
ORDER BY 1, 2;
```

Esperado: columnas presentes; check constraint; planes previos
`planning_origin='ors'` y `ors_metrics_calculated=true` (salvo filas nuevas
`simple_list`).

## Cuándo sí usar el runner completo

Solo en **BD vacía / staging recreado** o mantenimiento largo donde se acepte
reaplicar 001–052. No es el camino preferido para analytics productivo en este
go-live.

## Concurrencia (bloqueo restante)

Sin Postgres aislado en el entorno de desarrollo. Procedimiento cuando exista DSN:

```bash
export SIMPLE_PLANNING_TEST_DSN='postgresql://…@…/sp_concurrency_test'
# BD vacía, usuario sin privilegios sobre analytics
python -m pytest backend/tests/test_dispatch_plan_oc_concurrency_pg.py -q
```

No usar credenciales de analytics.
