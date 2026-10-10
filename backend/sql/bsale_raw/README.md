# backend/sql/bsale_raw

**Migraciones generadas, NO aplicadas.** Diseño y decisiones: `docs/BSALE_RAW_PHASE3_SQL_PROPOSAL.md`.

Orden de aplicación manual (primero en entorno de prueba). Paso a paso: `docs/BSALE_RAW_PHASE3_APPLY_RUNBOOK.md`.

1. `001_schema_sources.sql` … `008_webhooks.sql` (cada una en su propia transacción).
2. `verify_bsale_raw.sql`: verificación de sólo lectura; falla con `RAISE EXCEPTION` ante drift.
3. `009_seed_sources.sql`: seed idempotente (paso separado, requiere aprobación).
4. `010_variant_prices_missing_since.sql`: agrega `variant_prices.missing_since` (no destructiva, nullable, sin DEFAULT). Aplicar ANTES de desplegar/ejecutar `sync-prices`; luego `verify_bsale_raw.sql`.

Reglas:

- `IF NOT EXISTS` sólo para el bootstrap inicial. Las migraciones posteriores deben ser explícitas y actualizar `verify_bsale_raw.sql` (el bloque GENERATED lo valida `backend/tests/bsale_raw/test_bsale_raw_migrations.py`).
- Una columna por línea y constraints con nombre (`CONSTRAINT pk_/fk_/uq_/ck_…`): lo exige el parser de los tests.
- Sólo FK a `bsale.companies (company_id)` (`BIGINT`) y FK internas de control. No hay FK entre tablas raw de datos.
- No agregar CHECKs sobre valores externos de Bsale ni sobre `scope`.
- Guardar siempre `payload JSONB` completo + `payload_hash` + `api_fetched_at`.
- No guardar secretos ni headers de request; `sources.token_env` es el nombre de la variable.
- No tocar los schemas `bsale` ni `distribuidora`; no usar `DROP` / `TRUNCATE` / `DELETE` / `UPDATE` / `ALTER` en estas migraciones. Única excepción: `ALTER TABLE bsale_raw.<tabla> ADD COLUMN <col> <TIPO>` (una columna por sentencia, sin `IF NOT EXISTS`).
- Rollback con `DROP SCHEMA bsale_raw CASCADE` sólo antes del cutover (ver la propuesta, §8).
