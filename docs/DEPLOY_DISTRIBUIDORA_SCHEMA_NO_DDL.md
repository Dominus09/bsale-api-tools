# Deploy / rollback — DDL fuera del hot path (planning-rows lock fix)

## Qué cambió

- `ensure_distribuidora_schema` es **NO-OP** (ya no ejecuta `001_schema.sql` ni ALTER).
- DDL solo vía: `python -m backend.jobs.apply_distribuidora_schema`
- Syncs dejan de hacer HTTP dentro de una transacción abierta (causa de
  `idle in transaction` + AccessShareLock sobre `documents`).
- `planning-rows` usa `autocommit=True` en lecturas.

## Deploy

1. Desplegar backend (código nuevo) **sin** correr migraciones automáticas en el arranque.
2. Verificar que crons/live sync ya no loguean `SQL aplicado: 001_schema.sql`.
3. Si hace falta aplicar DDL pendiente (solo en ventana de mantenimiento):

```bash
python -m backend.jobs.apply_distribuidora_schema
```

4. Opcional (ops): terminar sesiones `idle in transaction` antiguas que aún bloqueen:

```sql
SELECT pid, state, wait_event, left(query,120),
       now() - xact_start AS xact_age
FROM pg_stat_activity
WHERE datname = current_database()
  AND state = 'idle in transaction'
  AND xact_start < now() - interval '2 minutes';
-- Solo si se confirma que son zombis:
-- SELECT pg_terminate_backend(pid) FROM ...;
```

5. Probar Pre-despacho OC (`/distribuidora/orders` → Buscar) y confirmar que
   `planning-rows` responde &lt; 30 s sin ECONNRESET.

## Rollback

1. Revertir el deploy del backend al commit anterior.
2. El NO-OP de `ensure_distribuidora_schema` es seguro hacia atrás en datos;
   el rollback de código vuelve a ejecutar DDL en cada sync (riesgo de reintroducir
   el bloqueo). Preferir hotfix forward si el problema es solo el runner.
3. No hace falta revertir SQL ya aplicado (idempotente `IF NOT EXISTS`).

## Pruebas locales

```bash
python -m pytest backend/tests/test_distribuidora_schema_no_ddl_in_sync.py -q
python -m pytest backend/tests -q
```
