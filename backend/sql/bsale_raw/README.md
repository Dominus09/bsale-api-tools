# backend/sql/bsale_raw

**Fase 1: sin migraciones.** El modelo conceptual está en `docs/BSALE_RAW_ARCHITECTURE.md` (sección "Tablas propuestas").

Cuando se aprueben, las migraciones de este directorio deberán:

- Crear el schema `bsale_raw` de forma idempotente (`CREATE SCHEMA IF NOT EXISTS`, `CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`).
- Usar PK `(company_id, bsale_id)` en entidades; `(company_id, variant_id, office_id)` como clave única operativa en `stocks`.
- No agregar CHECKs rígidos sobre valores externos de Bsale (estados, tipos, códigos SII).
- No agregar FKs entre tablas raw (los hijos pueden llegar antes que el padre); sólo FK a `bsale.companies(id)`.
- Guardar siempre `payload JSONB` completo + `payload_hash`.
- No tocar tablas del schema `bsale` ni `distribuidora`.
- Aplicarse manualmente con el playbook habitual; nunca desde el código de la aplicación.
