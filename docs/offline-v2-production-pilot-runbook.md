# Offline V2 — activación productiva (runbook)

**Alcance:** Offline V2 para **todos** los choferes autenticados, solo en despachos asignados, con picking/documentos validados. Sin allowlists de usuario ni despacho.

**NO ejecutar SQL ni redeploy desde Cursor.**

## 0. Estado verificado (solo lectura)

| Ítem | Estado |
|------|--------|
| CHECK 059 / 060 | Ya aplicados |
| Tabla `delivery_offline_ops` (061) | Aplicar si aún no existe |

## 1. Variables API (Coolify)

Arranque seguro:

```env
OFFLINE_SYNC_WRITES_ENABLED=false
```

**No** usar `OFFLINE_SYNC_PILOT_ALLOWLIST` ni `OFFLINE_SYNC_PILOT_PLAN_ALLOWLIST` (obsoletas).

PWA: no definir `OFFLINE_V2_LAB_ENABLED`. `/pruebas/offline-v2-lab` → 404 en `*.quillotana.cl`.

## 2. SQL 061 (si falta)

1. Respaldo verificable.
2. Preflight: `backend/sql/distribuidora/061_read_only_status_check.sql`
3. DDL: `061_delivery_offline_ops_AWAITING_APPROVAL.sql`
4. Post-check tabla + índices + UNIQUE `(driver_user_id, idempotency_key)`

Desactivación sin pérdida: `OFFLINE_SYNC_WRITES_ENABLED=false` (no DROP).

## 3. Coolify (manual)

1. Respaldo PG (+ 061 si falta).
2. Redeploy **API** con writes **OFF**.
3. `GET /distribuidora/pwa/driver/offline-ops/pilot-status` → `offline_v2_enabled=false`.
4. Redeploy **PWA**.
5. Verificar Offline V1 / online.
6. Activar: `OFFLINE_SYNC_WRITES_ENABLED=true`
7. Verificar chofer: `offline_v2_enabled=true`, UI Offline V2 en despacho descargado.

## 4. Kill switch

| Acción | Cómo |
|--------|------|
| Activar | `OFFLINE_SYNC_WRITES_ENABLED=true` |
| Detener nuevas escrituras | `OFFLINE_SYNC_WRITES_ENABLED=false` |

Conservar outbox del dispositivo y filas en `delivery_offline_ops`.

## 5. Garantías que permanecen

- Perfil chofer + acceso al despacho asignado
- Validación de picking / documentos
- Idempotencia, GPS diferido, fotos R2
- Outbox no se borra en logout/401
