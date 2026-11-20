# Offline V2 — Piloto productivo controlado (runbook)

**Objetivo:** probar Offline V2 en `app.quillotana.cl` → `api.quillotana.cl` con **un usuario y un despacho** autorizados. El resto de choferes permanece en Offline V1 / flujo online.

**NO ejecutar SQL ni redeploy desde Cursor.** Este documento es para el administrador en Coolify / pgAdmin.

## 0. Estado verificado (solo lectura, analytics)

| Ítem | Estado |
|------|--------|
| `delivery_visits` / evidence / collections | Presentes |
| CHECK 059 `cash_and_transfer` | **Ya aplicado** |
| CHECK 060 `check_declared` | **Ya aplicado** |
| Tabla `delivery_offline_ops` (061) | **NO aplicada** |

No reaplicar 059 ni 060.

## 1. Variables de entorno (API Coolify)

Arranque seguro (writes OFF):

```env
OFFLINE_SYNC_WRITES_ENABLED=false
OFFLINE_SYNC_PILOT_ALLOWLIST=
OFFLINE_SYNC_PILOT_PLAN_ALLOWLIST=
```

PWA (Coolify `app.quillotana.cl`):

```env
# NO definir OFFLINE_V2_LAB_ENABLED (o =0)
# NO usar NEXT_PUBLIC_OFFLINE_SYNC_WRITES para otorgar permisos
```

`/pruebas/offline-v2-lab` responde **404** en `*.quillotana.cl` siempre.

## 2. SQL 061 — solo administrador

### 2.1 Respaldo verificable

Respaldar al menos:

- esquema `distribuidora` (o DB completa analytics)
- confirmar restore-test o checksum del dump

### 2.2 Preflight (solo lectura)

Ejecutar: `backend/sql/distribuidora/061_read_only_status_check.sql`

Confirmar `offline_ops_table_exists = false` y dependencias de visitas OK.

### 2.3 Aplicar DDL

Archivo: `backend/sql/distribuidora/061_delivery_offline_ops_AWAITING_APPROVAL.sql`

Es aditivo (`CREATE TABLE IF NOT EXISTS` + índices). **No** usa DROP.

### 2.4 Post-check

```sql
SELECT to_regclass('distribuidora.delivery_offline_ops') IS NOT NULL AS ok;
SELECT indexname FROM pg_indexes
 WHERE schemaname='distribuidora' AND tablename='delivery_offline_ops'
 ORDER BY 1;
SELECT conname FROM pg_constraint
 WHERE conrelid = 'distribuidora.delivery_offline_ops'::regclass;
```

### 2.5 Desactivación sin pérdida

Kill switch (no DROP):

```env
OFFLINE_SYNC_WRITES_ENABLED=false
OFFLINE_SYNC_PILOT_ALLOWLIST=
OFFLINE_SYNC_PILOT_PLAN_ALLOWLIST=
```

Conservar filas en `delivery_offline_ops` y outbox del dispositivo.

## 3. Procedimiento Coolify (manual)

1. Respaldo PG + verificación.
2. Aplicar 061 (paso 2).
3. Redeploy **API** con writes **OFF** y allowlists vacías.
4. Verificar API: `GET /distribuidora/pwa/driver/offline-ops/pilot-status` → `offline_v2_enabled=false`.
5. Redeploy **PWA** (sin `OFFLINE_V2_LAB_ENABLED`).
6. Verificar login chofer normal: Offline V1 OK; sin botones Offline V2.
7. Configurar piloto (solo env API, sin redeploy de código si Coolify permite hot-env; si no, redeploy API):

```env
OFFLINE_SYNC_WRITES_ENABLED=true
OFFLINE_SYNC_PILOT_ALLOWLIST=<user_id_piloto>
OFFLINE_SYNC_PILOT_PLAN_ALLOWLIST=<dispatch_plan_id>
```

8. Verificar piloto: mismo GET con sesión del usuario → `offline_v2_enabled=true`, `authorized_plan_ids` correcto.
9. Usuario fuera de allowlist → sync POST → **403** `PILOT_USER_NOT_ALLOWED`.
10. Despacho no listado → **403** `PILOT_PLAN_NOT_ALLOWED`.

## 4. Activar / desactivar piloto

| Acción | Cómo |
|--------|------|
| Activar | writes=true + user_id + plan_id en allowlists |
| Kill switch | writes=false (inmediato para nuevas escrituras) |
| Quitar usuario | quitar id de `OFFLINE_SYNC_PILOT_ALLOWLIST` |
| Quitar despacho | quitar id de `OFFLINE_SYNC_PILOT_PLAN_ALLOWLIST` |

`localStorage` / `NEXT_PUBLIC_*` **no** habilitan escrituras en producción.

## 5. Primera prueba iPhone (admin)

Solo visita auténtica abierta, no histórica cerrada.

1. Login chofer piloto en `https://app.quillotana.cl`
2. Descargar despacho piloto (picking final)
3. Modo avión → entrega completa + local cerrado con foto
4. Cerrar/reabrir PWA → persistencia local
5. Recuperar red → sync automático
6. Read-only PG: una fila `delivery_offline_ops` + una visita por op; GPS `pending_review`
7. WhatsApp manual **después** del ACK
8. Reintento idempotente: misma `idempotency_key` → sin duplicado

Si la respuesta es incierta: **reconciliar idempotency_key** antes de re-registrar.

## 6. Incidentes

- Kill switch writes OFF
- Conservar outbox del iPhone
- No borrar visitas confirmadas
- Flujos online / Offline V1 siguen disponibles
