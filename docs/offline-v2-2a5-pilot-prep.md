# Offline V2 — 2A.5 preparación de piloto (NO publicación)

Estado: **diseño + laboratorio**. Sin push, sin SQL productivo, sin redeploy productivo.

## 1. Staging aislado (fuera de `*.quillotana.cl`)

El bloqueo PWA `isProductionQuillotanaHost` incluye **cualquier** host que contenga `quillotana.cl` (también subdominios de prueba). Por tanto:

| Rol | Host propuesto | Notas |
|-----|----------------|-------|
| PWA staging | `pwa-lab.lq-ops.dev` (o similar **fuera** de quillotana.cl) | HTTPS (Let's Encrypt / Cloudflare Tunnel) |
| API staging | `api-lab.lq-ops.dev` | PG de test dedicado; `OFFLINE_SYNC_WRITES_ENABLED=1` solo aquí |
| R2 staging | bucket `lq-ops-lab-evidence` | Prefijo `operaciones/entregas/…`; nunca bucket prod |
| Prod | `*.quillotana.cl` / `api.quillotana.cl` | Writes OFF; bloqueo host PWA intacto |

**No** usar `staging.quillotana.cl` ni debilitar el bloqueo de producción.

Acceso iPhone: agregar PWA staging a pantalla de inicio vía Safari → Compartir → “Añadir a inicio”.

## 2. Kill switch (servidor)

```
OFFLINE_SYNC_WRITES_ENABLED=false          # default / emergencia
# activación:
# OFFLINE_SYNC_WRITES_ENABLED=1
```

- Writes OFF ⇒ 503 para sync.
- Con writes ON: todos los choferes autenticados en despachos asignados.
- Sin `OFFLINE_SYNC_PILOT_*` allowlists.
- PWA: `localStorage` / `NEXT_PUBLIC_*` **no** anulan el servidor.

## 3. Procedimiento iPhone (manual, evidencia física pendiente)

1. Abrir `https://pwa-lab…` en Safari; iniciar sesión usuario piloto staging.
2. Descargar despacho de prueba (datos ficticios).
3. Añadir a inicio (PWA).
4. Modo avión.
5. Registrar 1 entrega completa + 1 local cerrado (foto real de prueba + GPS).
6. Ver panel: pendientes > 0; texto “Guardado en este dispositivo · Pendiente…”.
7. Cerrar app desde selector de apps (swipe up).
8. Reabrir PWA; confirmar pendientes + foto.
9. Desactivar modo avión; esperar sync / “Sync ahora”.
10. Confirmar “Entrega confirmada por el servidor”; progreso oficial actualizado.
11. WhatsApp manual **solo** tras ACK.
12. Verificar en API/PG staging: 1 visita por op, sin dupes; closed_store → `pending_review`.

### Qué NO se puede afirmar en el entorno actual

- Prueba iOS/Safari física **no ejecutada** en 2A.5.
- No hay aprobación iOS hasta evidencia en dispositivo real contra staging.
- Playwright Chromium ≠ WebKit iOS (IDB/quota/SW diferencias posibles).

## 4. Confirmación de estados (contrato)

| Afirmación | Garantía |
|------------|----------|
| Guardado local ≠ sincronizado | `status=pending`, `server_confirmed=false` hasta ACK |
| Sincronizado ≠ GPS verificado | ACK + `closure_review_status=pending_review` en diferido |
| pending_review tras ACK | GPS diferido nunca auto-`verified` |
| Progreso oficial | Solo tras visita confirmada backend |
| 409 | `status=conflict`, outbox conservada |
| Logout / 401 / SW / BUILD_ID | Outbox retenida; DB distinta de snapshots V1 |
| Otro usuario | Visibilidad 0; no puede sync ops ajenas |

## 5. Migración 061

Archivo: `backend/sql/distribuidora/061_delivery_offline_ops_AWAITING_APPROVAL.sql`  
Estado: **AWAITING_APPROVAL**. No ejecutar en prod.  
Verificación RO: `061_read_only_status_check.sql`, `061_r2_orphan_dry_run.sql`.

## 6. Criterio de éxito 2A.5 (lab)

Navegador real (Chromium Playwright) + IndexedDB persistente: 5 ops, foto sobrevive reopen, sync una sola vez a PG lab.
