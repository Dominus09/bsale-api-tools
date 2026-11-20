# Offline V2 — Staging + checklist iPhone (NO desplegado)

Estado: **preparación**. Esperar aprobación explícita antes de desplegar staging.

## 0. Control de dominio (obligatorio antes de usar un host)

Candidatos (fuera del bloqueo `*.quillotana.cl`):

| Uso | Candidato | Estado |
|-----|-----------|--------|
| PWA staging | `pwa-lab.lq-ops.dev` | **NXDOMAIN** (no usar; no controlado) |
| API staging | `api-lab.lq-ops.dev` | **NXDOMAIN** (no usar; no controlado) |
| Alternativa | Túnel HTTPS (`cloudflared` / ngrok) → lab local | **Requerido** hasta dominio propio confirmado |

**No proponer ni usar un dominio que no controlemos.**  
**No usar** `*.quillotana.cl` (el middleware y la política PWA lo bloquean a propósito).

## 1. Aislamiento de componentes

| Componente | Staging | Producción |
|------------|---------|------------|
| PWA | Build con `OFFLINE_V2_LAB_ENABLED` solo si se necesita harness; writes vía servidor | Writes OFF; lab route 404 |
| FastAPI | Instancia dedicada; JWT secret distinto | `api.quillotana.cl` |
| PostgreSQL | Cluster/DB de pruebas; aplicar 061 solo aquí tras aprobación staging | analytics/prod intacto |
| R2 | Bucket `lq-ops-lab-evidence` | Bucket productivo intacto |
| Credenciales | Usuarios lab; passwords rotables | No reutilizar |

Flags:

```
OFFLINE_SYNC_WRITES_ENABLED=false   # default global
# Solo en API staging, tras autorización:
# OFFLINE_SYNC_WRITES_ENABLED=1
# OFFLINE_SYNC_PILOT_ALLOWLIST=<user_ids>
```

## 2. Checklist Safari iOS / PWA instalada

Datos: **solo despachos ficticios de staging**.

1. [ ] Abrir PWA staging en Safari (HTTPS).
2. [ ] Login usuario lab allowlisted.
3. [ ] Descargar despacho ficticio completo (Mis despachos → Clientes).
4. [ ] Añadir a pantalla de inicio.
5. [ ] Modo avión.
6. [ ] Ficha → Entrega completa offline (×4 clientes) o mix + 1 Local cerrado.
7. [ ] Local cerrado: foto real de prueba + GPS.
8. [ ] Ver badge/panel de pendientes = N.
9. [ ] Cerrar PWA desde app switcher.
10. [ ] Reabrir; confirmar pendientes + foto.
11. [ ] Salir de modo avión; esperar sync automático / manual.
12. [ ] Textos: “Entrega confirmada por el servidor”.
13. [ ] WhatsApp manual solo tras ACK.
14. [ ] Verificar en PG staging: N visitas, 0 dupes, closed → `pending_review`.

### No afirmar aún

- Compatibilidad iOS física **no aprobada** hasta completar este checklist con evidencia.
- Playwright Chromium ≠ WebKit iOS.

## 3. Revisión SQL 061 (resumen)

Archivo: `backend/sql/distribuidora/061_delivery_offline_ops_AWAITING_APPROVAL.sql`  
Estado: **AWAITING_APPROVAL — no ejecutar en prod.**

| Tema | Hallazgo |
|------|----------|
| Compatibilidad | Aditiva; FK a `dispatch_plan` y `delivery_visits`. Requiere 056+ visitas. **No depende de 059/060** |
| Restricciones | UNIQUE(driver, idem); CHECKs op_type/status/stage/gps |
| Idempotencia | request_hash + response_json replay; advisory locks idem + plan/cliente |
| Foto | evidence_sha256 + object_key estable; upload fuera de TX larga |
| completed | status + visit_id + response_json + completed_at |
| updated_at | `mark_stage` / updates usan `NOW()` |
| Interrupción | etapas awaiting_evidence / processing recuperables |
| Reversión | `DROP TABLE delivery_offline_ops` (no borra visitas ya creadas). Backup: dump tabla antes |

RO checks: `061_read_only_status_check.sql`, `059_060_read_only_status_check.sql`, `061_r2_orphan_dry_run.sql`.

## 4. Protección lab route

`apps/pwa/middleware.ts`: `/pruebas/offline-v2-lab` → **404** salvo `OFFLINE_V2_LAB_ENABLED=1` y host no Quillotana.
