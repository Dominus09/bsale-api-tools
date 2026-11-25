# Inteligencia de Bodega — Slice 1.4 (Publicación formal y preparación Fase 1)

**Fecha:** 2026-11-24  
**Alcance:** publicación draft→published→archived, validación pre-publish, UI historial/restauración/impresión, E2E `main.py`+JWT real, revisión SQL/Git/Coolify  
**No:** commit/push, SQL productivo, redeploy Coolify, inventario, PWA, Choferes/Vendedores, stock Bsale

**Estado:** slice 1.4 **completado** en código y validación aislada.  
**DETENER.** Esperar autorización expresa para puesta en marcha (commit / push / migraciones / deploy).

---

## 1. Resumen de implementación

Se cerró el Editor Maestro para uso operativo por gerencia:

- Flujo formal **publicar / archivar / nuevo borrador desde histórico**.
- Validación previa con bloqueo ante `integrity_error` / `geom_conflict`.
- UI: Validar, Publicar (confirmación), Historial, Imprimir.
- Semántica de retiro: quitar del draft **no** invalida el layout published vigente.
- Seed BC permanece en **draft** (no auto-publicable por conflictos geométricos provisionales).
- Preparación de commit, VERIFY SQL y checklist Coolify **sin ejecutar** despliegue.

---

## 2. Funcionalidades operativas

| Capacidad | Estado |
|-----------|--------|
| Abrir Inteligencia de Bodega (gerencia) | Sí |
| Ver BC / PISO1 seed | Sí (draft) |
| Editar / generar / simbología / guardar | Sí (solo draft) |
| Validar plano | Sí |
| Publicar plano (atómico + row_version) | Sí |
| Historial + abrir solo lectura | Sí |
| Nuevo borrador desde published/archived | Sí (identidades reutilizadas) |
| Imprimir layout (browser print) | Sí |
| Inventario / FIFO / picking / PWA | Fuera de alcance |

---

## 3. Flujo de publicación

1. Usuario guarda borrador (`expected_row_version`).
2. **Evaluar** (`POST …/evaluate-publish`): blockers vs warnings vs pending.
3. Si `can_publish`: **Publicar** atómico:
   - Archiva published previa del piso (si existe).
   - Draft → `published` + `published_at/by` + `publish_meta_json` (snapshot facility/floor + conteos).
   - Auditoría `revision_published` / `revision_archived`.
4. Published/archived: **no editables**.
5. **Restaurar**: `POST …/create-draft` copia snaps/geometría a nuevo draft; no modifica histórico.  
   *Restaurar layout ≠ rollback de migración SQL.*

### Retiro de ubicaciones
Si el physical aparece en published vigente → solo se elimina del draft (`geometry_removed_from_draft_published_preserved`). No se marca `retired` prematuramente.

---

## 4. Endpoints nuevos / modificados

| Método | Ruta | Rol |
|--------|------|-----|
| POST | `/warehouse/revisions/{id}/evaluate-publish` | Pre-chequeo (no escribe) |
| POST | `/warehouse/revisions/{id}/publish` | Publicación atómica |
| POST | `/warehouse/revisions/{id}/create-draft` | Nuevo draft desde histórico |
| GET | `/warehouse/floors/{id}/revisions` | Incluye meta publish/archive |
| (existentes) | batch / validate / preview / audit | Intactos; retiro preserva published |

Auth: `require_management_access` → `require_erp_app` (sin ampliar capabilities).

---

## 5. Pruebas realizadas

| Suite | Resultado |
|-------|-----------|
| `_run_warehouse_isolated.py` (layout + generator + **publish**) | **33 passed** |
| Publicación / inmutabilidad / concurrencia / overlap / retire / draft-from | PASS |
| Seed BC no auto-publicable | PASS |
| ESLint warehouse | **0** |
| `tsc` warehouse / inteligencia-bodega | **0** |
| `tsc` global | **53** (= baseline 1.2/1.3) |
| `next build` | **OK** (`/inteligencia-bodega`) |
| E2E `main.py` + JWT real | **ok: true** → `_audit_artifacts/inteligencia_bodega_1_4_e2e_main_auth.json` |

### TypeScript 52 vs 53
- `origin/main`: **52** errores.
- Working tree (1.1–1.4): **53**.
- Diferencia **única**: `app/(dashboard)/distribuidora/seguimiento-entregas/page.tsx` (`customer_key`) — **ajena** a warehouse; **0** errores en paths warehouse/inteligencia-bodega.
- No se refactorizó código ajeno ni se ocultó con tsconfig.

---

## 6. Resultado E2E autenticación real (`backend/main.py`)

Entorno: pgserver aislado + `PG_*` apuntando al temporal + `DISTRIBUIDORA_SCHEMA_CHECK=warn` (arranque exige schema 048; en lab warehouse-only se usa warn — **documentado**).

Auth: `create_staff_token(role=gerencia)` → `require_management_access` / `require_erp_app` (**sin stub** de dependency).

| Paso | Resultado |
|------|-----------|
| Rol no gerencia | **403** |
| Anónimo | **401** |
| Listar facilities BC | **200** |
| Evaluate seed | `can_publish: false` |
| Publish seed | **400** |
| Get revision / audit | **200** |
| Batch row_version malo | **409** |

Bloqueo conocido: TestClient de `main.py` en PG sin schema distribuidora 048 falla en startup si `DISTRIBUIDORA_SCHEMA_CHECK=strict` (default prod). Lab usó `warn` solo para poder ejercitar rutas warehouse con JWT real.

Publicación válida de layout limpio: cubierta en suite isolated `test_publish_atomic_and_history` (no en seed BC).

---

## 7. Capturas ERP reales

| Archivo | Contenido |
|---------|-----------|
| `inteligencia_bodega_1_4_editor.png` | Editor con Validar / Publicar / Historial / Imprimir |
| `inteligencia_bodega_1_4_validacion_publicar.png` | Diálogo publicar: bloqueado por conflictos + pending |
| `inteligencia_bodega_1_4_historial.png` | Historial de versiones (draft seed) |
| `inteligencia_bodega_1_4_imprimible.png` | Hoja imprimible Grupo La Quillotana + advertencia provisional |

---

## 8. SQL definitivos (pendientes de autorización)

Orden:

1. `backend/sql/warehouse/001_schema_layout_AWAITING_APPROVAL.sql`
2. `backend/sql/warehouse/002_seed_bodega_central_draft_AWAITING_APPROVAL.sql`
3. `backend/sql/warehouse/003_seed_bc_special_zones_AWAITING_APPROVAL.sql`
4. `backend/sql/warehouse/004_publish_layout_AWAITING_APPROVAL.sql` *(índice 1 published/piso + columnas publish)*
5. Verificar con `backend/sql/warehouse/005_VERIFY_SELECT_ONLY.sql`

Idempotencia: `IF NOT EXISTS` / inserts condicionados. No tocan schemas ajenos. Seed BC **no** se publica automáticamente.

---

## 9. Lista exacta de archivos para commit futuro

**Incluir:**
```
backend/main.py                                          # include router warehouse
backend/routers/warehouse.py
backend/services/warehouse/**                            # service, repo, geometry, generator, validation, coords, errors
backend/sql/warehouse/001_*.sql
backend/sql/warehouse/002_*.sql
backend/sql/warehouse/003_*.sql
backend/sql/warehouse/004_*.sql
backend/sql/warehouse/005_VERIFY_SELECT_ONLY.sql
backend/tests/test_warehouse_*.py
backend/tests/_run_warehouse_isolated.py
backend/tests/_run_warehouse_e2e_lab.py
backend/tests/_run_warehouse_e2e_main_auth.py
backend/tests/_warehouse_e2e_lab_app.py
frontend/app/(dashboard)/inteligencia-bodega/**
frontend/components/warehouse/**
frontend/lib/warehouse/**
frontend/components/layout/sidebar.tsx                   # menú Inteligencia de Bodega
frontend/package.json
frontend/package-lock.json                               # konva / react-konva
_audit_artifacts/inteligencia_bodega_slice_1_*.md        # opcional docs
_audit_artifacts/inteligencia_bodega_1_*.png             # opcional evidencia
_audit_artifacts/inteligencia_bodega_fase1_auditoria_tecnica.md  # opcional
```

**Excluir:**
- `__pycache__/`, `*.pyc`, `.next/`
- `_tmp_*`, JWT/DSN en `_tmp_warehouse_e2e_lab.json`
- Tokens en logs
- Cambios ajenos (pycache routers, PWA offline-build, seguimiento no warehouse, etc.)
- Credenciales / `.env`

### Mensaje de commit propuesto (no ejecutar)
```
feat(warehouse): Editor Maestro Fase 1 — layout versionado, generador y publicación formal

Schema warehouse aislado, editor Konva ERP, generador de posiciones, validación
geométrica y flujo draft/published/archived con auditoría. Seed BC permanece en
borrador; sin inventario ni deploy automático.
```

---

## 10. Estado Git

- Rama: `main`
- Cambios warehouse 1.1–1.4: **locales, sin commit/push**
- Worktrees ajenos: no modificados intencionalmente

---

## 11. Checklist de despliegue Coolify (solo preparación)

Auditoría **solo lectura** desde docs del repo (`docs/auth/RUNBOOK_PUBLISH_MULTIAPP_1C.md`, PWA README, `.env.example`):

| Hallazgo | Detalle |
|----------|---------|
| Hosts documentados | FE `test.quillotana.cl` · API `api.quillotana.cl` · BD `analytics` (nombres de servicio Coolify **no asumidos**) |
| Autodeploy | No activar / no modificar en esta fase; redeploy **manual** tras autorización |
| Rollback imagen | No revierte SQL |
| Dependencia FE→API | No desplegar FE warehouse sin tablas/endpoints API |

### Secuencia propuesta (ajustar a nombres reales en Coolify UI)

1. Respaldo BD + anotar SHA API/FE actuales.
2. Verificar DSN destino (**no** prod por error) + `005_VERIFY` previo vacío.
3. Apply 001→004 en destino **autorizado** (`ON_ERROR_STOP`).
4. `005_VERIFY_SELECT_ONLY` — confirmar índices draft/published, seed BC draft.
5. Commit + push **autorizados** (considerar autodeploy real).
6. Deploy **backend** → health + `GET /warehouse/facilities` con usuario gerencia.
7. Deploy **frontend ERP**.
8. Smoke: login → menú Inteligencia de Bodega → BC/PISO1 → guardar draft → validar → (no forzar publish seed).
9. Verificar módulos existentes (distribuidora/auth) sin regresión.

**Primera versión operativa:** editor usable; seed en borrador; sin inventario ficticio ni publish automático.

---

## 12. Procedimiento de rollback

| Capa | Acción |
|------|--------|
| Frontend | Redeploy SHA anterior en Coolify |
| Backend | Redeploy SHA anterior |
| SQL warehouse | **No** drop automático. Si hace falta: dejar schema (aditivo) o `DROP SCHEMA warehouse CASCADE` solo con backup y autorización (destructivo) |
| Datos layout | Revisiones archived/published se conservan; no hay mecanismo que borre historial |

---

## 13. Riesgos y bloqueos

| Ítem | Nota |
|------|------|
| Seed BC no publicable | Esperado (conflictos BAE↔ESC, overlaps provisionales) |
| Geometrías IZ-SU/BAE | Provisionales / pending_validation |
| `DISTRIBUIDORA_SCHEMA_CHECK` | Lab E2E main requiere `warn` si falta 048 |
| Coolify nombres exactos | Confirmar en UI antes del go-live |
| Error tsc +1 seguimiento | Ajeno a warehouse; no corregido en este slice |

---

## 14. Fin slice 1.4

**DETENERSE.** No SQL productivo, no commit, no push, no redeploy.

Esperar autorización expresa para la puesta en marcha definitiva de la Fase 1.
