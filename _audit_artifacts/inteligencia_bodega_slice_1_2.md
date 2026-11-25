# Inteligencia de Bodega — Slice 1.2 (Editor Maestro ERP)

**Fecha:** 2026-11-24  
**Alcance:** aislamiento draft/published (identity snaps) + editor ERP `/inteligencia-bodega` + simbología + cliente API + pruebas  
**No:** commit/push, SQL productivo, PWA, Choferes/Vendedores, stock Bsale, publicación formal, generador 1.3, 3D, FIFO

---

## 1. Resumen de implementación

Se entregó el **Editor Maestro de Bodegas** en la ERP:

- Entrada de menú independiente **Inteligencia de Bodega** → `/inteligencia-bodega` (solo gerencia).
- Canvas Konva (`konva@10.11.0` + `react-konva@19.2.7`) con datos desde `/warehouse/*`.
- Toolbar, propiedades, simbología (3 pestañas), guardado batch con `expected_row_version`, 409, dirty state.
- Ajuste mínimo al modelo 1.1: **snapshots de identidad** en `layout_elements` para que rename/retire en draft no altere published.
- Suite warehouse: **17 passed** en PG aislado (15 previas + 2 de aislamiento).

---

## 2. Compatibilidad y versiones

| Paquete | Versión | Notas |
|---------|---------|--------|
| Next.js | 16.1.6 | Sin actualizar |
| React / React DOM | 19.2.4 | Sin actualizar |
| konva | **10.11.0** (exact) | Canvas 2D |
| react-konva | **19.2.7** (exact) | Compatible con React 19.2.x |

- `react-konva@19.3.x` exige React `^19.3` → **no** usado.
- Instalación **sin** `--force` / `--legacy-peer-deps`.
- Canvas cargado con `dynamic(..., { ssr: false })` (App Router + client components).

---

## 3. Archivos creados y modificados

### Creados
- `frontend/app/(dashboard)/inteligencia-bodega/page.tsx`
- `frontend/components/warehouse/WarehouseEditor.tsx`
- `frontend/components/warehouse/WarehouseCanvas.tsx`
- `frontend/components/warehouse/PropertiesPanel.tsx`
- `frontend/components/warehouse/SymbologyDialog.tsx`
- `frontend/lib/warehouse/{types,api,coords,style,validation}.ts`
- `frontend/lib/warehouse/coords.test.mjs`
- `_audit_artifacts/inteligencia_bodega_1_2_*.png` (4 capturas)
- `_audit_artifacts/inteligencia_bodega_slice_1_2.md` (este informe)
- `_tmp_warehouse_editor_preview.html` (preview visual seed para capturas; no es SoT)

### Modificados (respecto de slice 1.1 / baseline)
- `backend/sql/warehouse/001_schema_layout_AWAITING_APPROVAL.sql` — columnas `*_snap`
- `backend/sql/warehouse/002_seed_bodega_central_draft_AWAITING_APPROVAL.sql` — seed con snaps
- `backend/services/warehouse/{repository,service,coordinates}.py`
- `backend/tests/test_warehouse_layout_{isolated_pg,unit}.py`
- `frontend/components/layout/sidebar.tsx` — sección menú + gate gerencia
- `frontend/package.json` / `package-lock.json` — konva + react-konva

### Intactos (restricciones)
- PWA, Choferes, Vendedores, stock/sync Bsale: **sin cambios funcionales**.

---

## 4. Ajustes al modelo del slice 1.1

Problema: `GET /warehouse/revisions/{id}` hacía JOIN a `physical_elements` live → rename/retire en draft **habría** alterado la lectura de una revisión published.

**Cambio mínimo** (migraciones aún no en prod → se editó `001`/`002` en lugar de un `003`):

En `warehouse.layout_elements`:

| Columna | Rol |
|---------|-----|
| `code_snap` | Código visible congelado en la revisión |
| `name_snap` | Nombre congelado |
| `element_type_snap` | Tipo congelado |
| `parent_physical_id_snap` | Padre congelado |
| `physical_meta_snap` | meta de identidad congelada |

- Upsert de draft escribe geometría **y** snaps.
- Lectura de cualquier revisión usa snaps para identidad visible (`identity_source: revision_snapshot`).
- `physical_elements.id` permanece estable; `status` live solo informativo.
- Publicación formal (copiar draft→published) sigue siendo slice futuro; los tests simulan el copy de filas.

---

## 5. Funcionalidades realmente operativas

| Función | Estado |
|---------|--------|
| Nav ERP + gate gerencia UI | OK |
| API gate `require_management_access` | OK (1.1 + regresiones) |
| Carga facilities/floors/draft/revision | OK (cliente listo; requiere schema aplicado en el API local) |
| Canvas seed BC (orientación entrada abajo) | OK (transform Y invertida solo en pantalla) |
| Selección / mover / rotar / resize | OK (Konva Transformer) |
| Crear posición 1,26×1,06 + códigos tipo P2-nn / IZ-nn-D | OK |
| Panel propiedades ↔ canvas | OK |
| Simbología 3 pestañas desde catálogo API | OK |
| Guardar batch + row_version + 409 | OK (cliente + backend) |
| Undo/redo sesión local | OK (no persistente) |
| beforeunload si dirty | OK |
| Publicación formal | No (fuera de alcance) |
| Generador filas 4+4 / 4+5 | No → 1.3 |

---

## 6. Capturas del editor

Preview visual con geometría del seed 002 y la misma paleta/transformación del editor (Konva).  
**Nota:** capturas vía HTTP local del preview HTML; el shell ERP real requiere backend con schema `warehouse` + login gerencia.

1. Vista general BC — `_audit_artifacts/inteligencia_bodega_1_2_vista_general.png`
2. Pasillo P2 + propiedades — `_audit_artifacts/inteligencia_bodega_1_2_pasillo_props.png`
3. Posición P2-01 (1,26×1,06) — `_audit_artifacts/inteligencia_bodega_1_2_posicion.png`
4. Ventana Simbología — `_audit_artifacts/inteligencia_bodega_1_2_simbologia.png`

Orientación verificada en capturas: **PORTON abajo**, IZ/CIRC/DER hacia el fondo, P1 cerca OFC, P7 junto ESC.

---

## 7. Resultado de las pruebas

| Prueba | Resultado |
|--------|-----------|
| Suite 1.1 + aislamiento (`python -m backend.tests._run_warehouse_isolated`) | **17 passed** (PG temporal) |
| `test_draft_rename_does_not_alter_published_identity_snapshot` | PASSED |
| `test_retire_from_draft_keeps_published_geometry` | PASSED |
| `node frontend/lib/warehouse/coords.test.mjs` | OK (round-trip + Y invertida + rotación −CCW) |
| ESLint archivos warehouse + sidebar | EXIT 0 |
| `tsc --noEmit` | Fallos **preexistentes** en etiquetas/mapa/etc.; **0 errores** en paths warehouse/inteligencia-bodega |
| `npm run build` (Next 16) | **BUILD_EXIT:0** (`_tmp_erp_build_wh12d.log`) — incluye ruta inteligencia-bodega |
| Acceso UI no autorizado | Página muestra denegación; menú oculto sin management |
| Acceso API 403 | Cubierto por tests router |
| Carga/guardado E2E contra API ERP local con auth real | **No ejecutado** — schema `warehouse` no aplicado al DSN del API de desarrollo ERP; no se usó prod |
| Regresión módulos ajenos | Build completo OK; no se tocaron PWA/Choferes/Vendedores/stock |

---

## 8. Estado Git y diff resumido

- **HEAD:** `517ceb5` (`fix(catalog): hardening V2…`) — **sin commit** de este slice.
- Working tree: archivos warehouse 1.1 + 1.2 + frontend editor + konva deps + capturas (untracked/modified).
- Sin push / redeploy / worktrees ajenos.

---

## 9. Riesgos y bloqueos

- SQL `AWAITING_APPROVAL` **no** aplicado en producción ni necesariamente en el PG del API local ERP.
- Preview HTML sirve para orientación/simbología visual; persistencia real exige API con schema.
- Deshacer/rehacer solo en sesión; tras guardar se limpia el historial local.
- Validación de solapes es advertencia básica (AABB), no certificación de seguridad.
- Shell de tests aislados a veces reporta exit≠0 del wrapper pese a `pytest_exit=0` (cleanup pgserver); los 17 asserts pasaron.

---

## 10. Instrucciones verificadas para probar localmente

1. Aplicar en **PG aislado/local** (nunca prod):
   - `backend/sql/warehouse/001_schema_layout_AWAITING_APPROVAL.sql`
   - `backend/sql/warehouse/002_seed_bodega_central_draft_AWAITING_APPROVAL.sql`
2. Apuntar el backend local a ese DSN; reiniciar API.
3. `cd frontend && npm run dev`
4. Login ERP con rol gerencia/adm/admin u owner.
5. Menú **Inteligencia de Bodega** → Editor.
6. Verificar BC / PISO1, elementos seed, Simbología, crear posición, Guardar borrador, recargar.
7. Suite: `python -m backend.tests._run_warehouse_isolated`
8. Coords: `node frontend/lib/warehouse/coords.test.mjs`

---

## 11. Propuesta slice 1.3

1. Generador avanzado de filas/posiciones (4+4, 4+5, 5+5, IZ D/A).
2. Validaciones geométricas avanzadas (contención sector, pasillos, clearances).
3. Flujo de **publicación** formal (draft→published con copy de snaps + geometría).
4. Endpoints/capabilities granulares `warehouse_layout_view|edit|publish` si se desea salir del gate gerencia amplio.
5. (Opcional) Vista ocupación stub sin inventar inventario.

---

**STOP.** Esperar autorización expresa antes de slice 1.3, commit/push, migración productiva o deploy.
