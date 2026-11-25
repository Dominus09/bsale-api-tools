# Inteligencia de Bodega — Slice 1.3 (Generador, validación geométrica, zonas especiales, E2E real)

**Fecha:** 2026-11-24  
**Alcance:** generador avanzado de posiciones + validación geométrica (rotación) + seed IZ-SU/BAE + simbología + prueba integral ERP Next.js + FastAPI + PostgreSQL aislado  
**No:** commit/push, SQL productivo, publicación formal, PWA, Choferes/Vendedores, stock Bsale, inventarios ficticios, 3D, FIFO/FEFO, picking, RFID

**Estado:** slice 1.3 **completado** en código y validación aislada.  
**Detenerse aquí** — esperar autorización expresa para slice 1.4 (publicación formal / commit / push / migraciones productivas / despliegue).

---

## 1. Resumen de implementación

Se entregó el generador visual **Generar posiciones** (preview sin escritura BD → incorporar al borrador → persistir solo vía Guardar), validación geométrica avanzada centralizada en backend (SAT/rotación, severidades), tipos de almacenamiento (`loose_storage` / `storage_kind`), seed aditivo IZ-SU + BAE-01/02 provisionales, simbología actualizada, y **prueba E2E real** contra Next.js + FastAPI mínimo + pgserver aislado (no HTML preview).

Preservado íntegramente el trabajo de slices 1.1 y 1.2: schema `warehouse`, identidades físicas, geometría versionada, identity snaps, batch atómico, concurrencia optimista, auditoría, permisos ERP management, Next 16 / React 19 / Konva.

---

## 2. Archivos creados y modificados

### Creados (slice 1.3)
| Ruta | Rol |
|------|-----|
| `backend/services/warehouse/geometry.py` | Geometría con rotación (overlap, contención, área) |
| `backend/services/warehouse/generator.py` | Presets, referencias BC, `generate_positions`, `verify_reference_calcs` |
| `backend/services/warehouse/validation.py` | Validación avanzada + severidades |
| `backend/sql/warehouse/003_seed_bc_special_zones_AWAITING_APPROVAL.sql` | IZ-SU, BAE-01/02 aditivos provisionales |
| `backend/tests/test_warehouse_generator_unit.py` | Unit: presets, orientaciones, overlap, jerarquía |
| `backend/tests/_warehouse_e2e_lab_app.py` | App FastAPI mínima lab (warehouse + `/auth/me` stub) |
| `backend/tests/_run_warehouse_e2e_lab.py` | Orquestador E2E API aislado |
| `frontend/components/warehouse/GeneratePositionsDialog.tsx` | UI generador + preview |
| `_audit_artifacts/inteligencia_bodega_1_3_*.png` | Capturas ERP reales |
| `_audit_artifacts/inteligencia_bodega_1_3_e2e_api.json` | Resultado API E2E |
| `_audit_artifacts/inteligencia_bodega_slice_1_3.md` | Este informe |

### Modificados (respecto de 1.1/1.2)
| Ruta | Cambio |
|------|--------|
| `backend/sql/warehouse/001_schema_layout_AWAITING_APPROVAL.sql` | `loose_storage` en tipos |
| `backend/services/warehouse/coordinates.py` | `STORAGE_KINDS`, simbología SU/BAE/especial/extends |
| `backend/services/warehouse/service.py` | `preview_generate_positions`, `validate_revision_elements` |
| `backend/routers/warehouse.py` | `POST …/generate-positions/preview`, `POST …/validate` |
| `backend/tests/_run_warehouse_isolated.py` | Incluye generator unit; cleanup sin exit ambiguo |
| `backend/tests/test_warehouse_layout_unit.py` | Rutas generador / tipos |
| `frontend/lib/warehouse/{types,api,style}.ts` | Tipos API, estilos loose/conflict/pending |
| `frontend/components/warehouse/WarehouseEditor.tsx` | Generar posiciones, loose_storage, validate post-save |
| `frontend/components/warehouse/WarehouseCanvas.tsx` | Estilos conflicto / pending / hatched loose |
| `frontend/components/warehouse/SymbologyDialog.tsx` | Nomenclatura SU / IZ-SU / BAE |
| `frontend/components/warehouse/PropertiesPanel.tsx` | Tipo almacenamiento sin pallet |

### Intactos (restricciones)
- PWA, Choferes, Vendedores, stock/sync Bsale: **sin cambios funcionales**.
- Autenticación general: **sin ampliar capabilities**; lab usa stub / override management solo en entorno aislado.
- No commit / push / redeploy / SQL productivo.

---

## 3. Funcionalidades reales

### 3.1 Generador avanzado
- Herramienta **Generar posiciones** (toolbar + botón header) sobre sector/pasillo/bloque seleccionado.
- Config: dimensiones pallet (defecto 1,26×1,06), orientación H/V, filas, posiciones/fila, orientación independiente por fila, separaciones, márgenes, anclaje, dirección de crecimiento, prefijo/numeración, altura libre, restricciones, extensión fuera del límite nominal.
- Presets: `4+4`, `4+5`, `5+5`, `5+6`, `6+6`, `Personalizado`.
- Preview (API): cantidad base, superficie ocupada, separación interior, extensión fuera del sector, conflictos con obstáculos, advertencias de circulación — **sin escritura BD**.
- Al confirmar: incorpora al borrador local; cada posición editable (mover/girar/redimensionar/duplicar/retirar).
- **No regeneración destructiva**: no elimina ni sustituye códigos ya existentes; nuevos códigos evitan colisión.

### 3.2 Sector izquierdo
- Modo numeración `IZ-##-D` / `IZ-##-A` (delante/atrás) configurable.
- No impone cuadrícula uniforme inmodificable; permite irregularidades y ajustes individuales.

### 3.3 Tipos de almacenamiento
Sin tablas nuevas. Reutiliza `element_type` + `constraints_json` / meta:

| Clasificación | Representación |
|---------------|----------------|
| Estándar sobre pallet | `position` + `storage_kind: pallet_standard` |
| Sin pallet / sueltos | `loose_storage` + `storage_kind: loose_no_pallet` |
| Especial restringido | `position` + `storage_kind: special_restricted` (+ flags) |
| Circulación / no usable / estructuras | tipos existentes (`circulation`, `unusable`, `wall`, `stair`, …) |

Una zona sueltos **no** crea pallet ficticio.

### 3.4 Validación geométrica avanzada
Backend (`validation.py` + `geometry.py`): overlap con rotación, colisiones con muros/obstáculos, fuera de perímetro/sector, invasión de circulación, interferencia con escaleras/accesos, dimensiones inválidas, códigos duplicados, jerarquía incorrecta.

Severidades: `integrity_error` | `geom_conflict` | `warning` | `pending_validation`.

Solapamiento jerárquico válido (posición dentro de pasillo) ≠ conflicto físico.  
Guardar representación ≠ autorizar condición insegura.

Endpoints:
- `POST /warehouse/meta/generate-positions/preview`
- `POST /warehouse/revisions/{id}/validate`

### 3.5 Simbología
Catálogo + diálogo (Nomenclatura / Símbolos / Colores): SU, IZ-SU, BAE, BAE-01/02, almacenamiento especial, fuera de límite, conflictos geométricos, pending validation. Tooltips y leyenda alineados al canvas.

---

## 4. Cambios en el seed

### `003_seed_bc_special_zones_AWAITING_APPROVAL.sql` (aditivo)
| Código | Tipo | Notas |
|--------|------|-------|
| **IZ-SU** | `loose_storage` | ≈2,52×1,06 m (≈2,67 m²); extremo oeste IZ; forma exacta **provisional** |
| **BAE-01** | `position` especial | Candidato bajo ESC; `safe_storage_authorized: false`; pending |
| **BAE-02** | `position` especial | Igual; no habilitado como almacenamiento seguro |

- Solo inserta si el código activo no existe; no sobrescribe layout ya presente en el draft.
- ESC se mantiene; acceso a escalera debe permanecer despejado (validación reporta interferencia BAE↔ESC como conflicto geométrico / pendiente).
- Coordenadas BAE son **aproximadas**; el editor permite ajustarlas.

### Referencias geométricas BC (sin márgenes/obstáculos — no son capacidades seguras)
| Caso | Resultado comprobado en tests |
|------|-------------------------------|
| Transversal 1,06 / long 1,26 → 2 filas en 2,80 | Separación nominal **0,68 m** |
| 4+5 anclado al fondo | Extensión **0,30 m** |
| Girados transversal 1,26 / long 1,06 | Separación nominal **0,28 m** |
| 5+5 girado | Cabe en 6 m |
| 6+6 girado | Extensión **0,36 m** |

Los 60 cm observados **no** autorizan invadir circulación. Apilamiento **no** autorizado automáticamente.

---

## 5. Prueba E2E integral (ERP real + API + PG aislado)

### Entorno
| Componente | Detalle |
|------------|---------|
| PostgreSQL | pgserver temporal (`127.0.0.1`, data_directory en `%TEMP%`) |
| Schema | `warehouse` + seeds 001/002/003 |
| API | FastAPI lab (`_warehouse_e2e_lab_app.py`) — **no** prod; sin sync/cron |
| Frontend | Next.js real `http://127.0.0.1:3013` → `NEXT_PUBLIC_API_URL` lab |
| Auth | JWT gerencia de prueba + stub `/auth/me`; management gate intacto |
| DSN | Verificado **no** productivo (bloquea analytics/quillotana/prod/neon/…) |

### Flujo API automatizado (`inteligencia_bodega_1_3_e2e_api.json`)
| Paso | Resultado |
|------|-----------|
| Health | 200 |
| Acceso denegado sin auth | **403** |
| Cargar BC / PISO1 | OK (`rev` draft) |
| Preview 4+5 | 9 posiciones |
| Guardar borrador | OK → version 2 (lab API) |
| Reload persistencia | OK |
| Auditoría | OK |
| Validate | integrity 0 / geom_conflict 3 / pending 19 |
| Concurrencia | **409** |
| **ok** | **true** |

### Flujo UI browser (Next real)
1. Login lab → menú **Inteligencia de Bodega**.
2. BC + PISO1 cargados desde FastAPI.
3. Selección pasillo P2 → **Generar posiciones** → preview **4+5**.
4. Incorporar al borrador (P2-02… sin sustituir P2-01 existente).
5. Selección **IZ-SU** (tipo Almacenamiento sin pallet).
6. **Guardar borrador** → `draft · v3`.
7. Recarga página → persistencia P2-01…P2-09, IZ-SU, BAE-01/02; sin dirty.
8. Simbología (nomenclatura SU/BAE + símbolos loose_storage).

**No se usó** el HTML preview como sustituto de esta prueba.

---

## 6. Capturas reales (Next.js ERP)

Guardadas en `_audit_artifacts/`:

| Archivo | Contenido |
|---------|-----------|
| `inteligencia_bodega_1_3_vista_general.png` | Editor Maestro general |
| `inteligencia_bodega_1_3_pasillo_seleccionado.png` | Pasillo seleccionado |
| `inteligencia_bodega_1_3_generador.png` | Diálogo Generar posiciones |
| `inteligencia_bodega_1_3_preview_4x5.png` | Vista previa 4+5 |
| `inteligencia_bodega_1_3_posiciones_generadas.png` | Posiciones en canvas |
| `inteligencia_bodega_1_3_iz_su.png` | Zona IZ-SU (loose) |
| `inteligencia_bodega_1_3_simbologia.png` | Diálogo simbología (nomenclatura) |
| `inteligencia_bodega_1_3_simbolos.png` | Pestaña Símbolos (incl. loose_storage) |
| `inteligencia_bodega_1_3_persistido_reload.png` | Layout tras recargar (`draft · v3`, sin cambios) |

---

## 7. Resultados de pruebas automatizadas

| Suite / check | Resultado |
|---------------|-----------|
| `test_warehouse_layout_isolated_pg` + unit + generator vía `_run_warehouse_isolated.py` | **27 passed** |
| Generador / presets / asimétrico / orientaciones / IZ-D-A | PASS |
| Referencias 0,68 / 0,30 / 0,28 / 5+5 / 6+6 | PASS |
| Overlap rotado + jerarquía vs conflicto | PASS |
| Persistencia / identidades / concurrencia / auditoría / permisos | PASS (isolated + E2E) |
| ESLint paths warehouse / inteligencia-bodega | **0 errores** (`--max-warnings 0`) |
| `tsc --noEmit` paths warehouse / inteligencia-bodega | **0 errores** |
| `tsc --noEmit` global | **53** errores — **igual** baseline slice 1.2 (`_tmp_tsc_wh12.txt`); origin_main 52; **sin nuevos** en warehouse |
| `next build` | **OK** (incluye `/inteligencia-bodega`) |

Wrapper PG: `_run_warehouse_isolated.py` retorna el exit code de pytest y aísla fallos de cleanup de pgserver.

---

## 8. Estado de Git

- Rama: `main`
- **Sin commit / push** (restricción).
- Cambios warehouse 1.1–1.3: untracked bajo `backend/services/warehouse/`, `backend/sql/warehouse/`, `backend/routers/warehouse.py`, `frontend/components/warehouse/`, `frontend/lib/warehouse/`, `frontend/app/(dashboard)/inteligencia-bodega/`, tests; diffs menores en `backend/main.py`, `sidebar.tsx`, `package.json` (konva).
- Worktrees / cambios ajenos: **no tocados** de forma intencional (pycache y otros M preexistentes ajenos al slice).

---

## 9. SQL definitivo pendiente de autorización

Orden propuesto (solo tras autorización 1.4 / despliegue controlado):

1. `backend/sql/warehouse/001_schema_layout_AWAITING_APPROVAL.sql`
2. `backend/sql/warehouse/002_seed_bodega_central_draft_AWAITING_APPROVAL.sql`
3. `backend/sql/warehouse/003_seed_bc_special_zones_AWAITING_APPROVAL.sql`

**No ejecutar en producción** sin autorización expresa. Verificar DSN no productivo antes de cualquier apply.

---

## 10. Riesgos y bloqueos

| Ítem | Severidad | Nota |
|------|-----------|------|
| Geometrías IZ-SU / BAE provisionales | Media | Coordenadas/forma pendientes de medición en terreno |
| Conflictos geométricos seed (BAE↔ESC, pasillos vecinos) | Esperado | Representan estado físico a corregir; no son “OK seguridad” |
| Preview 4+5 en pasillo real puede solapar P2-01 existente | Esperado | Generador no borra; usuario debe ajustar |
| Lab E2E usa FastAPI mínimo (no `main.py` completo) | Bajo | Rutas warehouse + auth stub equivalentes; CORS local |
| Publicación formal / published revision | Bloqueado | Slice **1.4** |
| Token/JWT lab en `_tmp_warehouse_e2e_lab.json` | Info | Solo entorno temporal; no copiar a prod |

---

## 11. Propuesta slice 1.4 (publicación y despliegue controlado)

1. Revisión formal del informe 1.3 + capturas.
2. Commit acotado a paths warehouse/inteligencia-bodega (sin pycache / sin secretos lab).
3. Plan de migración: apply 001→003 en DSN de staging/aislado autorizado; smoke ERP.
4. Publicación formal de revisión (draft → published) con auditoría y rollback documentado.
5. Redeploy ERP+API solo tras verify SELECT-only / checklist.
6. No activar sync Bsale ni jobs externos como parte del go-live de layout.

**Fin slice 1.3 — DETENER.** Esperar autorización expresa para 1.4 / commit / push / SQL productivo / despliegue.
