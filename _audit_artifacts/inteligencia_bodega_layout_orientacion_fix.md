# Corrección layout inicial BC — orientación y geometrías

**Fecha:** 2026-11-25 (rev. OFC/P*/guards)  
**Estado:** SQL `006` corregido · tests aislados OK · **pendiente autorización** · sin SQL productivo / commit / push / redeploy

---

## 1. Diagnóstico de la orientación incorrecta

### Causa

El SoT métrico ya usaba:

| Eje | Significado seed | Extensión |
|-----|------------------|-----------|
| `+X` / `width_m` | Largo longitudinal | 23,10 m |
| `+Y` / `depth_m` | Ancho transversal | 13,70 m |

Pero el editor mapeaba **pantalla = landscape**:

- `screen_x ∝ x_m` → el largo aparecía **horizontal**
- `screen_y ∝ invert(y_m)` → el ancho aparecía **vertical**

Resultado en producción: plano de ~23 m de ancho × ~13 m de alto, con P1→P7 de izquierda a derecha y el portón sobre el muro **largo** (`y=0`), no sobre el muro corto de frente.

No bastaba rotar 90° el Stage: eso habría invertido también IZ/DER respecto al frente/fondo. Hacía falta **intercambiar ejes** de forma explícita:

- horizontal pantalla ← `y_m` (IZ izquierda)
- vertical pantalla ← `x_m` invertido (entrada/`x≈0` abajo, fondo arriba)

### Separación vista vs persistencia

| Capa | Qué cambió |
|------|------------|
| Transformación FE (`coords.ts` + canvas/impresión) | Vista canónica longitudinal + control `Girar vista` 0/90/180/270 **sin mutar** geometría |
| Geometría draft (SQL `006`, seeds 002/003) | PORTON / OFC / ESC / IZ-SU / BAE-* reubicados; IZ/CIRC/DER/P1–P7 intactos |

---

## 2. Captura

`_audit_artifacts/inteligencia_bodega_layout_longitudinal.png`  
Preview local con geometría **propuesta** (post-006): entrada abajo, IZ | CIRC | P1…P7, IZ-SU/ESC/BAE al fondo.

---

## 3. Archivos modificados

### Frontend (vista / UX)

- `frontend/lib/warehouse/coords.ts` — transformación longitudinal + `viewOrientationDeg`
- `frontend/lib/warehouse/coords.test.mjs` — tests de orientación / round-trip
- `frontend/lib/warehouse/style.ts` — colores menos saturados, helpers de etiqueta
- `frontend/components/warehouse/WarehouseCanvas.tsx` — fit, grid, markers FRENTE/FONDO, botón girar vista
- `frontend/components/warehouse/WarehouseEditor.tsx` — estado de orientación de vista
- `frontend/components/warehouse/PrintLayoutSheet.tsx` — impresión longitudinal alineada

### Backend / SQL

- `backend/services/warehouse/coordinates.py` — convención SoT + colores simbología
- `backend/tests/test_warehouse_layout_unit.py` — asserts de convención
- `backend/sql/warehouse/002_…` / `003_…` — seeds para **instalaciones nuevas**
- `backend/sql/warehouse/006_fix_bc_draft_layout_orientation_AWAITING_APPROVAL.sql` — corrección **draft BC existente**

### Artefacto

- `_audit_artifacts/inteligencia_bodega_layout_longitudinal.png`
- este informe

---

## 4. Pruebas

| Prueba | Resultado |
|--------|-----------|
| `node frontend/lib/warehouse/coords.test.mjs` | OK (entrada abajo, IZ izquierda, P1 bajo P7, round-trip, vista 90°) |
| Suite warehouse aislada | **33 passed** |
| `npx next build` | **Compiled successfully** (`/inteligencia-bodega` presente) |

---

## 5. SQL `006` (no ejecutado)

Ruta: `backend/sql/warehouse/006_fix_bc_draft_layout_orientation_AWAITING_APPROVAL.sql`

- Transaccional; solo `layout_elements` del **draft** BC/PISO1
- Preserva `physical_elements` / IDs
- No publica; no toca schemas ajenos
- Bump `row_version` + audit `fix_bc_draft_layout_006`
- Incluye PRECHECK de `revision_id` / `row_version` (en UI se vio draft **v3**; confirmar antes de aplicar)

Geometrías objetivo:

| Código | x_m | y_m | width_m | depth_m |
|--------|-----|-----|---------|---------|
| PORTON | 0 | 4.675 | 0.40 | 4.35 |
| OFC | 0.40 | 10.20 | 1.80 | 2.80 |
| ESC | 21.50 | 9.70 | 1.60 | 3.50 |
| IZ-SU | 20.58 | 0.10 | 2.52 | 1.06 |
| BAE-01 | 21.55 | 7.55 | 1.26 | 1.06 |
| BAE-02 | 21.55 | 8.75 | 1.26 | 1.06 |

Sin `006`, la **vista longitudinal** ya mejora el plano actual, pero PORTON seguirá en el muro largo hasta aplicar el SQL o editar en el editor.

---

## 6. Pendiente del usuario

1. Revisar captura + este diagnóstico.
2. Autorizar (o ajustar) `006` y ejecutarlo manualmente en `analytics`.
3. Luego autorizar commit/push/redeploy FE (y API solo si se desea el meta de convención actualizado).
4. No publicar el plano.

Sin Fase 2 · sin PWA · sin stock.
