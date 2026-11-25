# Layout BC corregido — instalaciones nuevas

Los archivos `001`–`004` **aplicados en producción no se modifican**.  
La corrección geométrica del draft productivo va en `006_fix_bc_draft_layout_orientation_AWAITING_APPROVAL.sql` (guards `revision_id=1` / `row_version=1` / `analytics`).

## Cómo reproducir el layout corregido en una BD nueva

1. Aplicar en orden (como go-live original): `001` → `002` → `003` → `004`.
2. Aplicar el overlay **sin IDs productivos**:  
   `APPLY_CORRECTED_BC_LAYOUT_NEW_INSTALLS.sql`  
   (busca draft `BC`/`PISO1` por código; no exige `revision_id=1` ni BD `analytics`).
3. Verificar con las consultas del final del overlay (17 elementos, `published=0`, BAE ⊂ ESC).

No usar el SQL `006` productivo en lab/staging con otros IDs: fallará a propósito.

## Coordenadas objetivo (equiv. 006 + 007)

Tras el overlay: OFC/P1–P7 como en 006; **ESC/BAE en CIRC** (frente a P7, Y ⊂ 2,65–7,35); IZ-SU compacta fondo IZ.  
Detalle: `_audit_artifacts/inteligencia_bodega_layout_007_esc_izsu.md`.
