# 007 — ESC en CIRC (definitivo · pendiente aplicación productiva)

**Estado:** SQL definitivo listo · **no ejecutado en analytics** · sin publish · sin commit hasta autorización

## Geometría

| Código | x_m | y_m | width_m | depth_m | Notas |
|--------|-----|-----|---------|---------|-------|
| IZ-SU | 21.84 | 0.10 | 1.26 | 2.12 | Fondo IZ (~2 pallets) |
| ESC | 21.58 | 4.05 | 1.52 | 3.20 | CIRC (Y 4,05–7,25 ⊂ 2,65–7,35) |
| BAE-01 | 21.68 | 4.20 | 1.26 | 1.06 | ⊂ ESC · pending seguridad |
| BAE-02 | 21.68 | 5.40 | 1.26 | 1.06 | ⊂ ESC · pending seguridad |
| P7 | 19.60 | 7.35 | 2.80 | 6.00 | **Sin cambio** |

- Parent físico ESC → CIRC; `parent_physical_id_snap` alineado (guard en SQL).
- Dims ESC/BAE provisionales; `safe_storage_authorized=false` en BAE.
- Ambigüedad PL/pgSQL `r RECORD` **corregida** (alias `rev`, sin variable `r`).

## Vista editor

- Peldaños ⊥ subida (+Y hacia P7).
- Flecha “2° piso” hacia DER/P7.
- FONDO desplazado a IZ para no solapar ESC/BAE.
- BAE seleccionables/editables individualmente (capa sobre ESC).

Archivo: `backend/sql/warehouse/007_fix_bc_draft_esc_izsu_bae_AWAITING_APPROVAL.sql`
