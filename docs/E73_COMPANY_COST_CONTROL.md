# Control de costos — ruta oficial `/costos`

## Cambio definitivo de ruta

La interfaz consolidada por empresa vive únicamente en:

- `https://test.quillotana.cl/costos`
- `frontend/app/(dashboard)/costos/page.tsx`

La ruta `/costos-v2` fue **eliminada** (sin redirect).

## Organización frontend

| Área | Ubicación |
|------|-----------|
| Página | `frontend/app/(dashboard)/costos/page.tsx` |
| Componentes consolidados | `frontend/components/costos/cost-*.tsx` (sin prefijo v2 en nombre de archivo) |
| Cliente/API/labels | `frontend/lib/costos/control/` |
| Helpers legacy compartidos (márgenes, etc.) | `frontend/lib/costos/format.ts`, `quality-labels.ts`, `adapt-cost-analytics.ts` |
| Componentes legacy no usados por la página | `cost-main-table`, `cost-detail-drawer`, `cost-history-chart`, `cost-quality-badge` (conservados por dependencias) |

## Backend (intactos)

Endpoints `/cost-analytics/v2/*` y company-* siguen siendo la fuente de datos.
No se eliminaron por el cambio de ruta frontend.

## Sidebar

`Control de costos` → `/costos`

## Confirmaciones

- Cursor no desplegó producción.
- `/costos-v2` no existe (404 esperado).
- No hay redirect `/costos-v2` → `/costos`.
