/** Filtrado y paginación visual del picking de productos (sin mutar datos). */

export const PICKING_PRODUCT_PAGE_SIZE = 15

/** Forma mínima compatible con DispatchPlanPickingProductRow (sin acoplar a @/lib/api). */
export type PickingProductTableRow = {
  sucursal_bodega?: string | null
  tipo_producto?: string | null
  producto_variante?: string | null
  display_name?: string | null
  codigo_barras?: string | null
  unidades?: number | null
  total_monto?: number | null
  units_per_box?: number | null
  cajas_efectivas?: number | null
  sin_unidad_caja?: boolean
}

export type PickingProductTableFilters = {
  query?: string
  category?: string | null
}

function normalizeCategory(tipo?: string | null): string {
  const t = (tipo || "").trim()
  if (!t || t.toLowerCase() === "sin tipo") return "OTROS"
  return t
}

function lineLabel(row: PickingProductTableRow): string {
  return (row.display_name || row.producto_variante || "").trim()
}

function boxesOf(row: PickingProductTableRow): number {
  if (row.sin_unidad_caja) return 0
  if (row.cajas_efectivas != null && Number.isFinite(Number(row.cajas_efectivas))) {
    return Number(row.cajas_efectivas)
  }
  const u = Number(row.unidades) || 0
  const per = Number(row.units_per_box) || 0
  if (per > 0) return Math.round((u / per) * 100) / 100
  return 0
}

export function uniquePickingProductCategories(
  items: PickingProductTableRow[],
): string[] {
  const set = new Set<string>()
  for (const row of items) {
    set.add(normalizeCategory(row.tipo_producto))
  }
  return [...set].sort((a, b) => a.localeCompare(b, "es"))
}

export function filterPickingProductRows(
  items: PickingProductTableRow[],
  filters: PickingProductTableFilters,
): PickingProductTableRow[] {
  const q = (filters.query || "").trim().toLowerCase()
  const cat = (filters.category || "").trim()
  const wantCat = cat && cat !== "__all__" ? normalizeCategory(cat) : null
  if (!q && !wantCat) return items
  return items.filter((row) => {
    if (wantCat && normalizeCategory(row.tipo_producto) !== wantCat) {
      return false
    }
    if (!q) return true
    const hay = [
      lineLabel(row),
      row.producto_variante,
      row.codigo_barras,
      row.sucursal_bodega,
      row.tipo_producto,
      normalizeCategory(row.tipo_producto),
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase()
    return hay.includes(q)
  })
}

export function paginatePickingProductRows<T>(
  items: T[],
  page: number,
  pageSize: number = PICKING_PRODUCT_PAGE_SIZE,
): {
  page: number
  pageSize: number
  total: number
  totalPages: number
  from: number
  to: number
  rows: T[]
} {
  const size = Math.max(1, Math.floor(pageSize) || PICKING_PRODUCT_PAGE_SIZE)
  const total = items.length
  const totalPages = Math.max(1, Math.ceil(total / size) || 1)
  const safePage = Math.min(Math.max(1, Math.floor(page) || 1), totalPages)
  const start = (safePage - 1) * size
  const rows = items.slice(start, start + size)
  const from = total === 0 ? 0 : start + 1
  const to = total === 0 ? 0 : start + rows.length
  return { page: safePage, pageSize: size, total, totalPages, from, to, rows }
}

export function summarizePickingProductRows(
  items: PickingProductTableRow[],
): { skus: number; units: number; boxes: number; monto: number } {
  const barcodes = new Set<string>()
  let units = 0
  let boxes = 0
  let monto = 0
  for (const it of items) {
    const bc = (it.codigo_barras || "").trim()
    if (bc) barcodes.add(bc)
    units += Number(it.unidades) || 0
    boxes += boxesOf(it)
    monto += Number(it.total_monto) || 0
  }
  return {
    skus: barcodes.size || items.length,
    units: Math.round(units * 100) / 100,
    boxes: Math.round(boxes * 100) / 100,
    monto: Math.round(monto),
  }
}
