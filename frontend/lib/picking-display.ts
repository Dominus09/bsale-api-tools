import type {
  DispatchPlanPickingClientRow,
  DispatchPlanPickingHeader,
  DispatchPlanPickingProductRow,
} from "@/lib/api"

/** Categoría para picking: OTROS solo si no hay tipo real. */
export function normalizePickingCategory(tipo?: string | null): string {
  const t = (tipo || "").trim()
  if (!t || t.toLowerCase() === "sin tipo") return "OTROS"
  return t
}

export function formatOperativeBoxes(value: number): string {
  if (!Number.isFinite(value)) return "—"
  return value.toFixed(2)
}

export function effectiveBoxes(row: DispatchPlanPickingProductRow): number {
  if (row.cajas_efectivas != null) return roundBoxes(Number(row.cajas_efectivas) || 0)
  const u = Number(row.unidades) || 0
  const upb = Number(row.units_per_box_efectivo ?? row.units_per_box) || 0
  if (upb > 0 && u > 0) return roundBoxes(u / upb)
  return row.sin_unidad_caja ? 0 : roundBoxes(Number(row.cajas) || 0)
}

function roundBoxes(n: number): number {
  return Math.round(n * 100) / 100
}

export function categoryStatsFromItems(
  items: DispatchPlanPickingProductRow[],
  category: string,
): { skus: number; units: number; boxes: number; monto: number } {
  const cat = normalizePickingCategory(category)
  const barcodes = new Set<string>()
  let units = 0
  let boxes = 0
  let monto = 0
  for (const it of items) {
    if (normalizePickingCategory(it.tipo_producto) !== cat) continue
    const bc = (it.codigo_barras || "").trim()
    if (bc) barcodes.add(bc)
    units += Number(it.unidades) || 0
    boxes += effectiveBoxes(it)
    monto += Number(it.total_monto) || 0
  }
  const lineCount = items.filter((i) => normalizePickingCategory(i.tipo_producto) === cat).length
  return {
    skus: barcodes.size || lineCount,
    units: Math.round(units),
    boxes: roundBoxes(boxes),
    monto: Math.round(monto),
  }
}

export function productLineLabel(row: DispatchPlanPickingProductRow): string {
  if (row.display_name?.trim()) return row.display_name.trim()
  const pn = (row.product_name || row.producto || "").trim()
  const vn = (row.variant_name || row.variante || "").trim()
  if (pn && vn) {
    if (pn.toLowerCase() === vn.toLowerCase() || pn.toLowerCase().endsWith(vn.toLowerCase())) return pn
    return `${pn} ${vn}`.trim()
  }
  return pn || vn || row.producto_variante || "Sin descripción"
}

export function clientPhone(row: DispatchPlanPickingClientRow): string {
  return (row.phone || "").trim()
}

export function clientDeliveryNotes(row: DispatchPlanPickingClientRow): string {
  const dn = (row.delivery_notes || "").trim()
  const obs = (row.observations || "").trim()
  if (dn && obs && dn !== obs) return `${dn} · ${obs}`
  return dn || obs
}

export function countDistinctClients(clients: DispatchPlanPickingClientRow[]): number {
  const keys = new Set<string>()
  for (const c of clients) {
    if (c.client_id != null && Number(c.client_id) !== 0) {
      keys.add(`id:${c.client_id}`)
    } else {
      const name = (c.client_name || c.fantasy_name || "").trim().toLowerCase()
      keys.add(name ? `name:${name}` : `doc:${c.related_document_id ?? c.document_number}`)
    }
  }
  return keys.size || clients.length
}

export function snapshotLoadKpis(
  header: DispatchPlanPickingHeader,
  clients: DispatchPlanPickingClientRow[],
  items: DispatchPlanPickingProductRow[],
) {
  const clientsN = countDistinctClients(clients)
  let totalUnits = 0
  let totalBoxes = 0
  const barcodes = new Set<string>()
  let sales = 0
  for (const c of clients) {
    sales += Number(c.document_total) || 0
  }
  for (const it of items) {
    totalUnits += Number(it.unidades) || 0
    totalBoxes += effectiveBoxes(it)
    const bc = (it.codigo_barras || "").trim()
    if (bc) barcodes.add(bc)
  }
  return {
    clients: clientsN,
    documents: clients.length,
    sales_total_clp: Math.round(sales),
    distinct_products: barcodes.size || items.length,
    total_units: totalUnits,
    estimated_boxes: roundBoxes(totalBoxes),
    planning_number: header.planning_number,
  }
}

/** KPIs estables en todas las páginas del PDF (mezcla snapshot + header.load_kpis). */
export function stablePickingKpiLine(
  header: DispatchPlanPickingHeader,
  clients: DispatchPlanPickingClientRow[],
  items: DispatchPlanPickingProductRow[],
  formatClp: (n: number) => string,
): string {
  const k = snapshotLoadKpis(header, clients, items)
  const hk = header.load_kpis
  const merged = {
    clients: k.clients || hk?.clients || 0,
    documents: k.documents || hk?.documents || 0,
    sales_total_clp: k.sales_total_clp || hk?.sales_total_clp || 0,
    distinct_products: k.distinct_products || hk?.distinct_products || 0,
    total_units: k.total_units || hk?.total_units || 0,
    estimated_boxes: k.estimated_boxes || hk?.estimated_boxes || 0,
  }
  return (
    `${merged.clients} clientes · ${merged.documents} documentos · ${formatClp(merged.sales_total_clp)} · ` +
    `${merged.distinct_products} SKU · ${Math.round(merged.total_units)} u · ${formatOperativeBoxes(merged.estimated_boxes)} cajas`
  )
}

export function formatPickingGeneratedAt(iso?: string | null): string {
  if (!iso) {
    const d = new Date()
    return d.toLocaleString("es-CL", {
      day: "2-digit",
      month: "2-digit",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    })
  }
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso.slice(0, 16)
  return d.toLocaleString("es-CL", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  })
}
