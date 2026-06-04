import type {
  DispatchPlanPickingClientRow,
  DispatchPlanPickingHeader,
  DispatchPlanPickingProductRow,
} from "@/lib/api"

export function productLineLabel(row: DispatchPlanPickingProductRow): string {
  if (row.display_name?.trim()) return row.display_name.trim()
  const pn = (row.product_name || row.producto || "").trim()
  const vn = (row.variant_name || row.variante || "").trim()
  if (pn && vn && pn !== vn) {
    if (pn.toLowerCase().endsWith(vn.toLowerCase())) return pn
    return `${pn} ${vn}`.trim()
  }
  return pn || vn || row.producto_variante || "Sin descripción"
}

export function effectiveBoxes(row: DispatchPlanPickingProductRow): number {
  if (row.cajas_efectivas != null) return Number(row.cajas_efectivas) || 0
  const u = Number(row.unidades) || 0
  const upb = Number(row.units_per_box_efectivo ?? row.units_per_box) || 0
  if (upb > 0 && u > 0) return Math.ceil(u / upb)
  return row.sin_unidad_caja ? 0 : Number(row.cajas) || 0
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
    estimated_boxes: totalBoxes,
    planning_number: header.planning_number,
  }
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
