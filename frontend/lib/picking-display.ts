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

function roundBoxes(n: number): number {
  return Math.round(n * 100) / 100
}

/** Cantidades operativas: conserva fracciones; no redondea a entero arbitrario. */
export function formatPickingQuantity(value: number | null | undefined): string {
  const n = Number(value)
  if (!Number.isFinite(n)) return "—"
  const rounded = roundBoxes(n)
  return Number.isInteger(rounded) ? String(rounded) : rounded.toFixed(2)
}

/**
 * Equivalencia comercial en cajas (número), usando units_per_box confiable.
 * No implica bulto físico precintado. null = no disponible.
 */
export function commercialBoxesValue(row: {
  units_per_box?: number | null
  units_per_box_efectivo?: number | null
  unidades?: number | null
  sin_unidad_caja?: boolean | null
  cajas_efectivas?: number | null
  cajas?: number | null
}): number | null {
  if (row.cajas_efectivas != null && Number.isFinite(Number(row.cajas_efectivas))) {
    return roundBoxes(Number(row.cajas_efectivas))
  }
  const upb = Number(row.units_per_box_efectivo ?? row.units_per_box) || 0
  const units = Number(row.unidades) || 0
  if (upb > 0 && units > 0) return roundBoxes(units / upb)
  if (row.sin_unidad_caja) return null
  const raw = Number(row.cajas)
  if (Number.isFinite(raw) && raw > 0) return roundBoxes(raw)
  return null
}

/** Celda «Cajas»: número o «n/d» si no hay equivalencia verificable. */
export function formatCommercialBoxesCell(row: Parameters<typeof commercialBoxesValue>[0]): string {
  const v = commercialBoxesValue(row)
  if (v == null) return "n/d"
  return formatPickingQuantity(v)
}

/** @deprecated Preferir formatCommercialBoxesCell (solo número). */
export function commercialBoxEquivLabel(row: Parameters<typeof commercialBoxesValue>[0]): string {
  return formatCommercialBoxesCell(row)
}

export function effectiveBoxes(row: DispatchPlanPickingProductRow): number {
  return commercialBoxesValue(row) ?? 0
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
    units: Math.round(units * 100) / 100,
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

const COMMUNE_CANONICAL: Record<string, string> = {
  quellon: "Quellón",
}

const SECTOR_TOKENS = new Set([
  "rural",
  "urbano",
  "urbana",
  "norte",
  "sur",
  "oriente",
  "poniente",
  "centro",
])

function stripAccents(value: string): string {
  return value.normalize("NFD").replace(/\p{M}/gu, "")
}

function titleEs(value: string): string {
  return value
    .split(" ")
    .filter(Boolean)
    .map((p) => p.charAt(0).toUpperCase() + p.slice(1).toLowerCase())
    .join(" ")
}

/** Clave de agrupación: sin acentos, minúsculas, espacios colapsados. */
export function normalizeCityKey(city?: string | null): string {
  if (!city?.trim()) return ""
  return stripAccents(city).trim().replace(/\s+/g, " ").toLowerCase()
}

export type CommuneSector = {
  communeKey: string
  displayCommune: string
  displaySector: string
  headerLabel: string
}

/** Separa comuna/sector solo para presentación (no altera datos fuente). */
export function parseCommuneSector(city?: string | null): CommuneSector {
  const label = (city || "").trim().replace(/\s+/g, " ")
  if (!label) {
    return { communeKey: "", displayCommune: "", displaySector: "", headerLabel: "" }
  }
  const parts = label.split(" ")
  let sector = ""
  let communeParts = parts
  const last = stripAccents(parts[parts.length - 1] || "").toLowerCase()
  if (parts.length >= 2 && SECTOR_TOKENS.has(last)) {
    sector = parts[parts.length - 1]
    communeParts = parts.slice(0, -1)
  }
  const communeRaw = communeParts.join(" ").trim() || label
  let key = normalizeCityKey(communeRaw)
  for (const tok of SECTOR_TOKENS) {
    if (key.endsWith(` ${tok}`)) {
      key = key.slice(0, -(tok.length + 1)).trim()
      if (!sector) sector = tok
    }
  }
  const displayCommune = COMMUNE_CANONICAL[key] || titleEs(communeRaw)
  const displaySector = sector ? titleEs(sector) : ""
  return {
    communeKey: key,
    displayCommune,
    displaySector,
    headerLabel: displaySector ? `${displayCommune} · ${displaySector}` : displayCommune,
  }
}

/** Etiqueta visible de ciudad (comuna canónica + sector si aplica). */
export function displayCityLabel(city?: string | null): string {
  const parsed = parseCommuneSector(city)
  return parsed.headerLabel || (city || "").trim().replace(/\s+/g, " ")
}

/** Numeración visible 1..N aunque route_order interno empiece en 0. */
export function visibleStopNumber(
  routeOrder: number | null | undefined,
  minRouteOrder = 0,
): number {
  const ro = Number(routeOrder)
  if (!Number.isFinite(ro)) return 1
  if (minRouteOrder <= 0) return ro - minRouteOrder + 1
  return ro >= 1 ? ro : ro + 1
}

export type CityGroupedRow<T> = {
  cityKey: string
  cityLabel: string
  rows: T[]
}

/**
 * Agrupa por comuna canónica (sin sector), para no dividir Quellón/Quellon.
 * El sector queda en la etiqueta de fila; no reordena la ruta.
 */
export function groupRowsByCity<T extends { city?: string | null }>(
  rows: T[],
): CityGroupedRow<T>[] {
  const map = new Map<string, CityGroupedRow<T>>()
  const order: string[] = []
  for (const row of rows) {
    const geo = parseCommuneSector(row.city)
    const key = geo.communeKey || normalizeCityKey(row.city) || "__sin_ciudad__"
    const label = geo.displayCommune || displayCityLabel(row.city) || "Sin ciudad"
    let group = map.get(key)
    if (!group) {
      group = { cityKey: key, cityLabel: label, rows: [] }
      map.set(key, group)
      order.push(key)
    }
    group.rows.push(row)
  }
  return order.map((k) => map.get(k)!)
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
