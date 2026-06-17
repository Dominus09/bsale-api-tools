import type { PromotionGridRow } from "@/lib/api"

export type PromotionEstadoVisual = "Activa" | "Próxima" | "Vencida" | "Pausada"

export function parsePrice(value: number | string | null | undefined): number | null {
  const n = typeof value === "string" ? parseFloat(value) : value
  return n != null && Number.isFinite(n) ? n : null
}

export function formatCurrency(value: number | string | null | undefined) {
  const n = parsePrice(value)
  if (n == null) return "—"
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  }).format(n)
}

export function calcDiscountPercent(
  regular: number | string | null | undefined,
  sale: number | string | null | undefined,
): number | null {
  const r = parsePrice(regular)
  const s = parsePrice(sale)
  if (r == null || s == null || r <= 0) return null
  return Math.round(((r - s) / r) * 100)
}

export function formatDiscountBadge(
  regular: number | string | null | undefined,
  sale: number | string | null | undefined,
): string {
  const pct = calcDiscountPercent(regular, sale)
  if (pct == null || pct <= 0) return "—"
  return `-${pct}%`
}

export function mapEstadoVisual(estado: string): PromotionEstadoVisual {
  switch (estado) {
    case "Programada":
      return "Próxima"
    case "Inactiva":
      return "Pausada"
    case "Vencida":
      return "Vencida"
    case "Activa":
    default:
      return estado === "Activa" ? "Activa" : "Pausada"
  }
}

export function estadoVisualClass(estado: PromotionEstadoVisual) {
  switch (estado) {
    case "Activa":
      return "bg-emerald-50 text-emerald-800 border-emerald-200"
    case "Próxima":
      return "bg-sky-50 text-sky-800 border-sky-200"
    case "Vencida":
      return "bg-zinc-100 text-zinc-600 border-zinc-200"
    case "Pausada":
    default:
      return "bg-amber-50 text-amber-900 border-amber-200"
  }
}

export function estadoDotClass(estado: PromotionEstadoVisual) {
  switch (estado) {
    case "Activa":
      return "bg-emerald-500"
    case "Próxima":
      return "bg-sky-500"
    case "Vencida":
      return "bg-zinc-400"
    case "Pausada":
    default:
      return "bg-amber-400"
  }
}

export function calcSavings(
  regular: number | string | null | undefined,
  sale: number | string | null | undefined,
): number | null {
  const r = parsePrice(regular)
  const s = parsePrice(sale)
  if (r == null || s == null) return null
  return Math.max(0, Math.round(r - s))
}

export type PromotionKpis = {
  activas: number
  proximas: number
  vencidas: number
  remates: number
  empresas: number
}

export function computePromotionKpis(rows: PromotionGridRow[]): PromotionKpis {
  const companyIds = new Set<number>()
  let activas = 0
  let proximas = 0
  let vencidas = 0
  let remates = 0
  for (const row of rows) {
    companyIds.add(row.company_id)
    if (row.estado === "Activa") activas += 1
    if (row.estado === "Programada") proximas += 1
    if (row.estado === "Vencida") vencidas += 1
    if (row.tipo?.toLowerCase() === "remate") remates += 1
  }
  return {
    activas,
    proximas,
    vencidas,
    remates,
    empresas: companyIds.size,
  }
}

function parseLocalDate(iso: string): Date | null {
  const key = (iso || "").slice(0, 10)
  if (!key) return null
  const d = new Date(`${key}T12:00:00`)
  return Number.isNaN(d.getTime()) ? null : d
}

function startOfLocalDay(d: Date): Date {
  return new Date(d.getFullYear(), d.getMonth(), d.getDate(), 12, 0, 0)
}

/** Prioridad: vence hoy → mañana → esta semana → resto */
export function expiryUrgencyRank(fechaFin: string): number {
  const end = parseLocalDate(fechaFin)
  if (!end) return 99
  const today = startOfLocalDay(new Date())
  const endDay = startOfLocalDay(end)
  const diffMs = endDay.getTime() - today.getTime()
  const diffDays = Math.round(diffMs / (24 * 60 * 60 * 1000))
  if (diffDays < 0) return 4
  if (diffDays === 0) return 0
  if (diffDays === 1) return 1
  if (diffDays <= 7) return 2
  return 3
}

export function sortByExpiryUrgency(rows: PromotionGridRow[]): PromotionGridRow[] {
  return [...rows].sort((a, b) => {
    const ra = expiryUrgencyRank(a.fecha_fin)
    const rb = expiryUrgencyRank(b.fecha_fin)
    if (ra !== rb) return ra - rb
    return (a.fecha_fin || "").localeCompare(b.fecha_fin || "")
  })
}

export function filterRowsForTab(
  rows: PromotionGridRow[],
  tab: string,
  filterEstado: string,
): PromotionGridRow[] {
  let out = rows
  if (tab === "activas") {
    out = out.filter((r) => r.estado === "Activa")
    return sortByExpiryUrgency(out)
  }
  if (filterEstado !== "all") {
    out = out.filter((r) => r.estado === filterEstado)
  }
  return out
}

export const DEFAULT_PROMOTION_PRICE_LIST = "Supermercado La Quillotana"

export function tipoCalendarColor(tipo: string): string {
  const t = tipo.toLowerCase()
  if (t === "remate") return "border-l-orange-500 bg-orange-50/50"
  if (t === "promocion") return "border-l-violet-500 bg-violet-50/50"
  return "border-l-sky-500 bg-sky-50/50"
}

export function calendarRowClass(tipo: string, estado: string): string {
  if (estado === "Vencida") {
    return "border-l-zinc-400 bg-zinc-100/80 text-zinc-600 opacity-80"
  }
  return tipoCalendarColor(tipo)
}

export function formatDateShort(iso: string | null | undefined): string {
  if (!iso) return "—"
  const d = new Date(iso.includes("T") ? iso : `${iso}T12:00:00`)
  if (Number.isNaN(d.getTime())) return iso
  return d.toLocaleDateString("es-CL", { day: "2-digit", month: "2-digit" })
}

export function formatVigencia(inicio: string, fin: string): string {
  return `${formatDateShort(inicio)} – ${formatDateShort(fin)}`
}

export function productDisplayName(row: PromotionGridRow): string {
  const name = (row.producto || "").trim()
  const variant = (row.variante || "").trim()
  if (!name && !variant) return row.codigo_barras || "Producto"
  if (!variant || name.toLowerCase().includes(variant.toLowerCase())) return name
  return `${name} ${variant}`.trim()
}

export function productTitleLines(row: PromotionGridRow): { line1: string; line2: string } {
  const name = (row.producto || "").trim()
  const variant = (row.variante || "").trim()
  if (name && variant && !name.toLowerCase().includes(variant.toLowerCase())) {
    return { line1: name.toUpperCase(), line2: variant.toUpperCase() }
  }
  const full = productDisplayName(row)
  const words = full.split(/\s+/)
  if (words.length <= 1) return { line1: full.toUpperCase(), line2: "" }
  const mid = Math.ceil(words.length / 2)
  return {
    line1: words.slice(0, mid).join(" ").toUpperCase(),
    line2: words.slice(mid).join(" ").toUpperCase(),
  }
}

export type CalendarDayGroup = {
  dateKey: string
  dateLabel: string
  items: PromotionGridRow[]
}

export type CalendarMonthGroup = {
  monthKey: string
  monthLabel: string
  days: CalendarDayGroup[]
}

export function groupPromotionsByStartDate(rows: PromotionGridRow[]): CalendarMonthGroup[] {
  const byDate = new Map<string, PromotionGridRow[]>()
  for (const row of rows) {
    const key = (row.fecha_inicio || "").slice(0, 10)
    if (!key) continue
    const list = byDate.get(key) ?? []
    list.push(row)
    byDate.set(key, list)
  }

  const sortedDates = [...byDate.keys()].sort((a, b) => b.localeCompare(a))
  const monthMap = new Map<string, CalendarDayGroup[]>()

  for (const dateKey of sortedDates) {
    const d = new Date(`${dateKey}T12:00:00`)
    const monthKey = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`
    const monthLabel = d.toLocaleDateString("es-CL", { month: "long", year: "numeric" })
    const dateLabel = d.toLocaleDateString("es-CL", {
      day: "numeric",
      month: "long",
    })
    const day: CalendarDayGroup = {
      dateKey,
      dateLabel,
      items: byDate.get(dateKey) ?? [],
    }
    const existing = monthMap.get(monthKey)
    if (existing) {
      existing.push(day)
    } else {
      monthMap.set(monthKey, [day])
    }
  }

  return [...monthMap.entries()]
    .sort(([a], [b]) => b.localeCompare(a))
    .map(([monthKey, days]) => {
      const d = new Date(`${days[0]?.dateKey ?? monthKey}-01T12:00:00`)
      return {
        monthKey,
        monthLabel: d.toLocaleDateString("es-CL", { month: "long", year: "numeric" }),
        days: days.sort((a, b) => b.dateKey.localeCompare(a.dateKey)),
      }
    })
}

export function parsePriceInput(value: string): number | null {
  const n = parseFloat(value.replace(/\./g, "").replace(",", "."))
  return Number.isFinite(n) && n >= 0 ? n : null
}
