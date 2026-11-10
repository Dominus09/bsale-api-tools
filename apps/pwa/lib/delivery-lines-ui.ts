/** Filtros/búsqueda de líneas de documento (P4.1 complemento UX). */

export type LineFilter = "pending" | "registered" | "all"

export type LineOutcome = "full" | "partial" | "none" | null

export type LineUiRow = {
  source_detail_id: number
  product_name: string
  sku_code?: string | null
  barcode?: string | null
  planned: number
  delivered: number
  reason?: string | null
  note?: string | null
  /** Solo true tras confirmación del backend (hidratación o respuesta de guardado). */
  persisted: boolean
}

export function lineOutcome(row: Pick<LineUiRow, "planned" | "delivered" | "persisted">): LineOutcome {
  if (!row.persisted) return null
  const planned = Number(row.planned) || 0
  const delivered = Number(row.delivered) || 0
  if (delivered >= planned) return "full"
  if (delivered <= 0) return "none"
  return "partial"
}

export function lineOutcomeLabel(outcome: LineOutcome): string {
  if (outcome === "full") return "Completa"
  if (outcome === "partial") return "Parcial"
  if (outcome === "none") return "No entregada"
  return "Pendiente"
}

export function matchesLineSearch(
  row: Pick<LineUiRow, "product_name" | "sku_code" | "barcode" | "source_detail_id">,
  query: string,
): boolean {
  const q = query.trim().toLowerCase()
  if (!q) return true
  const hay = [
    row.product_name,
    row.sku_code,
    row.barcode,
    String(row.source_detail_id),
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase()
  return hay.includes(q)
}

export function filterLines<T extends LineUiRow>(
  rows: T[],
  filter: LineFilter,
  query: string,
): T[] {
  return rows.filter((r) => {
    if (filter === "pending" && r.persisted) return false
    if (filter === "registered" && !r.persisted) return false
    return matchesLineSearch(r, query)
  })
}

export function lineProgress(rows: LineUiRow[]): {
  total: number
  registered: number
  pending: number
} {
  const total = rows.length
  const registered = rows.filter((r) => r.persisted).length
  return { total, registered, pending: Math.max(0, total - registered) }
}

export function reasonLabel(code: string | null | undefined): string {
  const map: Record<string, string> = {
    physical_shortage: "Faltante físico",
    damaged: "Producto dañado",
    customer_rejection: "Rechazo del cliente",
    prep_error: "Error de preparación",
    other: "Otro",
  }
  if (!code) return ""
  return map[code] || code
}
