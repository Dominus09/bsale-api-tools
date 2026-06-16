import type { PromotionGridRow } from "@/lib/api"
import { parsePrice } from "@/lib/promotions-utils"

const LABEL_STORAGE_KEY = "promo_label_generated_ids"

function readLabelIds(): Set<number> {
  if (typeof window === "undefined") return new Set()
  try {
    const raw = localStorage.getItem(LABEL_STORAGE_KEY)
    if (!raw) return new Set()
    const arr = JSON.parse(raw) as unknown
    if (!Array.isArray(arr)) return new Set()
    return new Set(arr.filter((x) => typeof x === "number"))
  } catch {
    return new Set()
  }
}

function writeLabelIds(ids: Set<number>) {
  if (typeof window === "undefined") return
  localStorage.setItem(LABEL_STORAGE_KEY, JSON.stringify([...ids]))
}

/** Preparado para futura sincronización con backend has_label_generated */
export function hasPromotionLabelGenerated(snapshotId: number): boolean {
  return readLabelIds().has(snapshotId)
}

export function markPromotionLabelGenerated(snapshotId: number) {
  const ids = readLabelIds()
  ids.add(snapshotId)
  writeLabelIds(ids)
}

export function enrichRowWithLabelStatus(row: PromotionGridRow): PromotionGridRow {
  return {
    ...row,
    has_label_generated: hasPromotionLabelGenerated(row.snapshot_id),
  }
}

export function buildEtiquetasUrlFromPromotion(row: PromotionGridRow): string {
  const regular = parsePrice(row.regular_price)
  const sale = parsePrice(row.sale_price)
  const params = new URLSearchParams({
    from: "promotion",
    company_id: String(row.company_id),
    barcode: row.codigo_barras,
    snapshot_id: String(row.snapshot_id),
    format: "C",
  })
  if (regular != null) params.set("regular_price", String(Math.round(regular)))
  if (sale != null) params.set("sale_price", String(Math.round(sale)))
  return `/sucursales/etiquetas?${params.toString()}`
}

export const QUICK_COMPANY_CHIPS = [
  { label: "Todas", match: null as string | null },
  { label: "La Quillotana", match: "quillotana spa" },
  { label: "Minimarket", match: "minimarket" },
  { label: "Carlos Romero", match: "carlos romero" },
] as const

export function resolveCompanyIdByChip(
  companies: { company_id: number; name: string }[],
  match: string | null,
): string {
  if (!match) return "all"
  const norm = match.toLowerCase()
  const found = companies.find((c) => c.name.trim().toLowerCase().includes(norm))
  return found ? String(found.company_id) : "all"
}
