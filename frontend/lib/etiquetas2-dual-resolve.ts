/**
 * Resolución dual Normal + Socio para Etiquetas V2.
 * Individual: 2× GET /labels/product
 * Masivo/Excel: 2× POST /labels/resolve (batch), merge por variant_id.
 */

import {
  lookupLabelProduct,
  resolveLabelProductsBatch,
  type LabelProductResolved,
} from "@/lib/api"
import {
  socioPriceStatus,
  statusLabel,
  type SocioPriceStatus,
} from "@/lib/etiquetas2-status"

export type { SocioPriceStatus }
export { socioPriceStatus, statusLabel }

export type Etiquetas2ProductRow = {
  id: string
  variantId: number
  barcode: string
  productType: string
  productName: string
  variantName: string
  displayName: string
  /** Precio lista normal */
  normalPrice: number | null
  /** Precio lista socio */
  socioPrice: number | null
  /** Reservado WOW — null en esta iteración */
  wowPrice: number | null
  quantity: number
  status: SocioPriceStatus
  normalListName?: string | null
  socioListName?: string | null
}

function newRowId(variantId: number): string {
  return `${variantId}-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`
}

function mergeDual(
  normal: LabelProductResolved | null,
  socio: LabelProductResolved | null,
  quantity: number,
): Etiquetas2ProductRow | null {
  const base = normal ?? socio
  if (!base) return null

  const normalPrice =
    normal?.price != null && Number.isFinite(normal.price) ? normal.price : null
  const socioPrice =
    socio?.price != null && Number.isFinite(socio.price) ? socio.price : null

  return {
    id: newRowId(base.variant_id),
    variantId: base.variant_id,
    barcode: base.barcode,
    productType: base.product_type || "",
    productName: base.product_name,
    variantName: base.variant_name || "",
    displayName: base.display_name,
    normalPrice,
    socioPrice,
    wowPrice: null,
    quantity: Math.max(1, quantity),
    status: socioPriceStatus(normalPrice, socioPrice),
    normalListName: normal?.price_list_name ?? null,
    socioListName: socio?.price_list_name ?? null,
  }
}

/** Escaneo / búsqueda individual: 2 requests paralelos. */
export async function lookupDualLabelProduct(
  companyId: number,
  normalListId: number,
  socioListId: number,
  barcode: string,
): Promise<Etiquetas2ProductRow | null> {
  const [normal, socio] = await Promise.all([
    lookupLabelProduct(companyId, normalListId, barcode),
    lookupLabelProduct(companyId, socioListId, barcode),
  ])
  return mergeDual(normal, socio, 1)
}

function indexResolved(
  items: (LabelProductResolved & { quantity: number })[],
): {
  byVariant: Map<number, LabelProductResolved & { quantity: number }>
  byBarcode: Map<string, LabelProductResolved & { quantity: number }>
} {
  const byVariant = new Map<number, LabelProductResolved & { quantity: number }>()
  const byBarcode = new Map<string, LabelProductResolved & { quantity: number }>()
  for (const it of items) {
    byVariant.set(it.variant_id, it)
    const bc = (it.matched_barcode || it.barcode || "").trim()
    if (bc) byBarcode.set(bc, it)
    const read = (it.read_barcode || "").trim()
    if (read) byBarcode.set(read, it)
  }
  return { byVariant, byBarcode }
}

/**
 * Excel / cola masiva: exactamente 2 batches (Normal + Socio), merge por variant_id.
 */
export async function resolveDualLabelProductsBatch(
  companyId: number,
  normalListId: number,
  socioListId: number,
  items: { barcode: string; quantity?: number }[],
): Promise<{
  resolved: Etiquetas2ProductRow[]
  errors: {
    line: number
    barcode: string
    error: string
  }[]
}> {
  const [normalBatch, socioBatch] = await Promise.all([
    resolveLabelProductsBatch(companyId, normalListId, items),
    resolveLabelProductsBatch(companyId, socioListId, items),
  ])

  const nIdx = indexResolved(normalBatch.resolved)
  const sIdx = indexResolved(socioBatch.resolved)

  const seenVariants = new Set<number>()
  const resolved: Etiquetas2ProductRow[] = []

  // Preferir filas presentes en Normal; completar Socio por variant_id.
  for (const n of normalBatch.resolved) {
    seenVariants.add(n.variant_id)
    const s =
      sIdx.byVariant.get(n.variant_id) ??
      sIdx.byBarcode.get((n.barcode || "").trim()) ??
      null
    const row = mergeDual(n, s, n.quantity)
    if (row) resolved.push(row)
  }

  // Productos solo en Socio (sin precio Normal) — aún se listan para revisión.
  for (const s of socioBatch.resolved) {
    if (seenVariants.has(s.variant_id)) continue
    seenVariants.add(s.variant_id)
    const row = mergeDual(null, s, s.quantity)
    if (row) resolved.push(row)
  }

  const errors = [
    ...normalBatch.errors.map((e) => ({
      line: e.line,
      barcode: e.barcode,
      error: `[Normal] ${e.error}`,
    })),
    ...socioBatch.errors.map((e) => ({
      line: e.line,
      barcode: e.barcode,
      error: `[Socio] ${e.error}`,
    })),
  ]

  return { resolved, errors }
}
