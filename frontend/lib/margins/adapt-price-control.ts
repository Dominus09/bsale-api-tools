/**
 * Adaptadores /margins — control de precios por lista (sin ventas).
 */

import {
  PRICE_POLICY_STATUS_LABEL,
  type PricePolicyStatus,
} from "./price-policy"

export type PriceControlRow = {
  companyId: number
  productTypeId: number | null
  productTypeName: string | null
  productName: string | null
  variantId: number
  variantName: string | null
  barcode: string | null
  sku: string | null
  priceListId: number
  priceListName: string | null
  stockQuantity: number | null
  grossPrice: number | null
  referenceGrossCost: number | null
  costDate: string | null
  costSource: string | null
  costAgeDays: number | null
  grossCostQuality: string | null
  isOutlier: boolean
  isStale: boolean
  resolutionReason: string | null
  actualMarkupPct: number | null
  grossMarginPct: number | null
  minMarkupPct: number | null
  maxMarkupPct: number | null
  minimumRecommendedGrossPrice: number | null
  maximumRecommendedGrossPrice: number | null
  priceAdjustmentToMinimum: number | null
  priceDiffVsCost: number | null
  status: PricePolicyStatus
  policyCompliance: PricePolicyStatus | null
  hasRule: boolean
}

export type PriceControlSummary = {
  evaluatedPairs: number
  withinPolicy: number
  withinPolicyPct: number | null
  belowMinimum: number
  aboveMaximum: number
  missingRule: number
  missingCost: number
  missingPrice: number
  staleCost: number
  costOutlier: number
  conflictingCost: number
  needsReview: number
  byStatus: Record<string, number>
}

function num(v: unknown): number | null {
  if (v === null || v === undefined || v === "") return null
  const n = typeof v === "number" ? v : Number(v)
  return Number.isFinite(n) ? n : null
}

function asStatus(v: unknown): PricePolicyStatus {
  const s = String(v ?? "")
  if (s in PRICE_POLICY_STATUS_LABEL) return s as PricePolicyStatus
  return "missing_price"
}

export function adaptPriceControlRow(raw: Record<string, unknown>): PriceControlRow {
  return {
    companyId: Number(raw.company_id) || 0,
    productTypeId: num(raw.product_type_id),
    productTypeName: (raw.product_type_name as string) ?? null,
    productName: (raw.product_name as string) ?? null,
    variantId: Number(raw.variant_id) || 0,
    variantName: (raw.variant_name as string) ?? null,
    barcode: (raw.barcode as string) ?? null,
    sku: (raw.sku as string) ?? null,
    priceListId: Number(raw.price_list_id) || 0,
    priceListName: (raw.price_list_name as string) ?? null,
    stockQuantity: num(raw.stock_quantity),
    grossPrice: num(raw.gross_price ?? raw.price),
    referenceGrossCost: num(raw.reference_gross_cost ?? raw.cost),
    costDate: (raw.cost_date as string) ?? null,
    costSource: (raw.cost_source as string) ?? null,
    costAgeDays: num(raw.cost_age_days),
    grossCostQuality: (raw.gross_cost_quality as string) ?? null,
    isOutlier: Boolean(raw.is_outlier),
    isStale: Boolean(raw.is_stale),
    resolutionReason: (raw.resolution_reason as string) ?? null,
    actualMarkupPct: num(raw.actual_markup_pct ?? raw.margin_percent),
    grossMarginPct: num(raw.gross_margin_pct),
    minMarkupPct: num(raw.min_markup_pct ?? raw.min_margin_percent),
    maxMarkupPct: num(raw.max_markup_pct),
    minimumRecommendedGrossPrice: num(raw.minimum_recommended_gross_price),
    maximumRecommendedGrossPrice: num(raw.maximum_recommended_gross_price),
    priceAdjustmentToMinimum: num(raw.price_adjustment_to_minimum),
    priceDiffVsCost: num(raw.price_diff_vs_cost),
    status: asStatus(raw.status),
    policyCompliance: raw.policy_compliance
      ? asStatus(raw.policy_compliance)
      : null,
    hasRule: Boolean(raw.has_rule),
  }
}

export function adaptPriceControlSummary(
  raw: Record<string, unknown> | null | undefined,
): PriceControlSummary {
  const r = raw ?? {}
  return {
    evaluatedPairs: Number(r.evaluated_pairs) || 0,
    withinPolicy: Number(r.within_policy) || 0,
    withinPolicyPct: num(r.within_policy_pct),
    belowMinimum: Number(r.below_minimum) || 0,
    aboveMaximum: Number(r.above_maximum) || 0,
    missingRule: Number(r.missing_rule) || 0,
    missingCost: Number(r.missing_cost) || 0,
    missingPrice: Number(r.missing_price) || 0,
    staleCost: Number(r.stale_cost) || 0,
    costOutlier: Number(r.cost_outlier) || 0,
    conflictingCost: Number(r.conflicting_cost) || 0,
    needsReview: Number(r.needs_review) || 0,
    byStatus: (r.by_status as Record<string, number>) ?? {},
  }
}

export function buildComplianceByList(rows: PriceControlRow[]) {
  const clean = new Map<string, { name: string; within: number; total: number }>()
  for (const r of rows) {
    const key = String(r.priceListId)
    const cur = clean.get(key) ?? {
      name: r.priceListName || `Lista ${r.priceListId}`,
      within: 0,
      total: 0,
    }
    cur.total += 1
    if (r.status === "within_policy") cur.within += 1
    clean.set(key, cur)
  }
  return [...clean.values()]
    .map((x) => ({
      name: x.name,
      within: x.within,
      total: x.total,
      pct: x.total ? Math.round((x.within / x.total) * 1000) / 10 : 0,
    }))
    .sort((a, b) => b.total - a.total)
    .slice(0, 12)
}

export function buildComplianceByType(rows: PriceControlRow[]) {
  const clean = new Map<string, { name: string; within: number; total: number }>()
  for (const r of rows) {
    const name = r.productTypeName || "Sin tipo"
    const cur = clean.get(name) ?? { name, within: 0, total: 0 }
    cur.total += 1
    if (r.status === "within_policy") cur.within += 1
    clean.set(name, cur)
  }
  return [...clean.values()]
    .map((x) => ({
      name: x.name,
      within: x.within,
      total: x.total,
      pct: x.total ? Math.round((x.within / x.total) * 1000) / 10 : 0,
    }))
    .sort((a, b) => b.total - a.total)
    .slice(0, 12)
}

export function buildStatusDistribution(rows: PriceControlRow[]) {
  const counts = new Map<string, number>()
  for (const r of rows) {
    counts.set(r.status, (counts.get(r.status) ?? 0) + 1)
  }
  return [...counts.entries()]
    .map(([status, count]) => ({
      status,
      label: PRICE_POLICY_STATUS_LABEL[status as PricePolicyStatus] ?? status,
      count,
    }))
    .sort((a, b) => b.count - a.count)
}

/** Productos más alejados del mínimo (ajuste requerido > 0, peores primero). */
export function buildTopBelowMinimum(rows: PriceControlRow[], limit = 10) {
  return [...rows]
    .filter(
      (r) =>
        r.status === "below_minimum" ||
        (r.policyCompliance === "below_minimum" && r.priceAdjustmentToMinimum != null),
    )
    .filter((r) => (r.priceAdjustmentToMinimum ?? 0) > 0)
    .sort(
      (a, b) => (b.priceAdjustmentToMinimum ?? 0) - (a.priceAdjustmentToMinimum ?? 0),
    )
    .slice(0, limit)
    .map((r) => ({
      key: `${r.variantId}-${r.priceListId}`,
      productName: r.productName,
      variantName: r.variantName,
      priceListName: r.priceListName,
      adjustment: r.priceAdjustmentToMinimum ?? 0,
      markup: r.actualMarkupPct,
      minMarkup: r.minMarkupPct,
    }))
}

export function ageBucket(days: number | null): "0_30" | "31_60" | "61_90" | "90_plus" | "unknown" {
  if (days == null || !Number.isFinite(days)) return "unknown"
  if (days <= 30) return "0_30"
  if (days <= 60) return "31_60"
  if (days <= 90) return "61_90"
  return "90_plus"
}
