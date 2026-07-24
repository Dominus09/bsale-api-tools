/**
 * Adaptador frontend: respuestas /cost-analytics/* → modelo visual de Costos.
 * No inventa montos; infiere calidad solo desde presencia de campos.
 */

import type {
  CostAnalyticsDashboard,
  CostHistoryRow,
  CostProductRow,
  CostVariantHistory,
} from "../api"
import { daysBetween } from "./format"
import type {
  AgeBucketKind,
  GrossCostQualityKind,
  TaxBreakdownKind,
} from "./quality-labels"

export type CostTableRow = {
  id: string
  variantId: number
  productName: string | null
  variantName: string | null
  barcode: string | null
  officeName: string | null
  lastReceptionDate: string | null
  costNet: number | null
  ivaAmount: number | null
  /** ILA u otros impuestos agregados (no afirmar ILA). */
  otherTaxes: number | null
  costGross: number | null
  /** Último bruto (misma fila que costGross cuando es latest). */
  lastGrossCost: number | null
  previousCostGross: number | null
  /** Máximo bruto válido en historial cargado (excl. NC/ajuste/devolución). */
  maxValidGrossCost: number | null
  /** Mínimo bruto válido en historial cargado. */
  minValidGrossCost: number | null
  variationAmount: number | null
  variationPct: number | null
  averageCost: number | null
  currentGrossFallback: number | null
  quantity: number | null
  receptionId: number | null
  documentLabel: string | null
  origin: "reception_history" | "variant_cost" | "unknown"
  ageDays: number | null
  ageBucket: AgeBucketKind
  grossCostQuality: GrossCostQualityKind
  taxBreakdownQuality: TaxBreakdownKind
  isOutlier: boolean
  isStale: boolean
  supplierName: string | null // API aún no entrega → null
  productTypeName: string | null // API aún no entrega → null
}

export type CostKpiModel = {
  variantsAnalyzed: number | null
  grossCoveragePct: number | null
  withoutCost: number | null
  updatedLast30d: number | null
  olderThan90d: number | null
  avgVariationPct: number | null
  productsWithMajorIncrease: number | null
  lastSyncAt: string | null
  lastSyncStatus: string | null
  /** Deltas vs período previo — no disponibles en API actual */
  deltas: {
    variantsAnalyzed: number | null
    grossCoveragePct: number | null
    withoutCost: number | null
  }
}

export type CostChartPoint = { date: string; costGross: number | null; costNet: number | null }

export type AgeDistribution = { bucket: AgeBucketKind; count: number; label: string }

export type TopIncreaseItem = {
  variantId: number
  label: string
  variationPct: number
  costGross: number | null
}

function num(v: unknown): number | null {
  if (v == null || v === "") return null
  const n = Number(v)
  return Number.isFinite(n) ? n : null
}

export function inferGrossCostQuality(row: {
  cost_bruto_erp?: number | null
  iva_amount?: number | null
  other_taxes?: number | null
  cost_net?: number | null
  average_cost?: number | null
  current_cost_gross?: number | null
}): GrossCostQualityKind {
  const bruto = num(row.cost_bruto_erp)
  if (bruto != null && bruto > 0) return "actual_purchase_gross"
  const iva = num(row.iva_amount)
  const other = num(row.other_taxes)
  const net = num(row.cost_net)
  if (net != null && net > 0 && iva != null && other != null) {
    return "reconstructed_from_actual_taxes"
  }
  const fallback = num(row.current_cost_gross) ?? num(row.average_cost)
  if (fallback != null && fallback > 0) return "current_cost_fallback"
  return "missing_gross_cost"
}

export function inferTaxBreakdownQuality(row: {
  iva_amount?: number | null
  other_taxes?: number | null
  cost_bruto_erp?: number | null
}): TaxBreakdownKind {
  const iva = num(row.iva_amount)
  const other = num(row.other_taxes)
  const bruto = num(row.cost_bruto_erp)
  if (iva != null && other != null) return "aggregated_other_taxes"
  if (iva != null && bruto != null) return "partial_breakdown"
  if (bruto != null && bruto > 0) return "partial_breakdown"
  return "missing_breakdown"
}

export function ageBucketFromDate(
  dateStr: string | null | undefined,
  now = new Date(),
): { days: number | null; bucket: AgeBucketKind } {
  if (!dateStr) return { days: null, bucket: "unknown" }
  const d = new Date(dateStr)
  if (Number.isNaN(d.getTime())) return { days: null, bucket: "unknown" }
  const days = daysBetween(d, now)
  if (days <= 30) return { days, bucket: "0_30" }
  if (days <= 60) return { days, bucket: "31_60" }
  if (days <= 90) return { days, bucket: "61_90" }
  return { days, bucket: "90_plus" }
}

/** Estima costo bruto anterior si hay variación % y bruto actual (no inventa si falta %). */
export function previousGrossFromVariation(
  costGross: number | null,
  variationPct: number | null,
): { previous: number | null; delta: number | null } {
  if (costGross == null || variationPct == null) return { previous: null, delta: null }
  const factor = 1 + variationPct / 100
  if (factor === 0) return { previous: null, delta: null }
  const previous = costGross / factor
  return { previous, delta: costGross - previous }
}

export function adaptHistoryRowToTable(row: CostHistoryRow, now = new Date()): CostTableRow {
  const costGross = num(row.cost_bruto_erp)
  const variationPct = num(row.variation_pct)
  const { previous, delta } = previousGrossFromVariation(costGross, variationPct)
  const age = ageBucketFromDate(row.admission_date, now)
  const doc =
    row.document_number != null
      ? `${row.document ?? "Doc"} ${row.document_number}`
      : row.document ?? null

  return {
    id: `${row.reception_detail_id}-${row.variant_id}`,
    variantId: row.variant_id,
    productName: row.product_name ?? null,
    variantName: row.variant_name ?? null,
    barcode: row.barcode ?? null,
    officeName: row.office_name ?? null,
    lastReceptionDate: row.admission_date ?? null,
    costNet: num(row.cost_net),
    ivaAmount: num(row.iva_amount),
    otherTaxes: num(row.other_taxes),
    costGross,
    lastGrossCost: costGross,
    previousCostGross: previous,
    maxValidGrossCost: costGross,
    minValidGrossCost: costGross,
    variationAmount: delta,
    variationPct,
    averageCost: num(row.average_cost),
    currentGrossFallback: null,
    quantity: num(row.quantity),
    receptionId: row.reception_id ?? null,
    documentLabel: doc,
    origin: "reception_history",
    ageDays: age.days,
    ageBucket: age.bucket,
    grossCostQuality: inferGrossCostQuality(row),
    taxBreakdownQuality: inferTaxBreakdownQuality(row),
    isOutlier: false,
    isStale: age.bucket === "90_plus",
    supplierName: null,
    productTypeName: null,
  }
}

const INVALID_RECEPTION = new Set([
  "recepcion_ajuste",
  "recepcion_devolucion",
  "recepcion_nc",
])

function effectiveGrossFromHistory(row: CostHistoryRow): number | null {
  const bruto = num(row.cost_bruto_erp)
  if (bruto != null && bruto > 0) return bruto
  const net = num(row.cost_net)
  const iva = num(row.iva_amount)
  const other = num(row.other_taxes)
  if (net != null && net > 0 && iva != null && other != null) return net + iva + other
  return null
}

/**
 * Auditoría por variante: último, anterior, min/máx válidos, outlier/stale.
 * No inventa costos; excluye NC/ajuste/devolución del max/min válido.
 */
export function buildVariantAuditRows(
  rows: CostHistoryRow[],
  now = new Date(),
): CostTableRow[] {
  const byVariant = new Map<number, CostHistoryRow[]>()
  for (const r of rows) {
    const list = byVariant.get(r.variant_id) ?? []
    list.push(r)
    byVariant.set(r.variant_id, list)
  }

  const out: CostTableRow[] = []
  for (const [, list] of byVariant) {
    const sorted = [...list].sort(
      (a, b) =>
        new Date(a.admission_date).getTime() - new Date(b.admission_date).getTime(),
    )
    const latest = sorted[sorted.length - 1]
    const previousRow = sorted.length >= 2 ? sorted[sorted.length - 2] : null
    const base = adaptHistoryRowToTable(latest, now)

    const validGrosses: number[] = []
    for (const r of sorted) {
      const t = (r.reception_type || "").toLowerCase()
      if (INVALID_RECEPTION.has(t)) continue
      const g = effectiveGrossFromHistory(r)
      if (g != null && g > 0) validGrosses.push(g)
    }
    const minG = validGrosses.length ? Math.min(...validGrosses) : null
    const maxG = validGrosses.length ? Math.max(...validGrosses) : null
    const lastG = effectiveGrossFromHistory(latest)
    const prevG = previousRow ? effectiveGrossFromHistory(previousRow) : null

    let isOutlier = false
    if (validGrosses.length >= 3 && maxG != null) {
      const mid = [...validGrosses].sort((a, b) => a - b)
      const med = mid[Math.floor(mid.length / 2)]!
      if (med > 0 && maxG > med * 3) isOutlier = true
    }

    const varAmt =
      lastG != null && prevG != null ? lastG - prevG : base.variationAmount
    const varPct =
      lastG != null && prevG != null && prevG > 0
        ? ((lastG - prevG) / prevG) * 100
        : base.variationPct

    out.push({
      ...base,
      costGross: lastG ?? base.costGross,
      lastGrossCost: lastG ?? base.costGross,
      previousCostGross: prevG,
      maxValidGrossCost: maxG,
      minValidGrossCost: minG,
      variationAmount: varAmt,
      variationPct: varPct,
      isOutlier,
      isStale: base.ageBucket === "90_plus",
    })
  }
  return out
}

/** @deprecated Prefer buildVariantAuditRows for auditoría completa. */
export function latestPerVariant(rows: CostHistoryRow[], now = new Date()): CostTableRow[] {
  return buildVariantAuditRows(rows, now)
}

export function adaptProductFallback(row: CostProductRow, now = new Date()): CostTableRow {
  const costGross = num(row.current_cost_gross) ?? num(row.average_cost_gross)
  const variationPct = num(row.variation_pct)
  const { previous, delta } = previousGrossFromVariation(costGross, variationPct)
  const age = ageBucketFromDate(row.last_reception_date, now)
  return {
    id: `product-${row.variant_id}`,
    variantId: row.variant_id,
    productName: row.product_name ?? null,
    variantName: row.variant_name ?? null,
    barcode: row.barcode ?? null,
    officeName: row.last_office_name ?? null,
    lastReceptionDate: row.last_reception_date ?? null,
    costNet: num(row.current_cost),
    ivaAmount: null,
    otherTaxes: null,
    costGross,
    lastGrossCost: costGross,
    previousCostGross: previous,
    maxValidGrossCost: costGross,
    minValidGrossCost: costGross,
    variationAmount: delta,
    variationPct,
    averageCost: num(row.average_cost),
    currentGrossFallback: num(row.average_cost_gross),
    quantity: null,
    receptionId: null,
    documentLabel: null,
    origin: row.last_reception_date ? "reception_history" : "variant_cost",
    ageDays: age.days,
    ageBucket: age.bucket,
    grossCostQuality: inferGrossCostQuality({
      cost_bruto_erp: row.current_cost_gross,
      cost_net: row.current_cost,
      average_cost: row.average_cost,
      current_cost_gross: row.average_cost_gross,
    }),
    taxBreakdownQuality: "missing_breakdown",
    isOutlier: false,
    isStale: age.bucket === "90_plus",
    supplierName: null,
    productTypeName: null,
  }
}

export function adaptDashboardKpis(
  dash: CostAnalyticsDashboard | null,
  tableRows: CostTableRow[],
): CostKpiModel {
  const k = dash?.kpis
  const withGross = tableRows.filter(
    (r) => r.costGross != null && r.costGross > 0,
  ).length
  const total = tableRows.length || k?.variants_total || null
  const grossCoveragePct =
    total && total > 0 ? (withGross / (tableRows.length || total)) * 100 : null
  const older90 = tableRows.filter((r) => r.ageBucket === "90_plus").length
  const vars = tableRows
    .map((r) => r.variationPct)
    .filter((v): v is number => v != null)
  const avgVar =
    vars.length > 0 ? vars.reduce((a, b) => a + b, 0) / vars.length : null

  return {
    variantsAnalyzed: k?.products_monitored ?? k?.variants_total ?? tableRows.length,
    grossCoveragePct:
      grossCoveragePct ??
      (k?.variants_total && k.variants_total > 0
        ? ((k.with_cost ?? 0) / k.variants_total) * 100
        : null),
    withoutCost: k?.without_cost ?? null,
    updatedLast30d: k?.receptions_30d ?? null,
    olderThan90d: older90 || null,
    avgVariationPct: avgVar,
    productsWithMajorIncrease: k?.products_cost_up ?? null,
    lastSyncAt: dash?.last_sync?.last_run_at ?? null,
    lastSyncStatus: dash?.last_sync?.last_status ?? null,
    deltas: {
      variantsAnalyzed: null,
      grossCoveragePct: null,
      withoutCost: null,
    },
  }
}

export function buildAgeDistribution(rows: CostTableRow[]): AgeDistribution[] {
  const labels: Record<AgeBucketKind, string> = {
    "0_30": "0–30 días",
    "31_60": "31–60 días",
    "61_90": "61–90 días",
    "90_plus": "+90 días",
    unknown: "Sin fecha",
  }
  const counts: Record<AgeBucketKind, number> = {
    "0_30": 0,
    "31_60": 0,
    "61_90": 0,
    "90_plus": 0,
    unknown: 0,
  }
  for (const r of rows) counts[r.ageBucket] += 1
  return (Object.keys(counts) as AgeBucketKind[])
    .filter((k) => k !== "unknown" || counts[k] > 0)
    .map((bucket) => ({ bucket, count: counts[bucket], label: labels[bucket] }))
}

export function buildTopIncreases(rows: CostTableRow[], limit = 8): TopIncreaseItem[] {
  return [...rows]
    .filter((r) => r.variationPct != null && r.variationPct > 0)
    .sort((a, b) => (b.variationPct ?? 0) - (a.variationPct ?? 0))
    .slice(0, limit)
    .map((r) => ({
      variantId: r.variantId,
      label: r.productName || r.variantName || `Variante ${r.variantId}`,
      variationPct: r.variationPct!,
      costGross: r.costGross,
    }))
}

export function buildEvolutionSeries(history: CostVariantHistory | null): CostChartPoint[] {
  if (!history?.chart_series?.length && !history?.items?.length) return []
  if (history.chart_series?.length) {
    return history.chart_series.map((p) => ({
      date: (p.date ?? "").slice(0, 10),
      costGross: num(p.cost_bruto_erp),
      costNet: num(p.cost_net),
    }))
  }
  return [...history.items]
    .sort(
      (a, b) =>
        new Date(a.admission_date).getTime() - new Date(b.admission_date).getTime(),
    )
    .map((r) => ({
      date: r.admission_date.slice(0, 10),
      costGross: num(r.cost_bruto_erp),
      costNet: num(r.cost_net),
    }))
}

export function aggregateGrossEvolution(rows: CostHistoryRow[]): CostChartPoint[] {
  const byDay = new Map<string, { gross: number; net: number; n: number }>()
  for (const r of rows) {
    const day = (r.admission_date || "").slice(0, 10)
    if (!day) continue
    const g = num(r.cost_bruto_erp)
    const n = num(r.cost_net) ?? 0
    if (g == null) continue
    const prev = byDay.get(day) ?? { gross: 0, net: 0, n: 0 }
    prev.gross += g
    prev.net += n
    prev.n += 1
    byDay.set(day, prev)
  }
  return Array.from(byDay.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([date, v]) => ({
      date,
      costGross: v.n ? v.gross / v.n : null,
      costNet: v.n ? v.net / v.n : null,
    }))
}

/**
 * Campos aún no disponibles en /cost-analytics (mostrar "Sin información"):
 * - supplier_id / proveedor
 * - product_type / categoría
 * - deltas KPI vs período anterior
 * - cobertura por categoría (sin taxonomía en API)
 * - ila_amount separado de other_taxes
 * - gross_cost_quality / tax_breakdown_quality oficiales del backend
 */
export const MISSING_COST_API_FIELDS = [
  "supplier_name",
  "product_type_name",
  "kpi_period_deltas",
  "coverage_by_category",
  "ila_amount",
  "server_gross_cost_quality",
] as const
