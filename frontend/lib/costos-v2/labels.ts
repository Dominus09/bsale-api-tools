import type { CostV2QualityStatus, CostV2WarningCode } from "./types"

export const COST_V2_STATUS_LABEL: Record<string, string> = {
  missing_taxes_in_gross: "Impuestos no incluidos",
  incomplete_tax_context: "Contexto tributario incompleto",
  missing_cost: "Costo faltante",
  valid_gross: "Costo válido",
  gross_component_mismatch: "Componentes no coinciden",
  duplicated_taxes_in_gross: "Posible impuesto duplicado",
}

export const COST_V2_WARNING_LABEL: Record<string, string> = {
  suspicious_outlier: "Costo atípico",
  reception_tax_context_unavailable: "Sin contexto tributario",
  stored_components_rounding: "Diferencia de redondeo",
  tax_ids_not_consumed: "Tax IDs no consumidos",
  variant_barcode_mismatch: "Barcode no coincide",
  source_conflict: "Conflicto de fuentes",
}

export function statusLabel(status: CostV2QualityStatus | null | undefined): string {
  if (!status) return "Sin estado"
  return COST_V2_STATUS_LABEL[status] ?? status
}

export function warningLabel(code: CostV2WarningCode | null | undefined): string {
  if (!code) return ""
  return COST_V2_WARNING_LABEL[code] ?? code
}

/** Orden preferido para gráfico de distribución. */
export const STATUS_CHART_ORDER = [
  "missing_taxes_in_gross",
  "incomplete_tax_context",
  "missing_cost",
  "valid_gross",
  "gross_component_mismatch",
  "duplicated_taxes_in_gross",
] as const

export function buildStatusChartData(
  byStatus: Record<string, number> | null | undefined,
): { status: string; label: string; count: number }[] {
  const map = byStatus ?? {}
  const seen = new Set<string>()
  const out: { status: string; label: string; count: number }[] = []
  for (const key of STATUS_CHART_ORDER) {
    seen.add(key)
    out.push({ status: key, label: statusLabel(key), count: Number(map[key] ?? 0) })
  }
  for (const [key, count] of Object.entries(map)) {
    if (seen.has(key)) continue
    out.push({ status: key, label: statusLabel(key), count: Number(count) || 0 })
  }
  return out
}

/** Textos prohibidos en UI V2 (no hay semántica de impacto agregado). */
export const FORBIDDEN_AGGREGATE_PHRASES = [
  "impacto total",
  "pérdida total",
  "perdida total",
  "sobrecosto total",
  "costo total comprado",
] as const
