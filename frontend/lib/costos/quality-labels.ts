/** Etiquetas legibles — no exponer códigos técnicos al usuario. */

export type GrossCostQualityKind =
  | "actual_purchase_gross"
  | "reconstructed_from_actual_taxes"
  | "current_cost_fallback"
  | "missing_gross_cost"
  | "conflicting_gross_cost"

export type TaxBreakdownKind =
  | "exact_iva_ila_split"
  | "aggregated_other_taxes"
  | "reconstructed_from_rates"
  | "partial_breakdown"
  | "missing_breakdown"

export type AgeBucketKind = "0_30" | "31_60" | "61_90" | "90_plus" | "unknown"

export const GROSS_COST_QUALITY_LABEL: Record<GrossCostQualityKind, string> = {
  actual_purchase_gross: "Bruto real de compra",
  reconstructed_from_actual_taxes: "Reconstruido",
  current_cost_fallback: "Costo actual (fallback)",
  missing_gross_cost: "Sin costo",
  conflicting_gross_cost: "Conflicto",
}

export const TAX_BREAKDOWN_LABEL: Record<TaxBreakdownKind, string> = {
  exact_iva_ila_split: "Desglose completo",
  aggregated_other_taxes: "Otros impuestos agrupados",
  reconstructed_from_rates: "Reconstruido por tasas",
  partial_breakdown: "Desglose incompleto",
  missing_breakdown: "Sin desglose",
}

export const AGE_BUCKET_LABEL: Record<AgeBucketKind, string> = {
  "0_30": "Actualizado",
  "31_60": "31–60 días",
  "61_90": "61–90 días",
  "90_plus": "Desactualizado",
  unknown: "Sin información",
}

export const COST_ORIGIN_LABEL = {
  reception_history: "Recepción / compra",
  variant_cost: "Costo actual Bsale",
  unknown: "Sin información",
} as const
