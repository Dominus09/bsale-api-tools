import type { CostV2QualityStatus, CostV2WarningCode } from "./types"

/** Mapeo único centralizado — valores técnicos internos → español operativo. */
export const COST_V2_STATUS_LABEL: Record<string, string> = {
  missing_taxes_in_gross: "Impuestos no incluidos",
  incomplete_tax_context: "Contexto tributario incompleto",
  missing_cost: "Costo faltante",
  valid_gross: "Costo correcto",
  duplicated_taxes_in_gross: "Posible impuesto duplicado",
  gross_component_mismatch: "Descuadre en el costo",
}

export const COST_V2_WARNING_LABEL: Record<string, string> = {
  suspicious_outlier: "Costo atípico",
  reception_tax_context_unavailable: "Sin información tributaria suficiente",
  stored_components_rounding: "Diferencia de redondeo",
}

/** Explicaciones breves (tooltip / panel). */
export const COST_V2_STATUS_SHORT_HELP: Record<string, string> = {
  missing_taxes_in_gross:
    "El bruto original no incluía todos los impuestos. Se calculó el valor corregido.",
  incomplete_tax_context:
    "No fue posible determinar todos los impuestos aplicables.",
  missing_cost: "La recepción no tiene costo suficiente para realizar el cálculo.",
  valid_gross: "El bruto almacenado coincide con el neto más los impuestos esperados.",
  duplicated_taxes_in_gross:
    "El bruto parece incluir uno o más impuestos aplicados más de una vez.",
  gross_component_mismatch:
    "El bruto no coincide con la suma esperada de neto, IVA e impuestos adicionales.",
}

export const COST_V2_WARNING_SHORT_HELP: Record<string, string> = {
  suspicious_outlier:
    "El costo se encuentra fuera del comportamiento habitual del mismo producto.",
  reception_tax_context_unavailable:
    "No fue posible obtener todo el contexto tributario necesario para la recepción.",
  stored_components_rounding:
    "Existe una diferencia menor que puede deberse al redondeo de los componentes.",
}

export type SymbologyStatusEntry = {
  code: string
  label: string
  description: string
  action: string
}

export type SymbologyAlertEntry = {
  code: string
  label: string
  description: string
}

export const SYMBOLOGY_INTRO =
  "Estas categorías indican la calidad del costo registrado y del cálculo tributario del módulo Control de costos."

export const SYMBOLOGY_SCOPE_NOTE =
  "Actualmente el módulo contiene información de Supermercado La Quillotana. Las demás oficinas se incorporarán progresivamente."

export const BUSINESS_SITUATION_LABEL: Record<string, string> = {
  requires_review: "Requiere revisión",
  office_difference: "Diferencia entre oficinas",
  partial_coverage: "Cobertura parcial",
  offices_aligned: "Oficinas alineadas",
  no_office_comparison: "Sin comparación entre oficinas",
}

export const SYMBOLOGY_BUSINESS_STATUSES: SymbologyStatusEntry[] = [
  {
    code: "partial_coverage",
    label: "Cobertura parcial",
    description:
      "No todas las oficinas activas tienen costos calculados disponibles para el producto.",
    action: "No interprete la falta de datos como una diferencia de costo.",
  },
  {
    code: "no_office_comparison",
    label: "Sin comparación entre oficinas",
    description:
      "Solo existe información suficiente de una oficina.",
    action: "Se necesita información de al menos dos oficinas para evaluar alineación.",
  },
  {
    code: "office_difference",
    label: "Diferencia entre oficinas",
    description:
      "Dos o más oficinas muestran costos vigentes distintos.",
    action: "Compare las recepciones y documentos de las oficinas involucradas.",
  },
  {
    code: "offices_aligned",
    label: "Oficinas alineadas",
    description:
      "Las oficinas con información disponible presentan el mismo costo vigente.",
    action: "No requiere acción por comparación entre oficinas.",
  },
  {
    code: "requires_review",
    label: "Requiere revisión",
    description:
      "El producto presenta una condición tributaria o de cobertura que conviene validar antes de usarlo como referencia.",
    action: "Revise el estado del costo y las alertas asociadas.",
  },
]

export const SYMBOLOGY_STATUSES: SymbologyStatusEntry[] = [
  {
    code: "missing_taxes_in_gross",
    label: COST_V2_STATUS_LABEL.missing_taxes_in_gross,
    description:
      "El costo bruto almacenado no incluía todos los impuestos correspondientes. Se calculó un bruto corregido agregando IVA e impuestos adicionales.",
    action: "Usar el costo corregido como referencia.",
  },
  {
    code: "incomplete_tax_context",
    label: COST_V2_STATUS_LABEL.incomplete_tax_context,
    description:
      "No existe información tributaria suficiente para calcular un costo bruto confiable.",
    action: "Revisar la configuración tributaria del producto.",
  },
  {
    code: "missing_cost",
    label: COST_V2_STATUS_LABEL.missing_cost,
    description:
      "La recepción no tiene un costo neto válido o el costo registrado es cero.",
    action: "Revisar el documento o la recepción original.",
  },
  {
    code: "valid_gross",
    label: COST_V2_STATUS_LABEL.valid_gross,
    description:
      "El costo bruto almacenado coincide con el neto más los impuestos esperados.",
    action: "No requiere revisión.",
  },
  {
    code: "duplicated_taxes_in_gross",
    label: COST_V2_STATUS_LABEL.duplicated_taxes_in_gross,
    description:
      "El costo bruto parece contener uno o más impuestos aplicados más de una vez.",
    action: "Revisar el detalle tributario de la recepción.",
  },
  {
    code: "gross_component_mismatch",
    label: COST_V2_STATUS_LABEL.gross_component_mismatch,
    description:
      "El costo bruto no coincide con la suma esperada de neto, IVA e impuestos adicionales.",
    action: "Revisar el cálculo y los componentes almacenados.",
  },
]

export const SYMBOLOGY_ALERTS: SymbologyAlertEntry[] = [
  {
    code: "suspicious_outlier",
    label: COST_V2_WARNING_LABEL.suspicious_outlier,
    description:
      "El costo se encuentra fuera del comportamiento habitual del mismo producto.",
  },
  {
    code: "reception_tax_context_unavailable",
    label: COST_V2_WARNING_LABEL.reception_tax_context_unavailable,
    description:
      "No fue posible obtener todo el contexto tributario necesario para la recepción.",
  },
  {
    code: "stored_components_rounding",
    label: COST_V2_WARNING_LABEL.stored_components_rounding,
    description:
      "Existe una diferencia menor que puede deberse al redondeo de los componentes.",
  },
]

/** Opciones de filtro Estado (value técnico, label español). */
export const FILTER_STATUS_OPTIONS = [
  { value: "missing_taxes_in_gross", label: COST_V2_STATUS_LABEL.missing_taxes_in_gross },
  { value: "incomplete_tax_context", label: COST_V2_STATUS_LABEL.incomplete_tax_context },
  { value: "missing_cost", label: COST_V2_STATUS_LABEL.missing_cost },
  { value: "valid_gross", label: COST_V2_STATUS_LABEL.valid_gross },
  { value: "duplicated_taxes_in_gross", label: COST_V2_STATUS_LABEL.duplicated_taxes_in_gross },
  { value: "gross_component_mismatch", label: COST_V2_STATUS_LABEL.gross_component_mismatch },
] as const

/** Opciones de filtro Alerta. */
export const FILTER_WARNING_OPTIONS = [
  { value: "suspicious_outlier", label: COST_V2_WARNING_LABEL.suspicious_outlier },
  {
    value: "reception_tax_context_unavailable",
    label: COST_V2_WARNING_LABEL.reception_tax_context_unavailable,
  },
  {
    value: "stored_components_rounding",
    label: COST_V2_WARNING_LABEL.stored_components_rounding,
  },
] as const

/** Códigos técnicos que no deben aparecer en UI principal. */
export const TECHNICAL_STATUS_CODES = Object.keys(COST_V2_STATUS_LABEL)
export const TECHNICAL_WARNING_CODES = Object.keys(COST_V2_WARNING_LABEL)

export function statusLabel(status: CostV2QualityStatus | null | undefined): string {
  if (!status) return "Sin estado"
  return COST_V2_STATUS_LABEL[status] ?? "Estado desconocido"
}

export function warningLabel(code: CostV2WarningCode | null | undefined): string {
  if (!code) return ""
  return COST_V2_WARNING_LABEL[code] ?? "Alerta"
}

export function statusShortHelp(status: CostV2QualityStatus | null | undefined): string {
  if (!status) return "Sin información de estado."
  return COST_V2_STATUS_SHORT_HELP[status] ?? "Consulte la pestaña Simbología."
}

export function statusDrawerDescription(
  status: CostV2QualityStatus | null | undefined,
): string {
  if (!status) return "Sin información de estado."
  const entry = SYMBOLOGY_STATUSES.find((s) => s.code === status)
  return entry?.description ?? statusShortHelp(status)
}

export function statusSuggestedAction(
  status: CostV2QualityStatus | null | undefined,
): string {
  if (!status) return "Consulte la pestaña Simbología."
  const entry = SYMBOLOGY_STATUSES.find((s) => s.code === status)
  return entry?.action ?? "Consulte la pestaña Simbología."
}

export function warningShortHelp(code: CostV2WarningCode | null | undefined): string {
  if (!code) return ""
  return COST_V2_WARNING_SHORT_HELP[code] ?? "Consulte la pestaña Simbología."
}

/** Nota de alcance operativa (sin IDs técnicos). */
export const COST_V2_SCOPE_NOTE_DRAWER =
  "Datos disponibles actualmente para La Quillotana SpA, Supermercado La Quillotana."

/** True si el texto contiene un código técnico expuesto (para tests / guardas). */
export function containsTechnicalCode(text: string): boolean {
  const lower = text.toLowerCase()
  for (const code of [...TECHNICAL_STATUS_CODES, ...TECHNICAL_WARNING_CODES]) {
    if (lower.includes(code.toLowerCase())) return true
  }
  return false
}

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

export const FORBIDDEN_AGGREGATE_PHRASES = [
  "impacto total",
  "pérdida total",
  "perdida total",
  "sobrecosto total",
  "costo total comprado",
] as const
