/**
 * Control de precios por lista (/margins) — semántica fijada.
 *
 * min_margin / max_margin en bsale.margin_rules = % SOBRE COSTO
 * (recargo / markup), confirmado en:
 * - backend/sql/margin_rules_extend.sql COMMENT
 * - backend/sql/margin_analysis_view.sql (% sobre costo)
 * - suggestedPrice = cost * (1 + min/100) en UI actual
 *
 * NO es margen realizado de ventas. NO usa documentos ni unidades vendidas.
 */

export type PricePolicyStatus =
  | "below_minimum"
  | "within_policy"
  | "above_maximum"
  | "missing_rule"
  | "missing_cost"
  | "missing_price"
  | "stale_cost"
  | "conflicting_cost"
  | "cost_outlier"

export const PRICE_POLICY_STATUS_LABEL: Record<PricePolicyStatus, string> = {
  below_minimum: "Bajo el mínimo",
  within_policy: "Dentro de política",
  above_maximum: "Sobre el máximo",
  missing_rule: "Sin regla",
  missing_cost: "Sin costo",
  missing_price: "Sin precio",
  stale_cost: "Costo desactualizado",
  conflicting_cost: "Costo en conflicto",
  cost_outlier: "Revisar costo",
}

/** Recargo real % = (precio − costo) / costo × 100 */
export function actualMarkupPct(
  grossPrice: number | null | undefined,
  grossCost: number | null | undefined,
): number | null {
  if (grossPrice == null || grossCost == null) return null
  if (!Number.isFinite(grossPrice) || !Number.isFinite(grossCost) || grossCost <= 0) {
    return null
  }
  return ((grossPrice - grossCost) / grossCost) * 100
}

/** Margen sobre precio (informativo) % = (precio − costo) / precio × 100 */
export function grossMarginPct(
  grossPrice: number | null | undefined,
  grossCost: number | null | undefined,
): number | null {
  if (grossPrice == null || grossCost == null) return null
  if (!Number.isFinite(grossPrice) || !Number.isFinite(grossCost) || grossPrice <= 0) {
    return null
  }
  return ((grossPrice - grossCost) / grossPrice) * 100
}

/** Precio bruto recomendado desde recargo % sobre costo. */
export function recommendedGrossPrice(
  grossCost: number | null | undefined,
  markupPct: number | null | undefined,
): number | null {
  if (grossCost == null || markupPct == null) return null
  if (!Number.isFinite(grossCost) || !Number.isFinite(markupPct) || grossCost <= 0) {
    return null
  }
  return Math.round(grossCost * (1 + markupPct / 100))
}

/**
 * Equivalencia informativa: un recargo m% sobre costo equivale a
 * margen sobre precio = m / (100 + m) * 100.
 */
export function markupToMarginOnPricePct(markupPct: number): number | null {
  if (!Number.isFinite(markupPct) || markupPct <= -100) return null
  return (markupPct / (100 + markupPct)) * 100
}

export function policyComplianceStatus(input: {
  grossPrice: number | null
  grossCost: number | null
  minMarkupPct: number | null
  maxMarkupPct: number | null
  hasRule: boolean
}): PricePolicyStatus | null {
  if (!input.hasRule || input.minMarkupPct == null) return "missing_rule"
  const markup = actualMarkupPct(input.grossPrice, input.grossCost)
  if (markup == null) return null
  if (markup < input.minMarkupPct) return "below_minimum"
  if (
    input.maxMarkupPct != null &&
    input.maxMarkupPct > 0 &&
    markup > input.maxMarkupPct
  ) {
    return "above_maximum"
  }
  return "within_policy"
}

/**
 * Prioridad UI:
 * 1 sin precio → 2 sin costo → 3 conflicto → 4 outlier → 5 sin regla
 * → 6 stale → 7 below/within/above
 */
export function resolvePricePolicyStatus(input: {
  grossPrice: number | null
  grossCost: number | null
  minMarkupPct: number | null
  maxMarkupPct: number | null
  hasRule: boolean
  isStale?: boolean
  isConflicting?: boolean
  isOutlier?: boolean
}): PricePolicyStatus {
  const priceOk = input.grossPrice != null && input.grossPrice > 0
  const costOk = input.grossCost != null && input.grossCost > 0
  if (!priceOk) return "missing_price"
  if (!costOk) return "missing_cost"
  if (input.isConflicting) return "conflicting_cost"

  const compliance = policyComplianceStatus(input)
  if (input.isOutlier) return "cost_outlier"
  if (compliance === "missing_rule") return "missing_rule"
  if (input.isStale) return "stale_cost"
  return compliance ?? "missing_price"
}

export function statusExplanation(status: PricePolicyStatus): string {
  switch (status) {
    case "below_minimum":
      return "El recargo real está bajo el recargo mínimo objetivo de la política."
    case "within_policy":
      return "El precio bruto actual cumple el rango de recargo definido."
    case "above_maximum":
      return "El recargo real supera el recargo máximo objetivo de la política."
    case "missing_rule":
      return "No hay regla activa de margen/recargo para este tipo y lista."
    case "missing_cost":
      return "No hay costo bruto de referencia válido para evaluar el precio."
    case "missing_price":
      return "No hay precio bruto actual en esta lista."
    case "stale_cost":
      return "El costo de referencia está desactualizado; revise el historial en Costos."
    case "conflicting_cost":
      return "Hay indicios de conflicto en el costo bruto; revise en Costos."
    case "cost_outlier":
      return "El costo máximo válido parece atípico frente al historial; revise en Costos."
  }
}
