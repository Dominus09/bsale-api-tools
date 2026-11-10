/** Etiquetas en español para el chofer (sin códigos internos). */

export const PAYMENT_METHOD_LABEL: Record<string, string> = {
  cash: "Efectivo",
  transfer_declared: "Transferencia informada",
  transfer_verified: "Transferencia verificada",
  credit: "Crédito",
  partial: "Cobranza parcial",
  pending: "Saldo pendiente",
  not_applicable: "No corresponde",
}

export const DOC_STATUS_LABEL: Record<string, string> = {
  pending: "Pendiente",
  full: "Entrega completa",
  partial: "Entrega parcial",
  none: "No entregado",
  skipped: "Omitido",
}

export const LINE_OUTCOME_LABEL: Record<string, string> = {
  full: "Entrega completa",
  partial: "Entrega parcial",
  none: "No entregado",
  pending: "Pendiente",
}

export function paymentMethodLabel(method: string | null | undefined): string {
  if (!method) return "—"
  return PAYMENT_METHOD_LABEL[method] || method
}

export function docStatusLabel(status: string | null | undefined): string {
  if (!status) return "—"
  return DOC_STATUS_LABEL[status] || status
}

export function lineOutcomeEs(outcome: string | null | undefined): string {
  if (!outcome) return "—"
  return LINE_OUTCOME_LABEL[outcome] || outcome
}

/** Nombre comercial para UI: producto + variante, sin IDs técnicos. */
export function commercialProductLabel(p: {
  commercial_name?: string | null
  description?: string | null
  producto?: string | null
  variante?: string | null
  name_short?: string | null
  product_name?: string | null
}): string {
  if (p.commercial_name && String(p.commercial_name).trim()) {
    return String(p.commercial_name).trim()
  }
  const prod = String(p.producto || "").trim()
  const vari = String(p.variante || p.description || "").trim()
  if (prod && vari && !vari.toLowerCase().includes(prod.toLowerCase())) {
    return `${prod} — ${vari}`
  }
  return (
    prod ||
    vari ||
    String(p.description || "").trim() ||
    String(p.product_name || "").trim() ||
    String(p.name_short || "").trim() ||
    "Producto"
  )
}
