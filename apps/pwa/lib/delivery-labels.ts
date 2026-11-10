/** Etiquetas en español para el chofer (sin códigos internos). */

export const PAYMENT_METHOD_LABEL: Record<string, string> = {
  cash: "Efectivo",
  transfer_declared: "Transferencia informada",
  cash_and_transfer: "Efectivo + transferencia",
  transfer_verified: "Transferencia verificada",
  credit: "Crédito",
  partial: "Cobranza parcial",
  pending: "Pago pendiente",
  not_applicable: "Sin cobro / No aplica",
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

export type ProductLabelInput = {
  commercial_name?: string | null
  category_name?: string | null
  product_name?: string | null
  variant_name?: string | null
  description?: string | null
  producto?: string | null
  variante?: string | null
  name_short?: string | null
  codigo_barras?: string | null
  barcode?: string | null
}

/** Título destacado: producto + formato/variante (datos verificados). */
export function commercialProductLabel(p: ProductLabelInput): string {
  const product = String(p.product_name || p.producto || "").trim()
  const variant = String(
    p.variant_name || p.variante || p.description || "",
  ).trim()
  if (product && variant) {
    if (variant.toLowerCase().includes(product.toLowerCase())) return variant
    return `${product} — ${variant}`
  }
  if (p.commercial_name && String(p.commercial_name).trim()) {
    return String(p.commercial_name).trim()
  }
  return (
    product ||
    variant ||
    String(p.description || "").trim() ||
    String(p.name_short || "").trim() ||
    "Producto"
  )
}

export function productCategoryLabel(p: ProductLabelInput): string | null {
  const c = String(p.category_name || "").trim()
  return c || null
}

export function productBarcodeLabel(p: ProductLabelInput): string | null {
  const b = String(p.barcode || p.codigo_barras || "").trim()
  return b || null
}
