/**
 * Formato de presentación para montos V2 (strings Decimal).
 * No convierte a float para cálculos; solo visualización.
 */

/** Formatea un string decimal a moneda CLP sin alterar el valor original. */
export function formatDecimalMoneyCLP(value: string | null | undefined): string {
  if (value == null || value === "") return "—"
  const trimmed = String(value).trim()
  if (!/^-?\d+(\.\d+)?$/.test(trimmed)) return trimmed

  const negative = trimmed.startsWith("-")
  const abs = negative ? trimmed.slice(1) : trimmed
  const [intRaw, fracRaw] = abs.split(".")
  const intFormatted = intRaw.replace(/\B(?=(\d{3})+(?!\d))/g, ".")
  const sign = negative ? "-" : ""
  const fracSignificant = fracRaw?.replace(/0+$/, "") ?? ""
  if (fracSignificant) {
    return `${sign}$${intFormatted},${fracRaw}`
  }
  return `${sign}$${intFormatted}`
}

/** Fecha ISO → dd-mm-yyyy (Chile). */
export function formatDateCL(value: string | null | undefined): string {
  if (!value) return "—"
  const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(value)
  if (m) return `${m[3]}-${m[2]}-${m[1]}`
  const d = new Date(value)
  if (Number.isNaN(d.getTime())) return "—"
  const dd = String(d.getUTCDate()).padStart(2, "0")
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0")
  const yyyy = d.getUTCFullYear()
  return `${dd}-${mm}-${yyyy}`
}

export function formatDateTimeCL(value: string | null | undefined): string {
  if (!value) return "—"
  const d = new Date(value)
  if (Number.isNaN(d.getTime())) return value
  return d.toLocaleString("es-CL", { dateStyle: "short", timeStyle: "short" })
}

export function formatTaxRate(value: string | null | undefined): string {
  if (value == null || value === "") return "—"
  return `${value}%`
}

/** Columna bruto corregido: null → "No calculable" (nunca $0 inventado). */
export function displayCorrectedGross(value: string | null | undefined): string {
  if (value == null || value === "") return "No calculable"
  return formatDecimalMoneyCLP(value)
}

/**
 * Diferencia unitaria: si no hay bruto almacenado → "—".
 * Usa el string que entrega la API (no recalcula).
 */
export function displayUnitDifference(params: {
  stored_cost_gross: string | null | undefined
  unit_difference: string | null | undefined
}): string {
  if (params.stored_cost_gross == null || params.stored_cost_gross === "") {
    return "—"
  }
  if (params.unit_difference == null || params.unit_difference === "") {
    return "—"
  }
  return formatDecimalMoneyCLP(params.unit_difference)
}

export function explanationForStatus(status: string | null | undefined): string | null {
  switch (status) {
    case "missing_cost":
      return "La recepción no tiene un costo neto válido."
    case "incomplete_tax_context":
      return "No existe contexto tributario suficiente para calcular el bruto."
    case "missing_taxes_in_gross":
      return "El cálculo V2 incorporó los impuestos que faltaban en el bruto almacenado."
    default:
      return null
  }
}

/** Etiqueta humana para categoría de impuesto adicional (no “IVA adicional”). */
export function additionalTaxCategoryLabel(category: string | null | undefined): string {
  const c = (category || "").toLowerCase()
  if (c === "iva_advance" || c === "anticipo" || c.includes("advance")) {
    return "Anticipo tributario"
  }
  if (c === "ila" || c.includes("ila")) return "ILA / impuesto específico"
  return "Impuesto adicional"
}
