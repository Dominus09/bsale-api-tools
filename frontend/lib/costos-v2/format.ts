/**
 * Formato de presentación Costos V2.
 * Conserva strings Decimal originales; solo formatea para UI.
 */

/** Tabla/KPI: pesos chilenos sin decimales ($3.478). */
export function formatMoneyCLPTable(value: string | null | undefined): string {
  if (value == null || value === "") return "—"
  const trimmed = String(value).trim()
  if (!/^-?\d+(\.\d+)?$/.test(trimmed)) return trimmed
  const negative = trimmed.startsWith("-")
  const abs = negative ? trimmed.slice(1) : trimmed
  const [intRaw, fracRaw = ""] = abs.split(".")
  // Redondeo visual hacia entero más cercano sin Number() flotante de todo el monto:
  // usa parte entera + primer decimal.
  let intPart = intRaw
  const firstDec = fracRaw.charAt(0)
  if (firstDec && Number(firstDec) >= 5) {
    // incrementar string entero
    const digits = intPart.split("").map(Number)
    let i = digits.length - 1
    let carry = 1
    while (i >= 0 && carry) {
      const n = digits[i] + carry
      digits[i] = n % 10
      carry = Math.floor(n / 10)
      i -= 1
    }
    intPart = (carry ? String(carry) : "") + digits.join("")
  }
  const intFormatted = intPart.replace(/\B(?=(\d{3})+(?!\d))/g, ".")
  return `${negative ? "-" : ""}$${intFormatted}`
}

/** Detalle técnico: hasta 4 decimales originales. */
export function formatMoneyCLPPrecise(value: string | null | undefined): string {
  if (value == null || value === "") return "—"
  const trimmed = String(value).trim()
  if (!/^-?\d+(\.\d+)?$/.test(trimmed)) return trimmed
  const negative = trimmed.startsWith("-")
  const abs = negative ? trimmed.slice(1) : trimmed
  const [intRaw, fracRaw] = abs.split(".")
  const intFormatted = intRaw.replace(/\B(?=(\d{3})+(?!\d))/g, ".")
  const sign = negative ? "-" : ""
  if (fracRaw) {
    const frac = fracRaw.slice(0, 4).replace(/0+$/, "")
    if (frac) return `${sign}$${intFormatted},${frac}`
  }
  return `${sign}$${intFormatted}`
}

/** Alias legacy de visualización general → tabla. */
export function formatDecimalMoneyCLP(value: string | null | undefined): string {
  return formatMoneyCLPTable(value)
}

/** Porcentaje UI: máximo 1 decimal (39,5 %). */
export function formatPercentCL(value: string | null | undefined): string {
  if (value == null || value === "") return "—"
  const trimmed = String(value).trim()
  if (!/^-?\d+(\.\d+)?$/.test(trimmed)) return trimmed
  const negative = trimmed.startsWith("-")
  const abs = negative ? trimmed.slice(1) : trimmed
  const [intRaw, fracRaw = ""] = abs.split(".")
  let intPart = intRaw
  let oneDec = fracRaw.charAt(0) || "0"
  const second = fracRaw.charAt(1)
  if (second && Number(second) >= 5) {
    const n = Number(oneDec) + 1
    if (n >= 10) {
      oneDec = "0"
      // bump int
      const digits = intPart.split("").map(Number)
      let i = digits.length - 1
      let carry = 1
      while (i >= 0 && carry) {
        const v = digits[i] + carry
        digits[i] = v % 10
        carry = Math.floor(v / 10)
        i -= 1
      }
      intPart = (carry ? String(carry) : "") + digits.join("")
    } else {
      oneDec = String(n)
    }
  }
  const sign = negative ? "-" : ""
  if (oneDec === "0") return `${sign}${intPart} %`
  return `${sign}${intPart},${oneDec} %`
}

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
  // tasas API suelen ser 0.19 → mostrar 19 %
  const trimmed = String(value).trim()
  if (!/^-?\d+(\.\d+)?$/.test(trimmed)) return `${trimmed}%`
  const asNum = trimmed
  // si parece fracción < 2, multiplicar visualmente ×100 con Decimal string
  const neg = asNum.startsWith("-")
  const abs = neg ? asNum.slice(1) : asNum
  const [i, f = ""] = abs.split(".")
  if (i === "0" || (i === "" && f)) {
    // 0.195 → 19.5
    const padded = (f + "0000").slice(0, 4)
    const whole = padded.slice(0, 2).replace(/^0/, "") || "0"
    const frac = padded.slice(2).replace(/0+$/, "")
    const body = frac ? `${whole},${frac.charAt(0)}` : whole
    return `${neg ? "-" : ""}${body} %`
  }
  return formatPercentCL(trimmed)
}

export function displayCorrectedGross(value: string | null | undefined): string {
  if (value == null || value === "") return "No calculable"
  return formatMoneyCLPTable(value)
}

export function displayCorrectedGrossPrecise(value: string | null | undefined): string {
  if (value == null || value === "") return "No calculable"
  return formatMoneyCLPPrecise(value)
}

export function displayUnitDifference(params: {
  stored_cost_gross?: string | null
  unit_difference: string | null | undefined
}): string {
  if (params.unit_difference == null || params.unit_difference === "") return "—"
  return formatMoneyCLPTable(params.unit_difference)
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

export function additionalTaxCategoryLabel(category: string | null | undefined): string {
  const c = (category || "").toLowerCase()
  if (c === "iva_advance" || c === "anticipo" || c.includes("advance")) {
    return "Anticipo tributario"
  }
  if (c === "ila" || c.includes("ila")) return "ILA / impuesto específico"
  return "Impuesto adicional"
}

/**
 * Título operativo de un impuesto adicional (nunca “IVA adicional”).
 * Ej.: "ILA destilados 31,5 %", "Anticipo harina 12 %".
 */
export function displayAdditionalTaxTitle(tax: {
  name?: string | null
  category?: string | null
  rate?: string | null
}): string {
  const rawName = (tax.name || "").trim()
  const lower = rawName.toLowerCase()
  const cat = (tax.category || "").toLowerCase()
  let title = rawName

  if (!title) {
    if (cat.includes("ila") || lower.includes("ila")) title = "ILA"
    else if (lower.includes("harina") || cat.includes("harina") || cat.includes("flour"))
      title = "Anticipo harina"
    else if (lower.includes("carne") || cat.includes("carne") || cat.includes("meat"))
      title = "Anticipo carne"
    else if (cat.includes("advance") || cat.includes("anticipo") || cat === "iva_advance")
      title = "Anticipo tributario"
    else title = "Impuesto adicional"
  } else {
    // Normalizar nombres que digan "IVA adicional"
    if (/iva\s*adicional/i.test(title)) {
      if (/harina/i.test(title)) title = "Anticipo harina"
      else if (/carne/i.test(title)) title = "Anticipo carne"
      else title = title.replace(/iva\s*adicional/gi, "Anticipo").trim()
    }
  }

  const rate = formatTaxRate(tax.rate)
  if (rate === "—") return title
  return `${title} ${rate}`
}

export function changeDirection(amount: string | null | undefined): "up" | "down" | "flat" | "none" {
  if (amount == null || amount === "" || amount === "0" || /^-?0+(\.0+)?$/.test(amount)) {
    return amount == null || amount === "" ? "none" : "flat"
  }
  return amount.startsWith("-") ? "down" : "up"
}
