/**
 * PPUM (precio por unidad de medida) para Etiquetas Socios V2.
 * Parsea contenido desde nombre/presentación; no inventa si faltan datos.
 */

export type PpumUnitLabel = "litro" | "l" | "kg"

export type ParsedProductContent = {
  /** Cantidad en unidad base (litros o kilogramos). */
  baseAmount: number
  /** Etiqueta de display preferida: "por litro" / "por kg". */
  unitLabel: PpumUnitLabel
  kind: "volume" | "weight"
}

const UNAVAILABLE = "PPUM no disponible"

function parseNumber(raw: string): number | null {
  const n = Number(String(raw).replace(",", ".").trim())
  return Number.isFinite(n) && n > 0 ? n : null
}

/**
 * Extrae contenido líquido o de peso del texto del producto/variante.
 * Preferencia retail: líquidos → litro; peso → kg.
 */
export function parseProductContent(
  ...texts: Array<string | null | undefined>
): ParsedProductContent | null {
  const haystack = texts
    .map((t) => String(t || "").trim())
    .filter(Boolean)
    .join(" ")
  if (!haystack) return null

  // Orden: unidades más específicas primero; última coincidencia gana (suele estar en variante).
  const patterns: Array<{
    re: RegExp
    kind: "volume" | "weight"
    toBase: (n: number) => number
    unitLabel: PpumUnitLabel
  }> = [
    {
      re: /(\d+(?:[.,]\d+)?)\s*(?:cc|c\.c\.|ml)\b/gi,
      kind: "volume",
      toBase: (n) => n / 1000,
      unitLabel: "litro",
    },
    {
      re: /(\d+(?:[.,]\d+)?)\s*(?:lt|lts|litros?)\b/gi,
      kind: "volume",
      toBase: (n) => n,
      unitLabel: "litro",
    },
    // "1.2 L" / "1 L" — evitar capturar letras sueltas dentro de palabras
    {
      re: /(\d+(?:[.,]\d+)?)\s*l\b/gi,
      kind: "volume",
      toBase: (n) => n,
      unitLabel: "litro",
    },
    {
      re: /(\d+(?:[.,]\d+)?)\s*(?:kg|kgs|kilos?)\b/gi,
      kind: "weight",
      toBase: (n) => n,
      unitLabel: "kg",
    },
    {
      re: /(\d+(?:[.,]\d+)?)\s*(?:grs?|gramos?|g)\b/gi,
      kind: "weight",
      toBase: (n) => n / 1000,
      unitLabel: "kg",
    },
  ]

  let best: ParsedProductContent | null = null
  let bestIndex = -1

  for (const p of patterns) {
    p.re.lastIndex = 0
    let m: RegExpExecArray | null
    while ((m = p.re.exec(haystack)) !== null) {
      const raw = parseNumber(m[1])
      if (raw == null) continue
      const baseAmount = p.toBase(raw)
      if (!(baseAmount > 0)) continue
      if (m.index >= bestIndex) {
        bestIndex = m.index
        best = { baseAmount, unitLabel: p.unitLabel, kind: p.kind }
      }
    }
  }

  return best
}

function formatClpAmount(value: number): string {
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  }).format(value)
}

function unitPhrase(unit: PpumUnitLabel): string {
  if (unit === "litro") return "por litro"
  if (unit === "l") return "por l"
  return "por kg"
}

/** Texto PPUM listo para imprimir, o "PPUM no disponible". */
export function formatPpum(
  price: number | null | undefined,
  content: ParsedProductContent | null,
): string {
  if (price == null || !Number.isFinite(price) || !content) return UNAVAILABLE
  if (!(content.baseAmount > 0)) return UNAVAILABLE
  const perUnit = Math.round(price / content.baseAmount)
  if (!Number.isFinite(perUnit) || perUnit <= 0) return UNAVAILABLE
  return `${formatClpAmount(perUnit)} ${unitPhrase(content.unitLabel)}`
}

export function resolvePpumLabel(
  price: number | null | undefined,
  productName: string,
  variantName?: string | null,
): string {
  const content = parseProductContent(productName, variantName)
  return formatPpum(price, content)
}

export const PPUM_UNAVAILABLE_LABEL = UNAVAILABLE
