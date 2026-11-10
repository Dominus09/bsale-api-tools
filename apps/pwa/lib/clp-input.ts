/** Parseo/visualización CLP para inputs (teclado numérico iPhone). */

/** Acepta "50000", "50.000", "50,000", "$50.000" → número. */
export function parseClpInput(raw: string): number | null {
  const t = String(raw || "").trim()
  if (!t) return null
  const cleaned = t
    .replace(/\$/g, "")
    .replace(/\s/g, "")
    .replace(/\./g, "")
    .replace(/,/g, "")
  if (!cleaned || !/^-?\d+$/.test(cleaned)) return null
  const n = Number(cleaned)
  return Number.isFinite(n) ? n : null
}

export function formatClpDisplay(n: number | null | undefined): string {
  if (n == null || Number.isNaN(n)) return ""
  return new Intl.NumberFormat("es-CL", {
    maximumFractionDigits: 0,
  }).format(Math.round(n))
}

export function formatClpCurrency(n: number | null | undefined): string {
  if (n == null || Number.isNaN(n)) return "—"
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  }).format(n)
}
