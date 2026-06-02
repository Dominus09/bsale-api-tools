/** Utilidades de rango de fechas para Pre-despacho OC. */

export const PRE_DESPACHO_WIDE_RANGE_DAYS = 7
export const PRE_DESPACHO_PAGE_LIMIT = 500

export function parseIsoDate(s: string): Date | null {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return null
  const d = new Date(`${s}T12:00:00`)
  return Number.isNaN(d.getTime()) ? null : d
}

export function rangeSpanDays(from: string, to: string): number {
  const a = parseIsoDate(from)
  const b = parseIsoDate(to)
  if (!a || !b) return 0
  const d0 = a <= b ? a : b
  const d1 = a <= b ? b : a
  const ms = d1.getTime() - d0.getTime()
  return Math.floor(ms / 86_400_000) + 1
}

export function isWidePreDespachoRange(from: string, to: string): boolean {
  return rangeSpanDays(from, to) > PRE_DESPACHO_WIDE_RANGE_DAYS
}

export const PRE_DESPACHO_WIDE_RANGE_HINT =
  "Rango amplio: esto puede tardar más. Recomendado filtrar por semana o día."

export const PRE_DESPACHO_TIMEOUT_MESSAGE =
  "La carga tardó demasiado o se interrumpió. Intente un rango más corto (una semana o el día actual) y pulse Recargar."
