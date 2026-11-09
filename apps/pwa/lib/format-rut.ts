/** Formato visual de RUT chileno; no altera el valor canónico del backend. */

export function formatRutDisplay(rut?: string | null): string {
  const raw = (rut || "").trim()
  if (!raw) return ""
  // Quitar puntos/guiones y espacios; conservar DV.
  const cleaned = raw.replace(/\./g, "").replace(/\s/g, "").toUpperCase()
  const m = cleaned.match(/^(\d{1,8})-?([0-9K])$/)
  if (!m) return raw
  const body = m[1]
  const dv = m[2]
  const withDots = body.replace(/\B(?=(\d{3})+(?!\d))/g, ".")
  return `${withDots}-${dv}`
}
