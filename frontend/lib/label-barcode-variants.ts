/**
 * Variantes de búsqueda de barcode (espejo de backend/utils/label_barcode_variants.py).
 * La resolución real ocurre en POST /labels/resolve.
 */
export function barcodeLookupCandidates(read: string): string[] {
  const base = read.trim()
  if (!base) return []

  const seen = new Set<string>()
  const out: string[] = []
  const add = (candidate: string) => {
    const c = candidate.trim()
    if (c && !seen.has(c)) {
      seen.add(c)
      out.push(c)
    }
  }

  add(base)

  if (/^\d+$/.test(base)) {
    for (const length of [12, 13, 14]) {
      if (base.length < length) add(base.padStart(length, "0"))
    }
  }

  return out
}
