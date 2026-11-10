/** Suma montos de picking sin duplicar related_document_id. */

export function sumUniqueDocumentTotals(
  docs: Array<{ related_document_id?: number | null; document_total?: number | null }>,
): number | null {
  let sum = 0
  let known = false
  const seen = new Set<number>()
  for (const d of docs) {
    const rel = d.related_document_id
    if (rel != null) {
      const id = Number(rel)
      if (seen.has(id)) continue
      seen.add(id)
    }
    if (typeof d.document_total !== "number" || !Number.isFinite(d.document_total)) {
      continue
    }
    sum += d.document_total
    known = true
  }
  return known ? Math.round(sum * 100) / 100 : null
}
