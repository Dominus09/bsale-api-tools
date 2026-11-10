/**
 * Cobranza vigente: solo filas ligadas a document_result actuales.
 * Excluye huérfanas (document_result_id null) y docs ya no presentes.
 */

export type CollectionRow = {
  id?: number | null
  document_result_id?: number | null
  method?: string | null
  declared_amount?: number | null
  pending_balance?: number | null
  document_amount?: number | null
  note?: string | null
  verified_amount?: number | null
}

export function filterActiveCollections(
  collections: CollectionRow[],
  documentResultIds: Array<number | string | null | undefined>,
): CollectionRow[] {
  const allowed = new Set(
    documentResultIds
      .map((x) => (x == null || x === "" ? null : Number(x)))
      .filter((n): n is number => n != null && Number.isFinite(n)),
  )
  if (allowed.size === 0) return []
  return collections.filter((c) => {
    const id = c.document_result_id
    if (id == null) return false
    return allowed.has(Number(id))
  })
}

export type CollectionTotals = {
  byMethod: Map<string, number>
  declaredAll: number
  pendingExplicit: number
  cash: number
  transferDeclared: number
  credit: number
  pendingBalance: number
  receivedLike: number
}

export function summarizeCollections(
  collections: CollectionRow[],
  documentTotal: number,
): CollectionTotals {
  const byMethod = new Map<string, number>()
  let declaredAll = 0
  let pendingExplicit = 0
  let cash = 0
  let transferDeclared = 0
  let credit = 0
  for (const c of collections) {
    const method = String(c.method || "")
    const declared = typeof c.declared_amount === "number" ? c.declared_amount : 0
    byMethod.set(method, (byMethod.get(method) || 0) + declared)
    if (method === "not_applicable") continue
    declaredAll += declared
    if (method === "pending") pendingExplicit += declared
    if (method === "cash") cash += declared
    if (method === "transfer_declared") transferDeclared += declared
    if (method === "credit") credit += declared
  }
  const implicit = Math.max(0, documentTotal - declaredAll)
  // Una sola vez: pending explícito + remanente solo si no cubre el total.
  const pendingBalance = pendingExplicit + implicit
  return {
    byMethod,
    declaredAll,
    pendingExplicit,
    cash,
    transferDeclared,
    credit,
    pendingBalance,
    receivedLike: cash, // solo efectivo se trata como "por rendir"; no credit ni transfer
  }
}

/** True si hay filas huérfanas o suma incoherente respecto al total. */
export function collectionConsistencyError(
  collections: CollectionRow[],
  documentResultIds: number[],
  documentTotal: number,
): string | null {
  const orphans = collections.filter(
    (c) => c.document_result_id == null || c.document_result_id === undefined,
  )
  if (orphans.length > 0) {
    return "Hay declaraciones de cobranza sin documento; vuelva a guardar la cobranza"
  }
  const active = filterActiveCollections(collections, documentResultIds)
  const foreign = collections.filter((c) => {
    if (c.document_result_id == null) return false
    return !documentResultIds.includes(Number(c.document_result_id))
  })
  if (foreign.length > 0 && documentResultIds.length > 0) {
    /* otras facturas de la misma visita: ok */
  }
  const sum = active.reduce((acc, c) => {
    if (c.method === "not_applicable") return acc
    return acc + (typeof c.declared_amount === "number" ? c.declared_amount : 0)
  }, 0)
  if (sum - documentTotal > 0.009) {
    return "La cobranza vigente supera el total del documento"
  }
  return null
}
