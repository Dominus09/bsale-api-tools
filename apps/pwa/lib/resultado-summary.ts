/** Resumen seguro para pantalla /entrega/resultado (sin PII extra). */

export type ResultadoDoc = {
  document_type?: string | null
  document_number?: number | string | null
  document_total?: number | null
  result_status?: string | null
}

export type ResultadoLine = {
  product_name?: string | null
  planned_units?: number | null
  delivered_units?: number | null
  undelivered_reason?: string | null
}

export type ResultadoCollection = {
  method?: string | null
  declared_amount?: number | null
  document_result_id?: number | null
}

export type ResultadoConformity = {
  receptor_name?: string | null
  refused?: boolean | null
  invalidated_at?: string | null
  signed_at?: string | null
  signature_object_key?: string | null
  signature_url?: string | null
}

export function buildResultadoSummary(input: {
  docs?: ResultadoDoc[] | null
  lines?: ResultadoLine[] | null
  collections?: ResultadoCollection[] | null
  conformity?: ResultadoConformity | null
}): {
  docCount: number
  lineCount: number
  collectionCount: number
  hasConformity: boolean
  conformityLabel: string
  compactLines: string[]
} {
  const docs = input.docs || []
  const lines = input.lines || []
  const cols = (input.collections || []).filter((c) => c.document_result_id != null)
  const conf = input.conformity
  const compact: string[] = []

  for (const d of docs) {
    compact.push(
      `${d.document_type || "Doc"} ${d.document_number ?? "—"} · ${d.result_status || "—"}`,
    )
  }
  let full = 0
  let partial = 0
  let none = 0
  for (const ln of lines) {
    const planned = Number(ln.planned_units || 0)
    const delivered = Number(ln.delivered_units || 0)
    if (delivered >= planned && planned > 0) full += 1
    else if (delivered <= 0) none += 1
    else partial += 1
  }
  if (lines.length) {
    compact.push(`Líneas: ${full} completas · ${partial} parciales · ${none} no entregadas`)
  }
  if (cols.length) {
    compact.push(`Cobranza: ${cols.length} declaración(es) vigente(s)`)
  }

  let conformityLabel = "Sin conformidad"
  let hasConformity = false
  if (conf && !conf.invalidated_at) {
    hasConformity = true
    if (conf.refused) conformityLabel = "Negativa registrada"
    else if (conf.receptor_name) {
      conformityLabel = `Firmada · ${String(conf.receptor_name)}`
    } else conformityLabel = "Conformidad registrada"
  } else if (conf?.invalidated_at) {
    conformityLabel = "Conformidad invalidada"
  }
  compact.push(`Conformidad: ${conformityLabel}`)

  return {
    docCount: docs.length,
    lineCount: lines.length,
    collectionCount: cols.length,
    hasConformity,
    conformityLabel,
    compactLines: compact,
  }
}
