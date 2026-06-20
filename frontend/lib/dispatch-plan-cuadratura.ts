export type CuadraturaDiffStatus = "green" | "yellow" | "red"

export type CuadraturaOperationalStatus =
  | "pending"
  | "draft"
  | "in_review"
  | "difference"
  | "squared"

export const CUADRATURA_DIFF_YELLOW_MAX_CLP = 5000

export const MEDIOS_PAGO = [
  "transferencia",
  "efectivo",
  "cheque",
  "caja_vecina",
  "debito",
  "credito",
  "pendiente",
] as const

export type MedioPago = (typeof MEDIOS_PAGO)[number]

export const MEDIO_PAGO_LABELS: Record<MedioPago, string> = {
  transferencia: "Transferencia",
  efectivo: "Efectivo",
  cheque: "Cheque",
  caja_vecina: "Caja Vecina",
  debito: "Débito",
  credito: "Crédito",
  pendiente: "Pendiente",
}

/** Legacy v1 */
export type CuadraturaCreditNoteRow = {
  documento_venta: string
  nota_credito: string
  monto: number
  motivo: string
}

/** Legacy v1 */
export type CuadraturaNotLoadedRow = {
  cliente: string
  documento: string
  monto: number
  motivo: string
}

export type CuadraturaDocumentRow = {
  related_document_id?: number | null
  document_number?: number | null
  oc_document_id?: number | null
  client_name?: string
  monto_clp: number
  medio_pago: MedioPago | string
  observacion?: string
  route_order?: number | null
}

export type CuadraturaCreditNoteV2Row = {
  documento: string
  nota_credito: string
  monto: number
  observacion: string
}

export type CuadraturaNotLoadedV2Row = {
  producto: string
  producto_variante?: string
  cantidad: number
  motivo: string
  product_id?: number | null
  variant_id?: number | null
  codigo_barras?: string | null
}

export type CuadraturaCashCountRow = {
  denominacion_clp: number
  cantidad: number
  subtotal_clp: number
}

export const CASH_DENOMINATIONS_CLP = [
  20000, 10000, 5000, 2000, 1000, 500, 100, 50, 10,
] as const

export type CuadraturaResult = {
  resumen_pagos?: Record<string, number>
  notas_credito_clp: number
  no_cargados_clp: number
  venta_ajustada_clp: number
  total_recaudado_clp: number
  total_recaudado_documental_clp?: number
  total_efectivo_documental_clp?: number
  total_efectivo_contado_clp?: number
  diferencia_efectivo_clp?: number
  diferencia_clp: number
  diferencia_general_clp?: number
  diferencia_status: CuadraturaDiffStatus
  cash_count?: CuadraturaCashCountRow[]
}

export type CuadraturaProductCatalogRow = {
  product_id?: number | null
  variant_id?: number | null
  producto: string
  variante?: string
  producto_variante: string
  codigo_barras?: string | null
}

export function defaultCashCount(): CuadraturaCashCountRow[] {
  return CASH_DENOMINATIONS_CLP.map((denominacion_clp) => ({
    denominacion_clp,
    cantidad: 0,
    subtotal_clp: 0,
  }))
}

export function normalizeCashCount(
  rows: CuadraturaCashCountRow[] | null | undefined,
): CuadraturaCashCountRow[] {
  const byDenom = new Map<number, number>()
  for (const row of rows || []) {
    byDenom.set(row.denominacion_clp, Math.max(0, Math.round(row.cantidad || 0)))
  }
  return CASH_DENOMINATIONS_CLP.map((denominacion_clp) => {
    const cantidad = byDenom.get(denominacion_clp) ?? 0
    return {
      denominacion_clp,
      cantidad,
      subtotal_clp: denominacion_clp * cantidad,
    }
  })
}

export function summarizeMedios(documents: CuadraturaDocumentRow[]): Record<string, number> {
  const out: Record<string, number> = Object.fromEntries(MEDIOS_PAGO.map((m) => [m, 0]))
  for (const doc of documents) {
    const key = (doc.medio_pago || "pendiente") as string
    out[key] = (out[key] || 0) + Math.round(Number(doc.monto_clp) || 0)
  }
  return out
}

export function computeCuadraturaV2Result(params: {
  venta_picking_clp: number
  documents: CuadraturaDocumentRow[]
  credit_notes_v2: CuadraturaCreditNoteV2Row[]
  cash_count: CuadraturaCashCountRow[]
}): CuadraturaResult {
  const resumen = summarizeMedios(params.documents)
  const notas = params.credit_notes_v2.reduce(
    (acc, row) => acc + Math.round(Number(row.monto) || 0),
    0,
  )
  const ventaAjustada = Math.round(params.venta_picking_clp) - notas
  const totalRecaudadoDocumental = Object.values(resumen).reduce((a, b) => a + b, 0)
  const totalEfectivoDocumental = resumen.efectivo || 0
  const cashRows = normalizeCashCount(params.cash_count)
  const totalEfectivoContado = cashRows.reduce((a, r) => a + r.subtotal_clp, 0)
  const diferenciaEfectivo = totalEfectivoContado - totalEfectivoDocumental
  const diferenciaGeneral = ventaAjustada - totalRecaudadoDocumental
  const ad = Math.abs(diferenciaGeneral)
  let diferencia_status: CuadraturaDiffStatus = "red"
  if (ad === 0) diferencia_status = "green"
  else if (ad < CUADRATURA_DIFF_YELLOW_MAX_CLP) diferencia_status = "yellow"
  return {
    resumen_pagos: resumen,
    notas_credito_clp: notas,
    no_cargados_clp: 0,
    venta_ajustada_clp: ventaAjustada,
    total_recaudado_clp: totalRecaudadoDocumental,
    total_recaudado_documental_clp: totalRecaudadoDocumental,
    total_efectivo_documental_clp: totalEfectivoDocumental,
    total_efectivo_contado_clp: totalEfectivoContado,
    diferencia_efectivo_clp: diferenciaEfectivo,
    diferencia_clp: diferenciaGeneral,
    diferencia_general_clp: diferenciaGeneral,
    diferencia_status,
    cash_count: cashRows,
  }
}

/** Legacy v1 */
export function computeCuadraturaResult(params: {
  venta_picking_clp: number
  credit_notes: CuadraturaCreditNoteRow[]
  not_loaded: CuadraturaNotLoadedRow[]
  transferencia_clp?: number
  efectivo_clp?: number
  cheque_clp?: number
  debito_clp?: number
}): CuadraturaResult {
  const notas = params.credit_notes.reduce(
    (acc, row) => acc + Math.round(Number(row.monto) || 0),
    0,
  )
  const noCargados = params.not_loaded.reduce(
    (acc, row) => acc + Math.round(Number(row.monto) || 0),
    0,
  )
  const ventaAjustada = Math.round(params.venta_picking_clp) - notas - noCargados
  const totalRecaudado =
    Math.round(params.transferencia_clp || 0) +
    Math.round(params.efectivo_clp || 0) +
    Math.round(params.cheque_clp || 0) +
    Math.round(params.debito_clp || 0)
  const diferencia = ventaAjustada - totalRecaudado
  const ad = Math.abs(diferencia)
  let diferencia_status: CuadraturaDiffStatus = "red"
  if (ad === 0) diferencia_status = "green"
  else if (ad < CUADRATURA_DIFF_YELLOW_MAX_CLP) diferencia_status = "yellow"
  return {
    notas_credito_clp: notas,
    no_cargados_clp: noCargados,
    venta_ajustada_clp: ventaAjustada,
    total_recaudado_clp: totalRecaudado,
    diferencia_clp: diferencia,
    diferencia_status,
  }
}

export function observacionRequired(resultado: CuadraturaResult): boolean {
  const gen = Math.round(resultado.diferencia_general_clp ?? resultado.diferencia_clp ?? 0)
  const cash = Math.round(resultado.diferencia_efectivo_clp ?? 0)
  return gen !== 0 || cash !== 0
}

export function diffStatusClass(status: CuadraturaDiffStatus): string {
  if (status === "green")
    return "border-emerald-500/50 bg-emerald-500/10 text-emerald-800 dark:text-emerald-300"
  if (status === "yellow")
    return "border-amber-500/50 bg-amber-500/10 text-amber-900 dark:text-amber-200"
  return "border-red-500/50 bg-red-500/10 text-red-800 dark:text-red-300"
}

export function operationalStatusBadge(status: CuadraturaOperationalStatus | string): {
  label: string
  emoji: string
  className: string
} {
  switch (status) {
    case "squared":
      return {
        label: "Cuadrado",
        emoji: "🟢",
        className: "bg-emerald-500/15 text-emerald-800 dark:text-emerald-300",
      }
    case "in_review":
      return {
        label: "En revisión",
        emoji: "🟡",
        className: "bg-amber-500/15 text-amber-900 dark:text-amber-200",
      }
    case "difference":
      return {
        label: "Con diferencia",
        emoji: "🔴",
        className: "bg-red-500/15 text-red-800 dark:text-red-300",
      }
    case "draft":
      return {
        label: "Borrador",
        emoji: "📝",
        className: "bg-slate-500/15 text-slate-700 dark:text-slate-300",
      }
    default:
      return {
        label: "Pendiente",
        emoji: "⚪",
        className: "bg-muted text-muted-foreground",
      }
  }
}

export function emptyCreditNoteV2Row(): CuadraturaCreditNoteV2Row {
  return { documento: "", nota_credito: "", monto: 0, observacion: "" }
}

export function emptyNotLoadedV2Row(): CuadraturaNotLoadedV2Row {
  return { producto: "", cantidad: 0, motivo: "" }
}

export function resolveProductFromCatalog(
  query: string,
  catalog: CuadraturaProductCatalogRow[],
): CuadraturaProductCatalogRow | null {
  const q = query.trim()
  if (!q) return null
  const qLower = q.toLowerCase()
  const byBarcode = catalog.find((p) => (p.codigo_barras || "").trim() === q)
  if (byBarcode) return byBarcode
  const exact = catalog.find((p) => p.producto_variante.toLowerCase() === qLower)
  if (exact) return exact
  return (
    catalog.find((p) => p.producto_variante.toLowerCase().includes(qLower)) ||
    catalog.find((p) => p.producto.toLowerCase().includes(qLower)) ||
    null
  )
}

export function normalizeNotLoadedRow(
  row: CuadraturaNotLoadedV2Row,
  catalog: CuadraturaProductCatalogRow[],
): CuadraturaNotLoadedV2Row {
  const match = resolveProductFromCatalog(row.producto, catalog)
  if (!match) return row
  return {
    ...row,
    producto: match.producto_variante,
    producto_variante: match.producto_variante,
    codigo_barras: match.codigo_barras,
    product_id: match.product_id,
    variant_id: match.variant_id,
  }
}

export function normalizeNotLoadedRows(
  rows: CuadraturaNotLoadedV2Row[],
  catalog: CuadraturaProductCatalogRow[],
): CuadraturaNotLoadedV2Row[] {
  return rows.map((row) => normalizeNotLoadedRow(row, catalog))
}

/** Legacy helpers */
export function emptyCreditNoteRow(): CuadraturaCreditNoteRow {
  return { documento_venta: "", nota_credito: "", monto: 0, motivo: "" }
}

export function emptyNotLoadedRow(): CuadraturaNotLoadedRow {
  return { cliente: "", documento: "", monto: 0, motivo: "" }
}
