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
  documento_venta: string
  cliente: string
  numero_nc: string
  monto: number
  motivo: string
  aplicada: boolean
}

export type CuadraturaNotLoadedV2Row = {
  cliente: string
  producto: string
  cantidad: number
  motivo: string
  product_id?: number | null
  variant_id?: number | null
  monto_clp?: number | null
}

export type CuadraturaResult = {
  resumen_pagos?: Record<string, number>
  notas_credito_clp: number
  no_cargados_clp: number
  venta_ajustada_clp: number
  total_recaudado_clp: number
  diferencia_clp: number
  diferencia_status: CuadraturaDiffStatus
}

export type CuadraturaProductCatalogRow = {
  product_id?: number | null
  variant_id?: number | null
  producto: string
  codigo_barras?: string | null
  unit_price_clp: number
}

function sumAppliedCreditNotes(rows: CuadraturaCreditNoteV2Row[]): number {
  return rows.reduce((acc, row) => {
    if (!row.aplicada) return acc
    return acc + Math.round(Number(row.monto) || 0)
  }, 0)
}

function sumNotLoaded(rows: CuadraturaNotLoadedV2Row[]): number {
  return rows.reduce((acc, row) => acc + Math.round(Number(row.monto_clp) || 0), 0)
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
  not_loaded_v2: CuadraturaNotLoadedV2Row[]
}): CuadraturaResult {
  const resumen = summarizeMedios(params.documents)
  const notas = sumAppliedCreditNotes(params.credit_notes_v2)
  const noCargados = sumNotLoaded(params.not_loaded_v2)
  const ventaAjustada = Math.round(params.venta_picking_clp) - notas - noCargados
  const totalRecaudado = Object.values(resumen).reduce((a, b) => a + b, 0)
  const diferencia = ventaAjustada - totalRecaudado
  const ad = Math.abs(diferencia)
  let diferencia_status: CuadraturaDiffStatus = "red"
  if (ad === 0) diferencia_status = "green"
  else if (ad < CUADRATURA_DIFF_YELLOW_MAX_CLP) diferencia_status = "yellow"
  return {
    resumen_pagos: resumen,
    notas_credito_clp: notas,
    no_cargados_clp: noCargados,
    venta_ajustada_clp: ventaAjustada,
    total_recaudado_clp: totalRecaudado,
    diferencia_clp: diferencia,
    diferencia_status,
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

export function observacionRequired(diferenciaClp: number): boolean {
  return Math.round(diferenciaClp) !== 0
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
        label: "Diferencia",
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
  return {
    documento_venta: "",
    cliente: "",
    numero_nc: "",
    monto: 0,
    motivo: "",
    aplicada: true,
  }
}

export function emptyNotLoadedV2Row(): CuadraturaNotLoadedV2Row {
  return { cliente: "", producto: "", cantidad: 0, motivo: "", monto_clp: 0 }
}

export function estimateNotLoadedMonto(
  row: CuadraturaNotLoadedV2Row,
  catalog: CuadraturaProductCatalogRow[],
): number {
  if (row.monto_clp && row.monto_clp > 0) return row.monto_clp
  const qty = Math.max(0, Number(row.cantidad) || 0)
  if (qty <= 0) return 0
  const needle = (row.producto || "").trim().toLowerCase()
  let match =
    catalog.find(
      (p) =>
        row.product_id != null &&
        p.product_id === row.product_id &&
        (row.variant_id == null || p.variant_id === row.variant_id),
    ) ||
    catalog.find((p) => needle && p.producto.toLowerCase() === needle) ||
    catalog.find((p) => needle && p.producto.toLowerCase().includes(needle))
  if (!match) return 0
  return Math.round((match.unit_price_clp || 0) * qty)
}

export function enrichNotLoadedRows(
  rows: CuadraturaNotLoadedV2Row[],
  catalog: CuadraturaProductCatalogRow[],
): CuadraturaNotLoadedV2Row[] {
  return rows.map((row) => ({
    ...row,
    monto_clp: estimateNotLoadedMonto(row, catalog),
  }))
}

/** Legacy helpers */
export function emptyCreditNoteRow(): CuadraturaCreditNoteRow {
  return { documento_venta: "", nota_credito: "", monto: 0, motivo: "" }
}

export function emptyNotLoadedRow(): CuadraturaNotLoadedRow {
  return { cliente: "", documento: "", monto: 0, motivo: "" }
}
