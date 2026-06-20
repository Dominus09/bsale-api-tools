export type CuadraturaDiffStatus = "green" | "yellow" | "red"

export const CUADRATURA_DIFF_YELLOW_MAX_CLP = 5000

export type CuadraturaCreditNoteRow = {
  documento_venta: string
  nota_credito: string
  monto: number
  motivo: string
}

export type CuadraturaNotLoadedRow = {
  cliente: string
  documento: string
  monto: number
  motivo: string
}

export type CuadraturaResult = {
  notas_credito_clp: number
  no_cargados_clp: number
  venta_ajustada_clp: number
  total_recaudado_clp: number
  diferencia_clp: number
  diferencia_status: CuadraturaDiffStatus
}

function sumRows(rows: { monto?: number | null }[]): number {
  return rows.reduce((acc, row) => acc + Math.round(Number(row.monto) || 0), 0)
}

export function computeCuadraturaResult(params: {
  venta_picking_clp: number
  credit_notes: CuadraturaCreditNoteRow[]
  not_loaded: CuadraturaNotLoadedRow[]
  transferencia_clp?: number
  efectivo_clp?: number
  cheque_clp?: number
  debito_clp?: number
}): CuadraturaResult {
  const notas = sumRows(params.credit_notes)
  const noCargados = sumRows(params.not_loaded)
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
  if (status === "green") return "border-emerald-500/50 bg-emerald-500/10 text-emerald-800 dark:text-emerald-300"
  if (status === "yellow") return "border-amber-500/50 bg-amber-500/10 text-amber-900 dark:text-amber-200"
  return "border-red-500/50 bg-red-500/10 text-red-800 dark:text-red-300"
}

export function emptyCreditNoteRow(): CuadraturaCreditNoteRow {
  return { documento_venta: "", nota_credito: "", monto: 0, motivo: "" }
}

export function emptyNotLoadedRow(): CuadraturaNotLoadedRow {
  return { cliente: "", documento: "", monto: 0, motivo: "" }
}
