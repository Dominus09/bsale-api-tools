import type {
  AnomalyCode,
  CostReliability,
  GrossCostStatus,
  MovementClass,
  Money,
  Severity,
  TaxProfileStatus,
  TaxValidation,
} from "./types"

export const MOVEMENT_LABELS: Record<MovementClass, string> = {
  factura: "Factura",
  guia_despacho: "Guía de despacho",
  nota_credito: "Nota de crédito",
  transferencia_interna: "Transferencia interna",
  ajuste: "Ajuste",
  sin_documento: "Sin documento",
  no_determinado: "No determinado",
}

export const RELIABILITY_LABELS: Record<CostReliability, string> = {
  consistente: "Consistente",
  requiere_revision: "Requiere revisión",
  desactualizado: "Desactualizado",
  solo_promedio_bsale: "Solo promedio Bsale",
  sin_costo: "Sin costo",
}

export const RELIABILITY_TONES: Record<CostReliability, string> = {
  consistente: "border-emerald-300 bg-emerald-50 text-emerald-800",
  requiere_revision: "border-red-300 bg-red-50 text-red-800",
  desactualizado: "border-amber-300 bg-amber-50 text-amber-800",
  solo_promedio_bsale: "border-sky-300 bg-sky-50 text-sky-800",
  sin_costo: "border-zinc-300 bg-zinc-100 text-zinc-700",
}

export const ANOMALY_LABELS: Record<AnomalyCode, string> = {
  alza_anormal: "Alza anormal",
  baja_sospechosa: "Baja sospechosa",
  costo_cero: "Costo cero",
  costo_nulo: "Costo nulo",
  cantidad_extrema: "Cantidad extrema",
  cantidad_no_positiva: "Cantidad no positiva",
  recepcion_modificada: "Recepción modificada",
  cambio_variante: "Cambio de variante",
  detalle_incompleto: "Detalle incompleto",
  movimiento_no_determinado: "Movimiento no determinado",
  diferencia_promedio_bsale: "Difiere del promedio Bsale",
}

export const SEVERITY_LABELS: Record<Severity, string> = { alta: "Alta", media: "Media", baja: "Baja" }

export const SEVERITY_TONES: Record<Severity, string> = {
  alta: "border-red-300 bg-red-50 text-red-800",
  media: "border-amber-300 bg-amber-50 text-amber-800",
  baja: "border-zinc-300 bg-zinc-50 text-zinc-700",
}

export const TAX_LABELS: Record<TaxProfileStatus, string> = {
  no_consultado: "Impuestos no consultados",
  sin_impuestos: "Sin impuestos (no se asume exento)",
  exento_verificado: "Exento verificado",
  iva: "IVA",
  iva_ila: "IVA + ILA",
  iva_anticipado: "IVA anticipado",
  ila_combinado: "Más de un ILA",
  impuesto_no_reconocido: "Impuesto no reconocido",
  configuracion_inconsistente: "Configuración inconsistente",
}

export const TAX_VALIDATION_LABELS: Record<TaxValidation, string> = {
  calculable: "Bruto calculable",
  requiere_revision_tributaria: "Requiere revisión tributaria",
  sin_configuracion: "Sin configuración tributaria",
}

export const GROSS_STATUS_LABELS: Record<GrossCostStatus, string> = {
  preliminar_no_verificado: "Preliminar, no verificado",
  requiere_revision_tributaria: "Requiere revisión tributaria",
  sin_configuracion_tributaria: "Sin configuración tributaria",
  sin_costo_base: "Sin costo base",
}

export const TAX_ISSUE_LABELS: Record<string, string> = {
  sin_impuestos_configurados: "Sin impuestos en Bsale",
  impuesto_repetido_en_bsale: "Impuesto repetido en el producto",
  configuracion_tributaria_retirada: "Configuración retirada de Bsale",
  impuesto_fuera_de_catalogo: "Impuesto fuera del catálogo",
  exento_con_impuestos: "Exento con impuestos en Bsale",
  impuesto_sin_clasificar: "Impuesto sin clasificar",
  codigo_sii_no_reconocido: "Código SII no reconocido",
  codigo_sii_ausente: "Sin código SII",
  tasa_distinta_a_codigo_sii: "Tasa distinta a la legal",
  tasa_nula: "Tasa vacía",
  impuesto_inactivo: "Impuesto inactivo",
  impuesto_retirado_de_bsale: "Impuesto retirado de Bsale",
  iva_anticipado: "IVA anticipado",
  ila_multiple: "Más de un ILA",
  iva_duplicado: "IVA duplicado",
  ila_sin_iva: "ILA sin IVA",
  clasificado_por_id_confirmado: "Clasificado por ID confirmado (sin código SII)",
  codigo_sii_contradice_id_confirmado: "Código SII contradice el ID confirmado",
  tasa_no_legal_para_tipo: "Tasa no legal para el tipo confirmado",
}

export const SORT_LABELS: Record<string, string> = {
  reliability: "Prioridad de revisión",
  change_pct_desc: "Mayor alza",
  change_pct_asc: "Mayor baja",
  last_date_desc: "Recepción más reciente",
  average_cost_desc: "Mayor costo promedio",
  product: "Producto (A–Z)",
}

export function toNumber(value: Money | number | undefined): number | null {
  if (value === null || value === undefined) return null
  const n = Number(value)
  return Number.isFinite(n) ? n : null
}

export function formatMoney(value: Money | number | undefined): string {
  const n = toNumber(value)
  if (n === null) return "—"
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    minimumFractionDigits: 0,
    maximumFractionDigits: n !== 0 && Math.abs(n) < 100 ? 2 : 0,
  }).format(n)
}

export function formatQty(value: Money | number | undefined): string {
  const n = toNumber(value)
  if (n === null) return "—"
  return new Intl.NumberFormat("es-CL", { maximumFractionDigits: 3 }).format(n)
}

export function formatPct(value: Money | number | undefined): string {
  const n = toNumber(value)
  if (n === null) return "—"
  return `${n > 0 ? "+" : ""}${n.toFixed(1)}%`
}

export function formatDate(value: string | null | undefined): string {
  if (!value) return "—"
  const [y, m, d] = value.slice(0, 10).split("-")
  if (!y || !m || !d) return "—"
  return `${d}-${m}-${y}`
}

export function formatDateTime(value: string | null | undefined): string {
  if (!value) return "—"
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return "—"
  return date.toLocaleString("es-CL", { dateStyle: "short", timeStyle: "short" })
}
