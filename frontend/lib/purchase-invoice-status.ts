/** Estados unificados OC (API / v_purchase_document_status_full). */

export type PurchaseInvoiceStatusCode =
  | "FACTURADA_CONFIRMADA"
  | "PROBABLE_FACTURADA_HIGH"
  | "PROBABLE_FACTURADA_MEDIUM"
  | "PROBABLE_FACTURADA_LOW"
  | "PENDIENTE"
  | "ANULADA"

export type PurchaseInvoiceStatusFilter =
  | "all"
  | "confirmed"
  | "probable"
  | "pending"

export type PurchaseInvoiceStatusFields = {
  is_invoiced?: boolean | null
  purchase_status?: string | null
  estado_real?: string | null
  billing_status?: string | null
  billing_label_es?: string | null
  dispatch_closed?: boolean | null
  planning_eligible?: boolean | null
  fulfillment_status?: string | null
  excluded_reason?: string | null
  probable_score?: number | null
  probable_tier?: string | null
  display_score?: number | null
  associated_document_label?: string | null
  invoicing_number?: number | null
  invoicing_document_type_id?: number | null
  probable_number?: number | null
  candidate_number?: number | null
  candidate_document_type_label?: string | null
  probable_document_type_id?: number | null
  candidate_document_type?: number | null
}

const CONFIRMED: PurchaseInvoiceStatusCode = "FACTURADA_CONFIRMADA"
const HIGH: PurchaseInvoiceStatusCode = "PROBABLE_FACTURADA_HIGH"
const MEDIUM: PurchaseInvoiceStatusCode = "PROBABLE_FACTURADA_MEDIUM"
const LOW: PurchaseInvoiceStatusCode = "PROBABLE_FACTURADA_LOW"
const PENDING: PurchaseInvoiceStatusCode = "PENDIENTE"
const CANCELLED: PurchaseInvoiceStatusCode = "ANULADA"

/** Umbrales operacionales (alineados con backend invoicing_auto_confirm). */
export const AUTO_CONFIRM_MIN_SCORE = 75
export const PROBABLE_MIN_SCORE = 60

function isCancelledRow(row: PurchaseInvoiceStatusFields): boolean {
  const raw =
    typeof row.purchase_status === "string" ? row.purchase_status.trim() : ""
  if (raw === CANCELLED || raw === "cancelled") return true
  if (row.billing_status === "cancelled") return true
  if (row.excluded_reason === "cancelled_order") return true
  const estado =
    typeof row.estado_real === "string" ? row.estado_real.trim() : ""
  return estado === "Anulada" || estado.toLowerCase() === "anulada"
}

export function resolvePurchaseStatusCode(
  row: PurchaseInvoiceStatusFields,
): PurchaseInvoiceStatusCode {
  // Precedencia: CANCELLED > CONFIRMED > PROBABLE > PENDING
  if (isCancelledRow(row)) return CANCELLED

  const raw =
    typeof row.purchase_status === "string" ? row.purchase_status.trim() : ""
  if (raw === CONFIRMED) return CONFIRMED
  if (raw === HIGH || raw === MEDIUM || raw === LOW) return raw
  if (raw === "PROBABLE_FACTURADA") {
    const tier =
      typeof row.probable_tier === "string" ? row.probable_tier.trim() : ""
    if (tier === HIGH || tier === MEDIUM || tier === LOW) return tier
    return HIGH
  }

  const estado =
    typeof row.estado_real === "string" ? row.estado_real.trim() : ""
  if (
    estado === "Facturada" ||
    estado.startsWith("Facturada con NC") ||
    row.is_invoiced === true ||
    (row.dispatch_closed === true && row.billing_status !== "cancelled")
  ) {
    return CONFIRMED
  }

  const sc = operationalScore(row)
  if (sc != null && sc >= AUTO_CONFIRM_MIN_SCORE) return CONFIRMED
  if (estado === "Probable facturada" || (sc != null && sc >= PROBABLE_MIN_SCORE)) {
    if (sc != null && sc >= 90) return HIGH
    if (sc != null && sc >= PROBABLE_MIN_SCORE) return MEDIUM
    return LOW
  }
  return PENDING
}

function operationalScore(row: PurchaseInvoiceStatusFields): number | null {
  const sc = row.probable_score ?? row.display_score
  if (sc == null || !Number.isFinite(Number(sc))) return null
  return Number(sc)
}

export function isAutoConfirmedOperational(row: PurchaseInvoiceStatusFields): boolean {
  if (row.is_invoiced === true) return false
  const sc = operationalScore(row)
  return sc != null && sc >= AUTO_CONFIRM_MIN_SCORE
}

export function isBsaleConfirmed(row: PurchaseInvoiceStatusFields): boolean {
  if (isCancelledRow(row)) return false
  if (row.is_invoiced === true) return true
  const raw =
    typeof row.purchase_status === "string" ? row.purchase_status.trim() : ""
  if (raw === CONFIRMED) return true
  const estado =
    typeof row.estado_real === "string" ? row.estado_real.trim() : ""
  return (
    estado === "Facturada" ||
    estado.startsWith("Facturada con NC") ||
    (row.dispatch_closed === true && row.billing_status !== "cancelled")
  )
}

export function purchaseStatusBadgeLabel(
  code: PurchaseInvoiceStatusCode,
  row?: PurchaseInvoiceStatusFields,
): string {
  if (row?.billing_label_es) return row.billing_label_es
  if (
    typeof row?.estado_real === "string" &&
    row.estado_real.startsWith("Facturada")
  ) {
    return row.estado_real
  }
  if (code === CANCELLED) return "Anulada"
  if (code === CONFIRMED && row && isAutoConfirmedOperational(row)) {
    const sc = operationalScore(row)
    return sc != null ? `Auto-confirmada (score ${Math.round(sc)})` : "Auto-confirmada"
  }
  switch (code) {
    case CONFIRMED:
      return "Facturada"
    case HIGH:
    case MEDIUM:
    case LOW:
      return "Probable facturada"
    default:
      return "Pendiente por facturar"
  }
}

export function purchaseStatusBadgeClass(code: PurchaseInvoiceStatusCode): string {
  switch (code) {
    case CONFIRMED:
      return "border-emerald-200 bg-emerald-50 text-emerald-800 hover:bg-emerald-50 dark:border-emerald-900 dark:bg-emerald-950/50 dark:text-emerald-200"
    case HIGH:
    case MEDIUM:
    case LOW:
      return "border-amber-200 bg-amber-50 text-amber-900 hover:bg-amber-50 dark:border-amber-900 dark:bg-amber-950/40 dark:text-amber-100"
    case CANCELLED:
      return "border-rose-200 bg-rose-50 text-rose-800 hover:bg-rose-50 dark:border-rose-900 dark:bg-rose-950/40 dark:text-rose-200"
    default:
      return "border-slate-200 bg-slate-100 text-slate-700 hover:bg-slate-100 dark:border-slate-700 dark:bg-slate-800/60 dark:text-slate-300"
  }
}

export function purchaseStatusTooltip(
  code: PurchaseInvoiceStatusCode,
  row?: PurchaseInvoiceStatusFields,
): string {
  if (code === CONFIRMED && row && isAutoConfirmedOperational(row)) {
    return "Coincidencia operacional con score ≥75; tratada como facturada para picking y KPIs"
  }
  switch (code) {
    case CONFIRMED:
      return "Relación confirmada vía relateddetailid"
    case HIGH:
    case MEDIUM:
    case LOW:
      return "Coincidencia operacional detectada automáticamente (Bsale API no expone relación oficial)"
    default:
      return "No se detectaron relaciones operacionales"
  }
}

export function documentTypeLabel(typeId: number | null | undefined): string {
  if (typeId === 1) return "Boleta"
  if (typeId === 6) return "Factura"
  return typeId != null ? `Tipo ${typeId}` : "Documento"
}

/** Etiqueta operacional sin IDs internos. */
export function associatedDocumentLabel(row: PurchaseInvoiceStatusFields): string | null {
  const fromApi =
    typeof row.associated_document_label === "string"
      ? row.associated_document_label.trim()
      : ""
  if (fromApi) return fromApi

  const code = resolvePurchaseStatusCode(row)
  if (code === CONFIRMED) {
    const num = row.invoicing_number
    if (num != null) return `${documentTypeLabel(row.invoicing_document_type_id)} ${num}`
    return null
  }
  if (code === HIGH || code === MEDIUM || code === LOW) {
    const num = row.candidate_number ?? row.probable_number
    const tipo =
      row.candidate_document_type_label?.trim() ||
      documentTypeLabel(
        row.candidate_document_type ?? row.probable_document_type_id,
      )
    if (num != null) return `${tipo} ${num}`
  }
  return null
}

export function displayScoreValue(row: PurchaseInvoiceStatusFields): number | null {
  const ds = row.display_score
  if (ds != null && Number.isFinite(Number(ds))) return Math.round(Number(ds))
  const code = resolvePurchaseStatusCode(row)
  if (code === CONFIRMED) return 100
  const sc = row.probable_score
  if (sc != null && Number.isFinite(Number(sc))) return Math.round(Number(sc))
  return null
}

export function matchesPurchaseStatusFilter(
  row: PurchaseInvoiceStatusFields,
  filter: PurchaseInvoiceStatusFilter,
): boolean {
  if (filter === "all") return true
  const code = resolvePurchaseStatusCode(row)
  if (filter === "confirmed") return code === CONFIRMED
  if (filter === "probable")
    return code === HIGH || code === MEDIUM || code === LOW
  return code === PENDING
}

/** Fila de plan dispatch (GET invoiced-documents / dashboard items). */
export type DispatchInvoicingRowFields = {
  status?: string | null
  relation_source?: string | null
  is_invoiced_confirmed?: boolean | null
  is_auto_confirmed?: boolean | null
  probable_score?: number | null
}

export function dispatchInvoicingBadgeLabel(row: DispatchInvoicingRowFields): string {
  if (row.relation_source === "auto_match" || row.is_auto_confirmed) {
    const sc = row.probable_score
    const n =
      sc != null && Number.isFinite(Number(sc)) ? Math.round(Number(sc)) : null
    return n != null ? `Auto-confirmada (score ${n})` : "Auto-confirmada"
  }
  if (row.status === "confirmed" || row.is_invoiced_confirmed) return "Facturada"
  if (row.status === "probable") return "Probable"
  return "Pendiente"
}

export function dispatchInvoicingBadgeClass(row: DispatchInvoicingRowFields): string {
  if (
    row.status === "confirmed" ||
    row.is_invoiced_confirmed ||
    row.relation_source === "auto_match"
  ) {
    return purchaseStatusBadgeClass(CONFIRMED)
  }
  if (row.status === "probable") return purchaseStatusBadgeClass(MEDIUM)
  return purchaseStatusBadgeClass(PENDING)
}
