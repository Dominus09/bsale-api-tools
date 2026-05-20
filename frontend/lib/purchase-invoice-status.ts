/** Estados unificados OC (API / v_purchase_document_status_full). */

export type PurchaseInvoiceStatusCode =
  | "FACTURADA_CONFIRMADA"
  | "PROBABLE_FACTURADA_HIGH"
  | "PROBABLE_FACTURADA_MEDIUM"
  | "PROBABLE_FACTURADA_LOW"
  | "PENDIENTE"

export type PurchaseInvoiceStatusFilter =
  | "all"
  | "confirmed"
  | "probable"
  | "pending"

export type PurchaseInvoiceStatusFields = {
  is_invoiced?: boolean | null
  purchase_status?: string | null
  estado_real?: string | null
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

export function resolvePurchaseStatusCode(
  row: PurchaseInvoiceStatusFields,
): PurchaseInvoiceStatusCode {
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
  if (estado === "Facturada" || row.is_invoiced === true) return CONFIRMED
  if (estado === "Probable facturada") return HIGH
  return PENDING
}

export function purchaseStatusBadgeLabel(code: PurchaseInvoiceStatusCode): string {
  switch (code) {
    case CONFIRMED:
      return "✔ Facturada"
    case HIGH:
    case MEDIUM:
    case LOW:
      return "⚠ Probable Facturada"
    default:
      return "○ Pendiente"
  }
}

export function purchaseStatusBadgeClass(code: PurchaseInvoiceStatusCode): string {
  switch (code) {
    case CONFIRMED:
      return "bg-green-100 text-green-800 border-green-200 hover:bg-green-100"
    case HIGH:
      return "bg-yellow-200 text-yellow-900 border-yellow-300 hover:bg-yellow-200"
    case MEDIUM:
      return "bg-yellow-100 text-yellow-800 border-yellow-200 hover:bg-yellow-100"
    case LOW:
      return "bg-orange-100 text-orange-800 border-orange-200 hover:bg-orange-100"
    default:
      return "bg-gray-100 text-gray-700 border-gray-200 hover:bg-gray-100"
  }
}

export function purchaseStatusTooltip(code: PurchaseInvoiceStatusCode): string {
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
