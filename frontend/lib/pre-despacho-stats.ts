import type { DistribuidoraDispatchPrepPlanningRow } from "@/lib/api"
import {
  matchesPurchaseStatusFilter,
  resolvePurchaseStatusCode,
  type PurchaseInvoiceStatusFilter,
  type PurchaseInvoiceStatusFields,
} from "@/lib/purchase-invoice-status"

export type PreDespachoOperationalStats = {
  totalOrders: number
  totalAmount: number
  pending: number
  invoiced: number
  probable: number
}

export function computePreDespachoStats(
  rows: DistribuidoraDispatchPrepPlanningRow[],
): PreDespachoOperationalStats {
  let totalAmount = 0
  let pending = 0
  let invoiced = 0
  let probable = 0

  for (const row of rows) {
    const amt = Number(row.total_amount)
    if (Number.isFinite(amt)) totalAmount += amt
    const code = resolvePurchaseStatusCode(row as PurchaseInvoiceStatusFields)
    if (code === "FACTURADA_CONFIRMADA") invoiced += 1
    else if (
      code === "PROBABLE_FACTURADA_HIGH" ||
      code === "PROBABLE_FACTURADA_MEDIUM" ||
      code === "PROBABLE_FACTURADA_LOW"
    ) {
      probable += 1
    } else pending += 1
  }

  return {
    totalOrders: rows.length,
    totalAmount,
    pending,
    invoiced,
    probable,
  }
}

export function filterPlanningRowsByStatus(
  rows: DistribuidoraDispatchPrepPlanningRow[],
  filter: PurchaseInvoiceStatusFilter,
): DistribuidoraDispatchPrepPlanningRow[] {
  if (filter === "all") return rows
  return rows.filter((r) =>
    matchesPurchaseStatusFilter(r as PurchaseInvoiceStatusFields, filter),
  )
}
