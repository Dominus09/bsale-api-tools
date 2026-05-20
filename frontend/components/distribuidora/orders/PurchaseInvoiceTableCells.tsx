"use client"

import {
  type PurchaseInvoiceStatusFields,
  associatedDocumentLabel,
  displayScoreValue,
} from "@/lib/purchase-invoice-status"
import { PurchaseInvoiceStatusBadge } from "@/components/distribuidora/orders/PurchaseInvoiceStatusBadge"

export function PurchaseInvoiceStatusCell({
  row,
}: {
  row: PurchaseInvoiceStatusFields
}) {
  return <PurchaseInvoiceStatusBadge row={row} />
}

export function PurchaseAssociatedDocumentCell({
  row,
}: {
  row: PurchaseInvoiceStatusFields
}) {
  const label = associatedDocumentLabel(row)
  return (
    <span className="text-sm tabular-nums">
      {label ?? "—"}
    </span>
  )
}

export function PurchaseInvoiceScoreCell({
  row,
}: {
  row: PurchaseInvoiceStatusFields
}) {
  const score = displayScoreValue(row)
  return (
    <span className="text-sm tabular-nums text-muted-foreground">
      {score != null ? String(score) : "—"}
    </span>
  )
}
