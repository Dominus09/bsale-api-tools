"use client"

import {
  type PurchaseInvoiceStatusFields,
  associatedDocumentLabel,
  displayScoreValue,
} from "@/lib/purchase-invoice-status"
import { PurchaseInvoiceStatusBadge } from "@/components/distribuidora/orders/PurchaseInvoiceStatusBadge"

export function PurchaseInvoiceStatusCell({
  row,
  compact,
}: {
  row: PurchaseInvoiceStatusFields
  compact?: boolean
}) {
  return <PurchaseInvoiceStatusBadge row={row} compact={compact} />
}

export function PurchaseAssociatedDocumentCell({
  row,
  compact,
}: {
  row: PurchaseInvoiceStatusFields
  compact?: boolean
}) {
  const label = associatedDocumentLabel(row)
  return (
    <span
      className={compact ? "text-[10px] tabular-nums" : "text-sm tabular-nums"}
    >
      {label ?? "—"}
    </span>
  )
}

export function PurchaseInvoiceScoreCell({
  row,
  compact,
}: {
  row: PurchaseInvoiceStatusFields
  compact?: boolean
}) {
  const score = displayScoreValue(row)
  return (
    <span
      className={
        compact
          ? "text-[10px] tabular-nums text-muted-foreground"
          : "text-sm tabular-nums text-muted-foreground"
      }
    >
      {score != null ? String(score) : "—"}
    </span>
  )
}
