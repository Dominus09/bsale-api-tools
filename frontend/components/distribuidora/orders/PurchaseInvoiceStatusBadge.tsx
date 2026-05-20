"use client"

import { Badge } from "@/components/ui/badge"
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import {
  type PurchaseInvoiceStatusFields,
  purchaseStatusBadgeClass,
  purchaseStatusBadgeLabel,
  purchaseStatusTooltip,
  resolvePurchaseStatusCode,
} from "@/lib/purchase-invoice-status"

export type { PurchaseInvoiceStatusFields }

export function PurchaseInvoiceStatusBadge({
  row,
}: {
  row: PurchaseInvoiceStatusFields
}) {
  const code = resolvePurchaseStatusCode(row)
  const label = purchaseStatusBadgeLabel(code)
  const className = purchaseStatusBadgeClass(code)
  const tooltip = purchaseStatusTooltip(code)

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="inline-flex cursor-help">
          <Badge variant="outline" className={className}>
            {label}
          </Badge>
        </span>
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-xs">
        {tooltip}
      </TooltipContent>
    </Tooltip>
  )
}
