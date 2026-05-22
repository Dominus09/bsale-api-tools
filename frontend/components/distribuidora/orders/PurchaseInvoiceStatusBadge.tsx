"use client"

import { Badge } from "@/components/ui/badge"
import { cn } from "@/lib/utils"
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

const COMPACT_STATUS_LABEL: Partial<Record<string, string>> = {
  FACTURADA_CONFIRMADA: "Fact.",
  PROBABLE_FACTURADA_HIGH: "Prob.",
  PROBABLE_FACTURADA_MEDIUM: "Prob.",
  PROBABLE_FACTURADA_LOW: "Prob.",
  PENDIENTE: "Pend.",
}

export function PurchaseInvoiceStatusBadge({
  row,
  compact,
}: {
  row: PurchaseInvoiceStatusFields
  compact?: boolean
}) {
  const code = resolvePurchaseStatusCode(row)
  const label = compact
    ? (COMPACT_STATUS_LABEL[code] ?? purchaseStatusBadgeLabel(code))
    : purchaseStatusBadgeLabel(code)
  const className = purchaseStatusBadgeClass(code)
  const tooltip = purchaseStatusTooltip(code)

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="inline-flex cursor-help">
          <Badge
            variant="outline"
            className={cn(
              className,
              compact && "px-1 py-0 text-[9px] font-medium leading-4",
            )}
          >
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
