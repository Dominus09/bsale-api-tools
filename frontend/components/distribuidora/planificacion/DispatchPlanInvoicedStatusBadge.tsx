"use client"

import { Badge } from "@/components/ui/badge"
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import {
  type DispatchInvoicingRowFields,
  dispatchInvoicingBadgeClass,
  dispatchInvoicingBadgeLabel,
} from "@/lib/purchase-invoice-status"
import { cn } from "@/lib/utils"

export function DispatchPlanInvoicedStatusBadge({
  row,
  compact,
}: {
  row: DispatchInvoicingRowFields
  compact?: boolean
}) {
  const label = dispatchInvoicingBadgeLabel(row)
  const className = dispatchInvoicingBadgeClass(row)
  const isAuto =
    row.relation_source === "auto_match" || row.is_auto_confirmed === true

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="inline-flex cursor-help">
          <Badge
            variant="outline"
            className={cn(
              className,
              isAuto && "border-sky-300 bg-sky-50 text-sky-900 dark:border-sky-800 dark:bg-sky-950/50 dark:text-sky-100",
              compact && "px-1 py-0 text-[9px] font-medium leading-4",
            )}
          >
            {label}
          </Badge>
        </span>
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-xs text-xs">
        {isAuto
          ? "Auto-confirmada operacionalmente (score ≥75). relation_source=auto_match. La coincidencia probable se conserva en los datos."
          : row.relation_source === "relateddetailid"
            ? "Confirmada en Bsale (document_related)."
            : row.status === "probable"
              ? "Coincidencia probable (score 60–74)."
              : "Sin facturación confirmada."}
      </TooltipContent>
    </Tooltip>
  )
}
