"use client"

import { CostV2InfoHint } from "@/components/costos-v2/cost-v2-info-hint"
import { Badge } from "@/components/ui/badge"
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { statusLabel, statusShortHelp } from "@/lib/costos-v2/labels"
import { cn } from "@/lib/utils"

const statusClass: Record<string, string> = {
  valid_gross:
    "bg-emerald-100 text-emerald-900 dark:bg-emerald-950 dark:text-emerald-200",
  missing_taxes_in_gross:
    "bg-amber-100 text-amber-950 dark:bg-amber-950 dark:text-amber-100",
  incomplete_tax_context:
    "bg-slate-200 text-slate-800 dark:bg-slate-800 dark:text-slate-100",
  missing_cost: "bg-muted text-muted-foreground",
  gross_component_mismatch:
    "bg-orange-100 text-orange-950 dark:bg-orange-950 dark:text-orange-100",
  duplicated_taxes_in_gross:
    "bg-rose-100 text-rose-950 dark:bg-rose-950 dark:text-rose-100",
}

export function CostV2StatusBadge({
  status,
  className,
  showHelp = true,
}: {
  status: string | null | undefined
  className?: string
  /** Tooltip con explicación en español (nunca el código técnico). */
  showHelp?: boolean
}) {
  const label = statusLabel(status)
  const help = statusShortHelp(status)
  const code = status || "unknown"

  const badge = (
    <Badge
      variant="secondary"
      className={cn(
        "font-normal",
        statusClass[code] ?? "bg-muted text-muted-foreground",
        className,
      )}
    >
      {label}
    </Badge>
  )

  if (!showHelp) return badge

  return (
    <span className="inline-flex items-center gap-1">
      <TooltipProvider delayDuration={150}>
        <Tooltip>
          <TooltipTrigger asChild>
            <span className="inline-flex cursor-default">{badge}</span>
          </TooltipTrigger>
          <TooltipContent className="max-w-xs text-xs leading-relaxed">
            <p className="font-medium">{label}</p>
            <p className="mt-0.5">{help}</p>
          </TooltipContent>
        </Tooltip>
      </TooltipProvider>
      <CostV2InfoHint title={label} text={help} />
    </span>
  )
}
