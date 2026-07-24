"use client"

import { Badge } from "@/components/ui/badge"
import {
  PRICE_POLICY_STATUS_LABEL,
  type PricePolicyStatus,
} from "@/lib/margins/price-policy"
import { cn } from "@/lib/utils"

const STATUS_CLASS: Record<PricePolicyStatus, string> = {
  below_minimum: "bg-red-500/15 text-red-800 dark:text-red-200 border-red-500/40",
  within_policy: "bg-emerald-500/15 text-emerald-800 dark:text-emerald-200 border-emerald-500/40",
  above_maximum: "bg-amber-500/15 text-amber-900 dark:text-amber-200 border-amber-500/40",
  missing_rule: "bg-slate-500/15 text-slate-800 dark:text-slate-200 border-slate-500/40",
  missing_cost: "bg-orange-500/15 text-orange-900 dark:text-orange-200 border-orange-500/40",
  missing_price: "bg-muted text-muted-foreground border-border",
  stale_cost: "bg-yellow-500/15 text-yellow-900 dark:text-yellow-200 border-yellow-500/40",
  conflicting_cost: "bg-violet-500/15 text-violet-900 dark:text-violet-200 border-violet-500/40",
  cost_outlier: "bg-fuchsia-500/15 text-fuchsia-900 dark:text-fuchsia-200 border-fuchsia-500/40",
}

export function PriceControlStatusBadge({ status }: { status: PricePolicyStatus }) {
  return (
    <Badge variant="outline" className={cn("text-xs font-medium", STATUS_CLASS[status])}>
      {PRICE_POLICY_STATUS_LABEL[status]}
    </Badge>
  )
}

export function rowStatusClass(status: PricePolicyStatus): string {
  if (status === "below_minimum") return "bg-red-500/[0.06]"
  if (status === "above_maximum" || status === "stale_cost") return "bg-amber-500/[0.06]"
  if (status === "cost_outlier" || status === "conflicting_cost") return "bg-violet-500/[0.05]"
  return ""
}
