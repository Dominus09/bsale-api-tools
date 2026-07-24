"use client"

import { Badge } from "@/components/ui/badge"
import {
  AGE_BUCKET_LABEL,
  GROSS_COST_QUALITY_LABEL,
  TAX_BREAKDOWN_LABEL,
  type AgeBucketKind,
  type GrossCostQualityKind,
  type TaxBreakdownKind,
} from "@/lib/costos/quality-labels"
import { cn } from "@/lib/utils"

const grossClass: Record<GrossCostQualityKind, string> = {
  actual_purchase_gross:
    "bg-emerald-100 text-emerald-900 dark:bg-emerald-950 dark:text-emerald-200",
  reconstructed_from_actual_taxes:
    "bg-sky-100 text-sky-900 dark:bg-sky-950 dark:text-sky-200",
  current_cost_fallback:
    "bg-amber-100 text-amber-900 dark:bg-amber-950 dark:text-amber-200",
  missing_gross_cost: "bg-muted text-muted-foreground",
  conflicting_gross_cost:
    "bg-red-100 text-red-900 dark:bg-red-950 dark:text-red-200",
}

const ageClass: Record<AgeBucketKind, string> = {
  "0_30": "bg-emerald-100 text-emerald-900 dark:bg-emerald-950 dark:text-emerald-200",
  "31_60": "bg-sky-100 text-sky-900 dark:bg-sky-950 dark:text-sky-200",
  "61_90": "bg-amber-100 text-amber-900 dark:bg-amber-950 dark:text-amber-200",
  "90_plus": "bg-red-100 text-red-900 dark:bg-red-950 dark:text-red-200",
  unknown: "bg-muted text-muted-foreground",
}

export function CostQualityBadge({
  kind,
  className,
}: {
  kind: GrossCostQualityKind
  className?: string
}) {
  return (
    <Badge
      variant="secondary"
      className={cn("font-normal", grossClass[kind], className)}
    >
      {GROSS_COST_QUALITY_LABEL[kind]}
    </Badge>
  )
}

export function CostAgeBadge({
  kind,
  className,
}: {
  kind: AgeBucketKind
  className?: string
}) {
  return (
    <Badge variant="secondary" className={cn("font-normal", ageClass[kind], className)}>
      {AGE_BUCKET_LABEL[kind]}
    </Badge>
  )
}

export function CostTaxBadge({
  kind,
  className,
}: {
  kind: TaxBreakdownKind
  className?: string
}) {
  return (
    <Badge variant="outline" className={cn("font-normal", className)}>
      {TAX_BREAKDOWN_LABEL[kind]}
    </Badge>
  )
}
