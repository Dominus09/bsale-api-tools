"use client"

import {
  countCommercialSemaphores,
  SEMAPHORE_EMOJI,
  type CommercialSemaphoreCounts,
} from "@/lib/ors-commercial-semaphore"
import { cn } from "@/lib/utils"
import { Skeleton } from "@/components/ui/skeleton"

export type CommercialKpiCounts = CommercialSemaphoreCounts & {
  isolated: number
}

const ISOLATED_EMOJI = "⚫"

type OrsCommercialKpiStripProps = {
  counts: CommercialKpiCounts
  loading?: boolean
  className?: string
}

function Chip({
  emoji,
  count,
  label,
}: {
  emoji: string
  count: number
  label: string
}) {
  return (
    <span
      className="inline-flex items-center gap-1 rounded-md border border-border/70 bg-background/90 px-2 py-1 text-xs tabular-nums"
      title={label}
    >
      <span aria-hidden>{emoji}</span>
      <span className="font-semibold text-foreground">{count}</span>
    </span>
  )
}

export function OrsCommercialKpiStrip({
  counts,
  loading,
  className,
}: OrsCommercialKpiStripProps) {
  if (loading) {
    return (
      <div className={cn("flex flex-wrap gap-2", className)}>
        <Skeleton className="h-7 w-14" />
        <Skeleton className="h-7 w-14" />
        <Skeleton className="h-7 w-14" />
      </div>
    )
  }

  return (
    <div
      className={cn("flex flex-wrap items-center gap-2", className)}
      aria-label="Semáforo comercial de clientes"
    >
      <Chip emoji={SEMAPHORE_EMOJI.green} count={counts.green} label="Clientes verdes" />
      <Chip emoji={SEMAPHORE_EMOJI.yellow} count={counts.yellow} label="Clientes amarillos" />
      <Chip emoji={SEMAPHORE_EMOJI.red} count={counts.red} label="Clientes rojos" />
      {counts.isolated > 0 ? (
        <Chip emoji={ISOLATED_EMOJI} count={counts.isolated} label="Clientes aislados" />
      ) : null}
    </div>
  )
}

export { countCommercialSemaphores }
