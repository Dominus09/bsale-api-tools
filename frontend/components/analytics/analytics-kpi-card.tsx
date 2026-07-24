"use client"

import type { ReactNode } from "react"
import { Info } from "lucide-react"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Skeleton } from "@/components/ui/skeleton"
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { cn } from "@/lib/utils"

export function AnalyticsKpiCard({
  title,
  value,
  subtitle,
  tooltip,
  delta,
  loading,
  className,
}: {
  title: string
  value: ReactNode
  subtitle?: string
  tooltip?: string
  delta?: string | null
  loading?: boolean
  className?: string
}) {
  return (
    <Card className={cn("shadow-none", className)}>
      <CardHeader className="flex flex-row items-start justify-between space-y-0 pb-2">
        <CardTitle className="text-xs font-medium text-muted-foreground">{title}</CardTitle>
        {tooltip ? (
          <TooltipProvider delayDuration={200}>
            <Tooltip>
              <TooltipTrigger asChild>
                <button
                  type="button"
                  className="rounded p-0.5 text-muted-foreground outline-none ring-offset-background focus-visible:ring-2 focus-visible:ring-ring"
                  aria-label={`Info: ${title}`}
                >
                  <Info className="h-3.5 w-3.5" />
                </button>
              </TooltipTrigger>
              <TooltipContent className="max-w-xs text-xs">{tooltip}</TooltipContent>
            </Tooltip>
          </TooltipProvider>
        ) : null}
      </CardHeader>
      <CardContent>
        {loading ? (
          <Skeleton className="h-7 w-20" />
        ) : (
          <div className="text-xl font-semibold tabular-nums tracking-tight sm:text-2xl">
            {value}
          </div>
        )}
        {subtitle ? (
          <p className="mt-1 text-[11px] text-muted-foreground">{subtitle}</p>
        ) : null}
        {delta ? (
          <p className="mt-0.5 text-[11px] text-muted-foreground">{delta}</p>
        ) : null}
      </CardContent>
    </Card>
  )
}
