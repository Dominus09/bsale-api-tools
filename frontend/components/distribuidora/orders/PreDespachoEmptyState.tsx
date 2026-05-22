"use client"

import { ClipboardList, Filter, Inbox } from "lucide-react"
import { cn } from "@/lib/utils"

type PreDespachoEmptyStateProps = {
  variant: "no-data" | "filtered-out"
  className?: string
}

export function PreDespachoEmptyState({
  variant,
  className,
}: PreDespachoEmptyStateProps) {
  const Icon = variant === "filtered-out" ? Filter : Inbox
  const title =
    variant === "filtered-out"
      ? "Sin órdenes para este filtro"
      : "Sin órdenes en el rango"
  const description =
    variant === "filtered-out"
      ? "Pruebe otro estado o amplíe el rango de fechas."
      : "Ajuste fechas, sincronice desde Bsale o desactive «Solo no facturadas»."

  return (
    <div
      className={cn(
        "flex flex-col items-center justify-center gap-3 rounded-lg border border-dashed border-border/80 bg-muted/20 px-6 py-14 text-center",
        className,
      )}
    >
      <div className="flex size-12 items-center justify-center rounded-full bg-muted/60">
        <Icon className="size-6 text-muted-foreground" aria-hidden />
      </div>
      <div className="space-y-1">
        <p className="text-sm font-medium text-foreground">{title}</p>
        <p className="max-w-sm text-xs leading-relaxed text-muted-foreground">
          {description}
        </p>
      </div>
      <ClipboardList
        className="size-4 text-muted-foreground/40"
        aria-hidden
      />
    </div>
  )
}
