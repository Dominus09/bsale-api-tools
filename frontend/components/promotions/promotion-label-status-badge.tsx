"use client"

import { cn } from "@/lib/utils"

export function PromotionLabelStatusBadge({
  generated,
  className,
}: {
  generated?: boolean
  className?: string
}) {
  if (generated) {
    return (
      <span
        className={cn(
          "inline-flex items-center rounded-full border border-emerald-200 bg-emerald-50 px-2 py-0.5 text-[10px] font-medium text-emerald-800",
          className,
        )}
      >
        Con etiqueta
      </span>
    )
  }
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full border border-zinc-200 bg-zinc-50 px-2 py-0.5 text-[10px] font-medium text-zinc-600",
        className,
      )}
    >
      Sin etiqueta
    </span>
  )
}
