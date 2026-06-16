"use client"

import { cn } from "@/lib/utils"
import {
  estadoDotClass,
  estadoVisualClass,
  mapEstadoVisual,
  type PromotionEstadoVisual,
} from "@/lib/promotions-utils"

export function PromotionStatusBadge({
  estado,
  className,
}: {
  estado: string
  className?: string
}) {
  const visual = mapEstadoVisual(estado)
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 rounded-full border px-2.5 py-0.5 text-xs font-medium",
        estadoVisualClass(visual),
        className,
      )}
    >
      <span className={cn("h-2 w-2 shrink-0 rounded-full", estadoDotClass(visual))} />
      {visual}
    </span>
  )
}

export function PromotionTipoBadge({
  tipo,
  className,
}: {
  tipo: string
  className?: string
}) {
  const isRemate = tipo.toLowerCase() === "remate"
  return (
    <span
      className={cn(
        "rounded-md px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide",
        isRemate
          ? "bg-orange-100 text-orange-800"
          : "bg-sky-100 text-sky-800",
        className,
      )}
    >
      {isRemate ? "Remate" : "Oferta"}
    </span>
  )
}

export function PromotionDiscountBadge({
  label,
  className,
}: {
  label: string
  className?: string
}) {
  if (label === "—") return null
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-full bg-rose-100 px-2.5 py-0.5 text-xs font-bold text-rose-800",
        className,
      )}
    >
      {label}
    </span>
  )
}

export type { PromotionEstadoVisual }
