"use client"

import { cn } from "@/lib/utils"
import type { PurchaseInvoiceStatusFilter } from "@/lib/purchase-invoice-status"

const CHIPS: { id: PurchaseInvoiceStatusFilter; label: string }[] = [
  { id: "all", label: "Todas" },
  { id: "pending", label: "Pendientes" },
  { id: "probable", label: "Probables" },
  { id: "confirmed", label: "Facturadas" },
]

type PreDespachoStatusChipsProps = {
  value: PurchaseInvoiceStatusFilter
  onChange: (value: PurchaseInvoiceStatusFilter) => void
  counts: Record<PurchaseInvoiceStatusFilter, number>
  disabled?: boolean
}

const EMPTY_COUNTS: Record<PurchaseInvoiceStatusFilter, number> = {
  all: 0,
  pending: 0,
  probable: 0,
  confirmed: 0,
}

export function PreDespachoStatusChips({
  value,
  onChange,
  counts,
  disabled,
}: PreDespachoStatusChipsProps) {
  const safeCounts = counts ?? EMPTY_COUNTS
  return (
    <div
      className="flex flex-wrap gap-1.5"
      role="group"
      aria-label="Filtro rápido por estado de facturación"
    >
      {CHIPS.map((chip) => {
        const active = value === chip.id
        return (
          <button
            key={chip.id}
            type="button"
            disabled={disabled}
            onClick={() => onChange(chip.id)}
            className={cn(
              "inline-flex min-h-11 items-center gap-1.5 rounded-lg border px-3.5 py-2 text-[13px] font-medium transition-colors duration-150 sm:min-h-9",
              "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
              active
                ? "border-primary/40 bg-primary/10 text-primary"
                : "border-border/80 bg-background text-muted-foreground hover:border-border hover:bg-muted/50 hover:text-foreground",
              disabled && "pointer-events-none opacity-50",
            )}
          >
            {chip.label}
            <span
              className={cn(
                "rounded px-1 py-0.5 text-[11px] tabular-nums",
                active ? "bg-primary/15" : "bg-muted/80 text-muted-foreground",
              )}
            >
              {safeCounts[chip.id] ?? 0}
            </span>
          </button>
        )
      })}
    </div>
  )
}
