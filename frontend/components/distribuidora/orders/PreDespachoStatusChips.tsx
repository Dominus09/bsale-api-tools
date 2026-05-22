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

export function PreDespachoStatusChips({
  value,
  onChange,
  counts,
  disabled,
}: PreDespachoStatusChipsProps) {
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
              "inline-flex items-center gap-1.5 rounded-md border px-3 py-1.5 text-xs font-medium transition-colors duration-150",
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
                "rounded px-1 py-0.5 text-[10px] tabular-nums",
                active ? "bg-primary/15" : "bg-muted/80 text-muted-foreground",
              )}
            >
              {counts[chip.id]}
            </span>
          </button>
        )
      })}
    </div>
  )
}
