"use client"

import { cn } from "@/lib/utils"
import { QUICK_COMPANY_CHIPS } from "@/lib/promotion-labels-bridge"

type PromotionCompanyChipsProps = {
  filterCompanyId: string
  companies: { company_id: number; name: string }[]
  onSelect: (companyId: string) => void
}

export function PromotionCompanyChips({
  filterCompanyId,
  companies,
  onSelect,
}: PromotionCompanyChipsProps) {
  return (
    <div className="flex flex-wrap items-center gap-2">
      <span className="text-muted-foreground text-xs font-medium">Empresa:</span>
      {QUICK_COMPANY_CHIPS.map((chip) => {
        const id =
          chip.match === null
            ? "all"
            : String(
                companies.find((c) =>
                  c.name.trim().toLowerCase().includes(chip.match!),
                )?.company_id ?? "all",
              )
        const active = filterCompanyId === id || (chip.match === null && filterCompanyId === "all")
        if (chip.match !== null && id === "all") return null
        return (
          <button
            key={chip.label}
            type="button"
            onClick={() => onSelect(id)}
            className={cn(
              "rounded-full border px-3 py-1 text-xs font-medium transition-colors",
              active
                ? "border-primary bg-primary text-primary-foreground"
                : "bg-background hover:bg-muted border-border text-foreground",
            )}
          >
            {chip.label}
          </button>
        )
      })}
    </div>
  )
}
