"use client"

import { Loader2 } from "lucide-react"
import type { PromotionGridRow } from "@/lib/api"
import { PromotionCard } from "@/components/promotions/promotion-card"

type PromotionActiveGridProps = {
  rows: PromotionGridRow[]
  loading: boolean
  companyNameById: Map<number, string>
  onOpen: (row: PromotionGridRow) => void
  onEdit: (row: PromotionGridRow) => void
  onDuplicate: (row: PromotionGridRow) => void
  onLabels: (row: PromotionGridRow) => void
}

export function PromotionActiveGrid({
  rows,
  loading,
  companyNameById,
  onOpen,
  onEdit,
  onDuplicate,
  onLabels,
}: PromotionActiveGridProps) {
  if (loading) {
    return (
      <div className="flex items-center justify-center gap-2 py-24 text-muted-foreground">
        <Loader2 className="h-6 w-6 animate-spin" />
        <span>Cargando promociones…</span>
      </div>
    )
  }

  if (rows.length === 0) {
    return (
      <div className="rounded-xl border border-dashed py-20 text-center">
        <p className="text-muted-foreground text-sm">No hay promociones activas con estos filtros.</p>
      </div>
    )
  }

  return (
    <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 2xl:grid-cols-5">
      {rows.map((row) => (
        <PromotionCard
          key={`${row.snapshot_id}-${row.promotion_id}`}
          row={row}
          companyName={companyNameById.get(row.company_id) ?? `Empresa ${row.company_id}`}
          onOpen={onOpen}
          onEdit={onEdit}
          onDuplicate={onDuplicate}
          onLabels={onLabels}
        />
      ))}
    </div>
  )
}
