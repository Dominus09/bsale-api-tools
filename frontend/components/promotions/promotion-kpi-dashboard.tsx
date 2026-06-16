"use client"

import { Card, CardContent } from "@/components/ui/card"
import type { PromotionKpis } from "@/lib/promotions-utils"

type PromotionKpiDashboardProps = {
  kpis: PromotionKpis
  loading?: boolean
}

const ITEMS: {
  key: keyof PromotionKpis
  label: string
  accent: string
}[] = [
  { key: "activas", label: "Activas", accent: "text-emerald-700" },
  { key: "proximas", label: "Próximas", accent: "text-sky-700" },
  { key: "vencidas", label: "Vencidas", accent: "text-zinc-600" },
  { key: "remates", label: "Remates", accent: "text-orange-700" },
  { key: "empresas", label: "Empresas", accent: "text-foreground" },
]

export function PromotionKpiDashboard({ kpis, loading }: PromotionKpiDashboardProps) {
  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
      {ITEMS.map(({ key, label, accent }) => (
        <Card key={key} className="shadow-sm">
          <CardContent className="p-4">
            <p className="text-muted-foreground text-xs font-medium">{label}</p>
            <p className={`mt-1 text-2xl font-bold tabular-nums ${accent}`}>
              {loading ? "—" : kpis[key]}
            </p>
          </CardContent>
        </Card>
      ))}
    </div>
  )
}
