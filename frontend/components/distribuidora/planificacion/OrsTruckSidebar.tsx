"use client"

import { Truck } from "lucide-react"

import type { DispatchPlanSummary } from "@/lib/api"
import { cn } from "@/lib/utils"
import { Badge } from "@/components/ui/badge"

const STATUS_LABEL: Record<string, string> = {
  draft: "Borrador",
  planned: "Planificado",
  invoicing: "Facturando",
  ready_for_picking: "Listo picking",
  picking_generated: "Picking OK",
  dispatched: "Despachado",
}

type TruckRow = {
  camion: string
  truckId: number
  stopCount: number
  plan?: DispatchPlanSummary | null
}

type OrsTruckSidebarProps = {
  trucks: TruckRow[]
  selectedCamion: string | null
  onSelect: (camion: string) => void
  loading?: boolean
}

export function OrsTruckSidebar({
  trucks,
  selectedCamion,
  onSelect,
  loading,
}: OrsTruckSidebarProps) {
  return (
    <div className="flex h-full min-h-0 w-44 shrink-0 flex-col border-r border-border/80 bg-muted/10 md:w-48">
      <div className="border-b border-border/70 px-3 py-2.5">
        <p className="flex items-center gap-1.5 text-xs font-semibold text-foreground">
          <Truck className="size-3.5 text-primary" aria-hidden />
          Camiones / rutas
        </p>
        <p className="mt-0.5 text-[10px] text-muted-foreground">
          Un camión a la vez en mapa y costos
        </p>
      </div>
      <ul className="min-h-0 flex-1 space-y-1 overflow-y-auto p-2">
        {trucks.map((t) => {
          const active = t.camion === selectedCamion
          const st = t.plan?.status
          return (
            <li key={t.camion}>
              <button
                type="button"
                disabled={loading}
                onClick={() => onSelect(t.camion)}
                className={cn(
                  "w-full rounded-lg border px-2.5 py-2 text-left transition-colors",
                  active
                    ? "border-primary/50 bg-primary/10 shadow-sm"
                    : "border-border/60 bg-card/80 hover:bg-muted/50",
                )}
              >
                <p className="truncate text-xs font-medium">{t.camion}</p>
                <p className="mt-0.5 text-[10px] text-muted-foreground">
                  {t.stopCount} parada{t.stopCount === 1 ? "" : "s"}
                </p>
                {st ? (
                  <Badge variant="secondary" className="mt-1.5 h-5 px-1.5 text-[9px]">
                    {STATUS_LABEL[st] ?? st}
                  </Badge>
                ) : (
                  <Badge variant="outline" className="mt-1.5 h-5 px-1.5 text-[9px]">
                    Sin confirmar
                  </Badge>
                )}
              </button>
            </li>
          )
        })}
      </ul>
    </div>
  )
}
