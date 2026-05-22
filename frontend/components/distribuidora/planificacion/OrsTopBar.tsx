"use client"

import { Clock, Fuel, MapPinned, Users } from "lucide-react"
import { cn } from "@/lib/utils"
import { formatClp } from "@/lib/ors-map-ui"
import { Skeleton } from "@/components/ui/skeleton"

type OrsTopBarProps = {
  kmTotal: number
  clientCount: number
  durationMin: number
  fuelCostClp: number
  loading?: boolean
}

function MetricCard({
  label,
  value,
  sub,
  icon: Icon,
  loading,
}: {
  label: string
  value: string
  sub?: string
  icon: React.ElementType
  loading?: boolean
}) {
  return (
    <div
      className={cn(
        "flex min-w-[8.5rem] flex-1 flex-col gap-0.5 rounded-lg border border-border/80 bg-card/90 px-3 py-2.5 shadow-sm",
      )}
    >
      <div className="flex items-center justify-between gap-2">
        <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
          {label}
        </span>
        <Icon className="size-3.5 shrink-0 text-muted-foreground" aria-hidden />
      </div>
      {loading ? (
        <Skeleton className="h-7 w-20" />
      ) : (
        <p className="text-lg font-semibold tabular-nums tracking-tight text-foreground">
          {value}
        </p>
      )}
      {sub && !loading ? (
        <p className="text-[10px] text-muted-foreground">{sub}</p>
      ) : null}
    </div>
  )
}

export function OrsTopBar({
  kmTotal,
  clientCount,
  durationMin,
  fuelCostClp,
  loading,
}: OrsTopBarProps) {
  return (
    <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
      <MetricCard
        label="Km total"
        value={`${kmTotal.toFixed(1)} km`}
        sub="Rutas ORS"
        icon={MapPinned}
        loading={loading}
      />
      <MetricCard
        label="Clientes"
        value={clientCount.toLocaleString("es-CL")}
        sub="Únicos en cola"
        icon={Users}
        loading={loading}
      />
      <MetricCard
        label="Tiempo est."
        value={`${Math.round(durationMin)} min`}
        sub="Conducción ORS"
        icon={Clock}
        loading={loading}
      />
      <MetricCard
        label="Combustible est."
        value={formatClp(fuelCostClp)}
        sub="Referencia UI"
        icon={Fuel}
        loading={loading}
      />
    </div>
  )
}
