"use client"

import { Banknote, Clock, Droplets, Fuel, MapPinned, Users } from "lucide-react"
import { cn } from "@/lib/utils"
import { formatClp } from "@/lib/ors-map-ui"
import { Skeleton } from "@/components/ui/skeleton"

type OrsTopBarProps = {
  kmTotal: number
  clientCount: number
  durationMin: number
  litersEstimated: number
  fuelCostClp: number
  crewCostClp?: number
  totalRouteCostClp?: number
  dieselPricePerLiter?: number
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
        "flex min-w-[7.5rem] flex-1 flex-col gap-0.5 rounded-lg border border-border/80 bg-card/90 px-3 py-2.5 shadow-sm",
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
  litersEstimated,
  fuelCostClp,
  crewCostClp = 0,
  totalRouteCostClp,
  dieselPricePerLiter,
  loading,
}: OrsTopBarProps) {
  const totalClp = totalRouteCostClp ?? fuelCostClp + crewCostClp
  const dieselSub =
    dieselPricePerLiter != null
      ? `Diesel ${Math.round(dieselPricePerLiter).toLocaleString("es-CL")} CLP/L`
      : "Bodega ida y vuelta"

  return (
    <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
      <MetricCard
        label="Km total"
        value={`${kmTotal.toFixed(1)} km`}
        sub="ORS con retorno bodega"
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
        label="Tiempo total"
        value={`${Math.round(durationMin)} min`}
        sub="Conducción ORS"
        icon={Clock}
        loading={loading}
      />
      <MetricCard
        label="Litros est."
        value={`${litersEstimated.toFixed(1)} L`}
        sub="Suma por rendimiento BD"
        icon={Droplets}
        loading={loading}
      />
      <MetricCard
        label="Costo combustible"
        value={formatClp(fuelCostClp)}
        sub={dieselSub}
        icon={Fuel}
        loading={loading}
      />
      <MetricCard
        label="Personal"
        value={formatClp(crewCostClp)}
        sub="Chofer + peoneta / vuelta"
        icon={Users}
        loading={loading}
      />
      <MetricCard
        label="Costo total ruta"
        value={formatClp(totalClp)}
        sub="Combustible + personal"
        icon={Banknote}
        loading={loading}
      />
    </div>
  )
}
