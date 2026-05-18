"use client"

import { AlertTriangle, CheckCircle2, Clock, MapPin, RefreshCw, Users } from "lucide-react"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import type { OperacionesDashboardKpis } from "@/services/operaciones"

function KpiCard({
  title,
  value,
  sub,
  icon: Icon,
}: {
  title: string
  value: string | number
  sub?: string
  icon: React.ElementType
}) {
  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">{title}</CardTitle>
        <Icon className="h-4 w-4 text-muted-foreground" />
      </CardHeader>
      <CardContent>
        <div className="text-2xl font-bold tabular-nums">{value}</div>
        {sub ? <p className="mt-1 text-xs text-muted-foreground">{sub}</p> : null}
      </CardContent>
    </Card>
  )
}

export function OperacionesKpiCards({ kpis }: { kpis: OperacionesDashboardKpis }) {
  const syncLabel = kpis.ultima_sincronizacion
    ? new Date(kpis.ultima_sincronizacion).toLocaleString("es-CL")
    : "Sin datos"

  return (
    <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
      <KpiCard title="Clientes en ruta" value={kpis.total_clientes} sub={`${kpis.clientes_visitados} visitados`} icon={Users} />
      <KpiCard title="Pendientes" value={kpis.clientes_pendientes} icon={Clock} />
      <KpiCard title="Incidencias" value={kpis.incidencias} icon={AlertTriangle} />
      <KpiCard
        title="Cumplimiento"
        value={`${kpis.porcentaje_cumplimiento.toFixed(1)}%`}
        sub={`${kpis.vendedores_activos}/${kpis.vendedores_total} vendedores activos`}
        icon={CheckCircle2}
      />
      <KpiCard title="Km recorridos" value={kpis.kilometros_recorridos.toFixed(1)} icon={MapPin} />
      <KpiCard title="Sync pendiente" value={kpis.visitas_pending_sync} icon={RefreshCw} />
      <KpiCard title="Última sync" value="—" sub={syncLabel} icon={RefreshCw} />
    </div>
  )
}
