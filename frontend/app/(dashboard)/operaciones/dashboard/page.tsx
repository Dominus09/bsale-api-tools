"use client"

import { useCallback, useMemo, useState } from "react"
import { Loader2, RefreshCw } from "lucide-react"

import { OperacionesKpiCards } from "@/components/operaciones/kpi-cards"
import { VendedoresOperacionesTable } from "@/components/operaciones/vendedores-table"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { useOperacionesPoll } from "@/hooks/use-operaciones-poll"
import { getOperacionesDashboard, localIsoDate } from "@/services/operaciones"

export default function OperacionesDashboardPage() {
  const [fecha, setFecha] = useState(localIsoDate())
  const loader = useCallback(() => getOperacionesDashboard(fecha), [fecha])
  const { data, loading, error, refresh } = useOperacionesPoll(loader, [fecha])

  const subtitle = useMemo(
    () => `Actualización automática · ${new Date().toLocaleTimeString("es-CL")}`,
    [data],
  )

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Operaciones Quillotana</h1>
          <p className="text-sm text-muted-foreground">Monitoreo en vivo de vendedores y rutas (app móvil)</p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Input type="date" value={fecha} onChange={(e) => setFecha(e.target.value)} className="w-[160px]" />
          <Button variant="outline" size="sm" onClick={() => void refresh()} disabled={loading}>
            {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
            <span className="ml-2 hidden sm:inline">Actualizar</span>
          </Button>
        </div>
      </div>

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {data?.kpis ? <OperacionesKpiCards kpis={data.kpis} /> : loading ? <div className="h-32 animate-pulse rounded-xl bg-muted" /> : null}

      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle>Vendedores hoy</CardTitle>
          <span className="text-xs text-muted-foreground">{subtitle}</span>
        </CardHeader>
        <CardContent>
          {loading && !data ? (
            <div className="flex justify-center py-12">
              <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
          ) : (
            <VendedoresOperacionesTable items={data?.vendedores_resumen ?? []} />
          )}
        </CardContent>
      </Card>
    </div>
  )
}
