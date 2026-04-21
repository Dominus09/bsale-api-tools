"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import dynamic from "next/dynamic"
import Link from "next/link"
import { Loader2 } from "lucide-react"

import {
  postDistribuidoraPlanificacionOrsRoutes,
  type DistribuidoraPlanificacionOrsRoute,
} from "@/lib/api"
import {
  readPlanificacionPayload,
  clearPlanificacionPayload,
  type PlanificacionStoredOrder,
} from "@/lib/planificacion-despacho-storage"
import type { PlanificacionMapRoute } from "@/components/distribuidora/planificacion-despacho-map-client"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"

const PlanificacionMap = dynamic(
  () =>
    import("@/components/distribuidora/planificacion-despacho-map-client").then((m) => ({
      default: m.PlanificacionDespachoMapClient,
    })),
  { ssr: false, loading: () => <div className="text-sm text-muted-foreground">Cargando mapa…</div> },
)

const TRUCK_COLORS = ["#2563eb", "#16a34a", "#ca8a04", "#9333ea", "#db2777", "#0891b2"]

function lineStringToLatLngs(
  g: DistribuidoraPlanificacionOrsRoute["geometry"],
): [number, number][] {
  if (!g?.coordinates?.length) return []
  return g.coordinates.map(([lon, lat]) => [lat, lon] as [number, number])
}

function groupOrdersByTruck(orders: PlanificacionStoredOrder[]) {
  const sorted = [...orders].sort((a, b) => {
    const c = a.camion.localeCompare(b.camion, "es")
    if (c !== 0) return c
    return a.stop_index - b.stop_index
  })
  const map = new Map<string, PlanificacionStoredOrder[]>()
  for (const o of sorted) {
    const arr = map.get(o.camion)
    if (arr) arr.push(o)
    else map.set(o.camion, [o])
  }
  return map
}

export default function PlanificacionDespachoPage() {
  const [orders, setOrders] = useState<PlanificacionStoredOrder[]>([])
  const [orsRoutes, setOrsRoutes] = useState<DistribuidoraPlanificacionOrsRoute[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const reloadFromStorage = useCallback(() => {
    const p = readPlanificacionPayload()
    setOrders(p?.orders ?? [])
  }, [])

  useEffect(() => {
    reloadFromStorage()
  }, [reloadFromStorage])

  const fetchRoutes = useCallback(async (list: PlanificacionStoredOrder[]) => {
    if (list.length === 0) {
      setOrsRoutes([])
      setLoading(false)
      return
    }
    setLoading(true)
    setError(null)
    try {
      const byT = groupOrdersByTruck(list)
      const truckEntries = Array.from(byT.entries()) as [
        string,
        PlanificacionStoredOrder[],
      ][]
      const routesPayload = truckEntries.map(([camion, stops]) => ({
        camion,
        coordinates: stops.map((o: PlanificacionStoredOrder) => [o.lng, o.lat] as number[]),
      }))
      const res = await postDistribuidoraPlanificacionOrsRoutes({ routes: routesPayload })
      setOrsRoutes(res.routes)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error ORS")
      setOrsRoutes([])
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void fetchRoutes(orders)
  }, [orders, fetchRoutes])

  const mapRoutes: PlanificacionMapRoute[] = useMemo(() => {
    const byT = groupOrdersByTruck(orders)
    const truckKeys = Array.from(byT.keys())
    return orsRoutes.map((r, i) => {
      const color = TRUCK_COLORS[i % TRUCK_COLORS.length]!
      const stops = byT.get(r.camion) ?? []
      return {
        camion: r.camion,
        color,
        positions: lineStringToLatLngs(r.geometry),
        stops: stops.map((s: PlanificacionStoredOrder) => ({
          lat: s.lat,
          lng: s.lng,
          num: s.stop_index,
          label: `${s.nombre_fantasia?.trim() || "Cliente"} · OC ${s.oc ?? s.document_id}`,
        })),
      }
    })
  }, [orders, orsRoutes])

  const totals = useMemo(() => {
    let km = 0
    let min = 0
    for (const r of orsRoutes) {
      km += Number(r.distance_km) || 0
      min += Number(r.duration_min) || 0
    }
    const clients = new Set<number>()
    let amount = 0
    for (const o of orders) {
      if (o.client_id != null && Number.isFinite(Number(o.client_id))) {
        clients.add(Number(o.client_id))
      }
      amount += Number(o.total_amount ?? 0)
    }
    return {
      km,
      min,
      clientCount: clients.size,
      amount,
    }
  }, [orders, orsRoutes])

  const perTruck = useMemo(() => {
    return orsRoutes.map((r) => {
      const stops = orders.filter((o) => o.camion === r.camion)
      const amt = stops.reduce((s, o) => s + Number(o.total_amount ?? 0), 0)
      return {
        camion: r.camion,
        km: r.distance_km,
        min: r.duration_min,
        stops: stops.length,
        amount: amt,
      }
    })
  }, [orders, orsRoutes])

  if (!loading && orders.length === 0) {
    return (
      <div className="mx-auto flex max-w-lg flex-col gap-6 py-16">
        <h1 className="text-2xl font-semibold">Planificación de despacho</h1>
        <p className="text-sm text-muted-foreground">
          No hay órdenes en cola. Seleccione documentos con georreferencia y camión en la pantalla
          previa y pulse &quot;Enviar a planificación&quot;.
        </p>
        <Button asChild>
          <Link href="/distribuidora/pre-planificacion">Ir a pre‑planificación</Link>
        </Button>
      </div>
    )
  }

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-8 pb-16">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Planificación de despacho</h1>
          <p className="text-sm text-muted-foreground">
            Rutas ORS por camión, distancias y mapa con paradas numeradas.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button asChild variant="outline" size="sm">
            <Link href="/distribuidora/pre-planificacion">Volver a selección</Link>
          </Button>
          <Button
            type="button"
            variant="ghost"
            size="sm"
            onClick={() => {
              clearPlanificacionPayload()
              setOrders([])
              setOrsRoutes([])
            }}
          >
            Limpiar cola
          </Button>
        </div>
      </div>

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Loader2 className="size-4 animate-spin" />
          Calculando rutas…
        </div>
      ) : null}

      <section className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <Card className="border-0 bg-muted/30 py-4 shadow-sm">
          <CardHeader className="pb-1">
            <CardTitle className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
              Km total
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0 text-2xl font-semibold tabular-nums">
            {totals.km.toFixed(1)}
          </CardContent>
        </Card>
        <Card className="border-0 bg-muted/30 py-4 shadow-sm">
          <CardHeader className="pb-1">
            <CardTitle className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
              Tiempo conducción (min)
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0 text-2xl font-semibold tabular-nums">
            {Math.round(totals.min)}
          </CardContent>
        </Card>
        <Card className="border-0 bg-muted/30 py-4 shadow-sm">
          <CardHeader className="pb-1">
            <CardTitle className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
              Clientes únicos
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0 text-2xl font-semibold tabular-nums">
            {totals.clientCount}
          </CardContent>
        </Card>
        <Card className="border-0 bg-muted/30 py-4 shadow-sm">
          <CardHeader className="pb-1">
            <CardTitle className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
              Monto total
            </CardTitle>
          </CardHeader>
          <CardContent className="pt-0 text-xl font-semibold tabular-nums sm:text-2xl">
            {totals.amount.toLocaleString("es-CL", {
              style: "currency",
              currency: "CLP",
              maximumFractionDigits: 0,
            })}
          </CardContent>
        </Card>
      </section>

      <PlanificacionMap routes={mapRoutes} />

      <section className="space-y-3">
        <h2 className="text-sm font-semibold uppercase tracking-wide text-muted-foreground">
          Resumen por camión
        </h2>
        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {perTruck.map((t, i) => (
            <Card
              key={t.camion}
              className="border-l-4 py-3 shadow-sm"
              style={{ borderLeftColor: TRUCK_COLORS[i % TRUCK_COLORS.length] }}
            >
              <CardHeader className="pb-2">
                <CardTitle className="text-base">{t.camion}</CardTitle>
              </CardHeader>
              <CardContent className="space-y-1 text-sm text-muted-foreground">
                <p>
                  <span className="text-foreground">{t.km.toFixed(1)}</span> km ·{" "}
                  <span className="text-foreground">{Math.round(t.min)}</span> min
                </p>
                <p>
                  Paradas: <span className="font-medium text-foreground">{t.stops}</span>
                </p>
                <p className="tabular-nums font-medium text-foreground">
                  {t.amount.toLocaleString("es-CL", {
                    style: "currency",
                    currency: "CLP",
                    maximumFractionDigits: 0,
                  })}
                </p>
              </CardContent>
            </Card>
          ))}
        </div>
      </section>
    </div>
  )
}
