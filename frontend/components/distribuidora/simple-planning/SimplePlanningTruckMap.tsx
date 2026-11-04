"use client"

import dynamic from "next/dynamic"
import { useMemo, useState } from "react"
import { Loader2, Map as MapIcon, Route } from "lucide-react"

import {
  postSimplePlanningCalculateRoute,
  type SimplePlanningCalculateRouteResult,
  type SimplePlanningOrsSnapshot,
  type SimplePlanningPreviewItem,
} from "@/lib/api"
import { formatClp } from "@/lib/ors-map-ui"
import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"
import type {
  PlanificacionMapRoute,
  PlanificacionMapStop,
} from "@/components/distribuidora/planificacion-despacho-map-client"

const PlanificacionDespachoMapClient = dynamic(
  () =>
    import("@/components/distribuidora/planificacion-despacho-map-client").then(
      (m) => m.PlanificacionDespachoMapClient,
    ),
  {
    ssr: false,
    loading: () => (
      <div className="flex h-[360px] items-center justify-center rounded-lg border border-border/60 text-sm text-muted-foreground">
        <Loader2 className="mr-2 size-4 animate-spin" /> Cargando mapa…
      </div>
    ),
  },
)

const ROUTE_COLOR = "#38bdf8"

function lineStringToLatLngs(
  g: { coordinates?: [number, number][] } | null | undefined,
): [number, number][] {
  if (!g?.coordinates?.length) return []
  return g.coordinates.map(([lon, lat]) => [lat, lon] as [number, number])
}

type ConsolidatedStop = {
  key: string
  client_id: number | null
  client_name: string
  address: string | null
  municipality: string | null
  lat: number | null
  lng: number | null
  has_georef: boolean
  document_ids: number[]
  folios: number[]
}

function consolidateOrders(orders: SimplePlanningPreviewItem[]): ConsolidatedStop[] {
  const out: ConsolidatedStop[] = []
  const seen = new Map<string, number>()
  for (const o of orders) {
    const c = o.client || {}
    const clientId = c.client_id ?? null
    const address = (c.address || "").trim()
    const key = `${clientId ?? "x"}|${address.toLowerCase()}`
    const folio = o.folio != null ? Number(o.folio) : null
    const docId = o.document_id != null ? Number(o.document_id) : null
    const lat = c.lat != null && Number.isFinite(Number(c.lat)) ? Number(c.lat) : null
    const lng = c.lng != null && Number.isFinite(Number(c.lng)) ? Number(c.lng) : null
    const hasGeo = Boolean(c.has_georef) && lat != null && lng != null
    const existingIdx = seen.get(key)
    if (existingIdx != null) {
      const stop = out[existingIdx]
      if (docId != null && !stop.document_ids.includes(docId)) stop.document_ids.push(docId)
      if (folio != null && !stop.folios.includes(folio)) stop.folios.push(folio)
      continue
    }
    seen.set(key, out.length)
    out.push({
      key,
      client_id: clientId,
      client_name: c.client_name || "Cliente",
      address: c.address || null,
      municipality: c.municipality || null,
      lat,
      lng,
      has_georef: hasGeo,
      document_ids: docId != null ? [docId] : [],
      folios: folio != null ? [folio] : [],
    })
  }
  return out
}

type Props = {
  companyId: number
  officeId: number
  truckId: number
  truckName: string
  orders: SimplePlanningPreviewItem[]
  /** Snapshot ORS válido; null si se invalidó. */
  orsSnapshot: SimplePlanningOrsSnapshot | null
  onOrsSnapshot: (snap: SimplePlanningOrsSnapshot | null, preview: SimplePlanningCalculateRouteResult | null) => void
  className?: string
}

export function SimplePlanningTruckMap({
  companyId,
  officeId,
  truckId,
  truckName,
  orders,
  orsSnapshot,
  onOrsSnapshot,
  className,
}: Props) {
  const [open, setOpen] = useState(false)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [preview, setPreview] = useState<SimplePlanningCalculateRouteResult | null>(null)

  const stops = useMemo(() => consolidateOrders(orders), [orders])
  const withGeo = stops.filter((s) => s.has_georef && s.lat != null && s.lng != null)
  const withoutGeo = stops.filter((s) => !s.has_georef || s.lat == null || s.lng == null)

  const mapRoutes: PlanificacionMapRoute[] = useMemo(() => {
    const mapStops: PlanificacionMapStop[] = withGeo.map((s, i) => ({
      lat: s.lat as number,
      lng: s.lng as number,
      num: i + 1,
      stopKey: `${truckId}-${s.key}`,
      label: s.client_name,
      comuna: s.municipality,
      documentId: s.document_ids[0],
      clientId: s.client_id,
      popup: {
        nombre: s.client_name,
        direccion: s.address,
        comuna: s.municipality,
        ventaTotal: 0,
        ocCount: s.folios.length || s.document_ids.length || 1,
        observaciones: s.folios.length
          ? [`OC ${s.folios.filter(Boolean).join(", ")}`]
          : undefined,
      },
    }))
    const geometry = preview?.geometry || orsSnapshot?.route_geometry
    const positions = lineStringToLatLngs(geometry)
    return [
      {
        camion: truckName,
        color: ROUTE_COLOR,
        positions,
        stops: mapStops,
      },
    ]
  }, [withGeo, truckId, truckName, preview, orsSnapshot])

  const depot =
    preview?.depot ||
    (orsSnapshot && "depot" in orsSnapshot
      ? (orsSnapshot as SimplePlanningOrsSnapshot & { depot?: { lat: number; lng: number } })
          .depot
      : null) ||
    null

  const runCalculate = async () => {
    setBusy(true)
    setError(null)
    try {
      const documentIds = orders
        .map((o) => o.document_id)
        .filter((id): id is number => id != null && Number.isFinite(Number(id)))
        .map(Number)
      if (!documentIds.length) throw new Error("Sin document_id de OC para calcular ruta")
      const res = await postSimplePlanningCalculateRoute({
        company_id: companyId,
        office_id: officeId,
        truck_id: truckId,
        document_ids: documentIds,
        camion: truckName,
      })
      setPreview(res)
      if (res.ok && res.ors_snapshot) {
        onOrsSnapshot(res.ors_snapshot, res)
      } else {
        onOrsSnapshot(null, res)
        setError(res.message || "Ruta no calculada (marcadores disponibles)")
      }
    } catch (e: unknown) {
      setPreview(null)
      onOrsSnapshot(null, null)
      setError(e instanceof Error ? e.message : "Error ORS")
    } finally {
      setBusy(false)
    }
  }

  const metrics = preview?.ors_metrics_calculated
    ? preview
    : orsSnapshot
      ? {
          distance_km: orsSnapshot.distance_km,
          duration_min: orsSnapshot.duration_min,
          liters_estimated: orsSnapshot.liters_estimated,
          fuel_cost_clp: orsSnapshot.fuel_cost_clp,
          crew_cost_clp: orsSnapshot.crew_cost_clp,
          total_route_cost_clp: orsSnapshot.total_route_cost_clp,
          ors_metrics_calculated: true,
          stops_georef_count: withGeo.length,
          stops_pending_count: withoutGeo.length,
        }
      : null

  return (
    <div className={cn("space-y-2", className)}>
      <Button
        type="button"
        variant="outline"
        size="sm"
        className="w-full sm:w-auto"
        onClick={() => setOpen((v) => !v)}
      >
        <MapIcon className="mr-1.5 size-3.5" aria-hidden />
        {open ? "Ocultar mapa" : "Ver mapa y calcular ruta"}
      </Button>

      {open ? (
        <div className="space-y-3 rounded-lg border border-border/70 bg-card/40 p-3">
          <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
            <span>
              {withGeo.length} parada(s) con coordenadas · {withoutGeo.length} sin georref
            </span>
            <Button
              type="button"
              size="sm"
              disabled={busy || withGeo.length < 1}
              onClick={() => void runCalculate()}
            >
              {busy ? (
                <Loader2 className="mr-1.5 size-3.5 animate-spin" />
              ) : (
                <Route className="mr-1.5 size-3.5" />
              )}
              Calcular ruta
            </Button>
          </div>

          <PlanificacionDespachoMapClient
            routes={mapRoutes}
            depot={depot}
            className="h-[360px] w-full overflow-hidden rounded-md border border-border/60"
          />

          {withoutGeo.length ? (
            <div className="rounded-md border border-amber-500/30 bg-amber-500/5 px-3 py-2 text-xs text-amber-900 dark:text-amber-200">
              <p className="font-medium">Sin coordenadas confiables</p>
              <ul className="mt-1 list-inside list-disc">
                {withoutGeo.slice(0, 12).map((s) => (
                  <li key={s.key}>
                    {s.client_name}
                    {s.folios.length ? ` · OC ${s.folios.join(", ")}` : ""}
                    {s.municipality ? ` · ${s.municipality}` : ""}
                    {s.address ? ` — ${s.address}` : ""}
                  </li>
                ))}
              </ul>
              <p className="mt-1 text-[11px] opacity-90">
                No se inventan coordenadas. Estas paradas no entran al cálculo ORS.
              </p>
            </div>
          ) : null}

          <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4 text-xs">
            <Metric
              label="Km carretera"
              value={
                metrics?.ors_metrics_calculated && metrics.distance_km != null
                  ? `${Number(metrics.distance_km).toFixed(1)} km`
                  : "No disponible"
              }
            />
            <Metric
              label="Duración est."
              value={
                metrics?.ors_metrics_calculated && metrics.duration_min != null
                  ? `${Math.round(Number(metrics.duration_min))} min`
                  : "No disponible"
              }
            />
            <Metric
              label="Combustible"
              value={
                metrics?.ors_metrics_calculated && metrics.liters_estimated != null
                  ? `${Number(metrics.liters_estimated).toFixed(1)} L`
                  : "No disponible"
              }
            />
            <Metric
              label="Costo transporte"
              value={
                metrics?.ors_metrics_calculated && metrics.total_route_cost_clp != null
                  ? formatClp(metrics.total_route_cost_clp)
                  : "No disponible"
              }
            />
          </div>
          {metrics?.ors_metrics_calculated ? (
            <p className="text-[11px] text-muted-foreground">
              Ruta por carretera (ORS), orden de visita preservado. Margen comercial y resultado
              post-transporte se muestran en el detalle del plan tras confirmar/facturar.
            </p>
          ) : (
            <p className="text-[11px] text-muted-foreground">
              Opcional: puede confirmar sin calcular ruta. Km y costos quedarán como no calculados.
            </p>
          )}
          {error ? <p className="text-xs text-destructive">{error}</p> : null}
        </div>
      ) : null}
    </div>
  )
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-md border border-border/60 px-2.5 py-2">
      <p className="text-[10px] uppercase tracking-wide text-muted-foreground">{label}</p>
      <p className="font-semibold tabular-nums">{value}</p>
    </div>
  )
}
