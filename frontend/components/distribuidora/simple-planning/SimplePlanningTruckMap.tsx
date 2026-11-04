"use client"

import dynamic from "next/dynamic"
import { useMemo, useState } from "react"
import { ChevronDown, Loader2, Map as MapIcon, Route } from "lucide-react"

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
  missing_reason: string | null
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
      missing_reason: hasGeo ? null : "client_lat_lon_null",
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
  const [pendingOpen, setPendingOpen] = useState(true)

  const stops = useMemo(() => consolidateOrders(orders), [orders])
  const withGeo = stops.filter((s) => s.has_georef && s.lat != null && s.lng != null)
  const withoutGeo = stops.filter((s) => !s.has_georef || s.lat == null || s.lng == null)
  const clientsTotal = stops.length
  const clientsGeoref = withGeo.length
  const clientsPending = withoutGeo.length

  const coverage =
    preview?.route_coverage ||
    orsSnapshot?.route_coverage ||
    (clientsPending === 0 && clientsGeoref > 0
      ? "full"
      : clientsGeoref > 0
        ? "partial"
        : "none")

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
        observaciones: [
          s.folios.length ? `OC ${s.folios.filter(Boolean).join(", ")}` : null,
          s.client_id != null ? `Cliente #${s.client_id}` : null,
        ].filter(Boolean) as string[],
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
          route_coverage: orsSnapshot.route_coverage,
          clients_georef: orsSnapshot.clients_georef ?? withGeo.length,
          clients_pending: orsSnapshot.clients_pending ?? withoutGeo.length,
          clients_total: orsSnapshot.clients_total ?? clientsTotal,
        }
      : null

  const isPartial =
    coverage === "partial" ||
    (metrics?.ors_metrics_calculated && clientsPending > 0)

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
          <div className="flex flex-wrap items-center justify-between gap-2 text-xs">
            <div className="space-y-0.5">
              <p className="font-medium text-foreground">
                Georreferenciados: {clientsGeoref} de {clientsTotal} clientes.
              </p>
              {clientsPending > 0 ? (
                <p className="text-amber-800 dark:text-amber-200">
                  Pendientes de georreferencia: {clientsPending} clientes.
                </p>
              ) : (
                <p className="text-muted-foreground">Todos los clientes tienen coordenadas.</p>
              )}
            </div>
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
              {clientsPending > 0 && withGeo.length > 0
                ? "Calcular ruta parcial"
                : "Calcular ruta"}
            </Button>
          </div>

          <PlanificacionDespachoMapClient
            routes={mapRoutes}
            depot={depot}
            className="h-[360px] w-full overflow-hidden rounded-md border border-border/60"
          />

          {withoutGeo.length ? (
            <div className="rounded-md border border-amber-500/30 bg-amber-500/5 px-3 py-2 text-xs text-amber-900 dark:text-amber-200">
              <button
                type="button"
                className="flex w-full items-center justify-between gap-2 text-left font-medium"
                onClick={() => setPendingOpen((v) => !v)}
              >
                <span>
                  Pendientes de georreferencia ({withoutGeo.length})
                </span>
                <ChevronDown
                  className={cn(
                    "size-3.5 shrink-0 transition-transform",
                    pendingOpen ? "rotate-180" : "",
                  )}
                />
              </button>
              {pendingOpen ? (
                <ul className="mt-2 space-y-2">
                  {withoutGeo.map((s) => (
                    <li
                      key={s.key}
                      className="rounded border border-amber-500/20 bg-background/40 px-2 py-1.5"
                    >
                      <p className="font-medium text-foreground">{s.client_name}</p>
                      <p className="text-[11px] opacity-90">
                        {s.client_id != null ? `ID ${s.client_id}` : "Sin ID"}
                        {s.municipality ? ` · ${s.municipality}` : ""}
                      </p>
                      {s.address ? (
                        <p className="text-[11px] opacity-90">{s.address}</p>
                      ) : null}
                      <p className="text-[11px] opacity-90">
                        OC:{" "}
                        {s.folios.length
                          ? s.folios.join(", ")
                          : s.document_ids.join(", ") || "—"}
                      </p>
                      <p className="text-[11px] opacity-80">
                        Motivo: {s.missing_reason || "client_lat_lon_null"} (sin inventar
                        coordenadas ni centro de comuna)
                      </p>
                    </li>
                  ))}
                </ul>
              ) : null}
            </div>
          ) : null}

          {isPartial && metrics?.ors_metrics_calculated ? (
            <div className="rounded-md border border-sky-500/30 bg-sky-500/5 px-3 py-2 text-xs text-sky-900 dark:text-sky-200">
              <p className="font-medium">
                Ruta parcial: {clientsGeoref} de {clientsTotal} paradas.
              </p>
              <p className="mt-0.5 opacity-90">
                Kilómetros y duración solo del recorrido georreferenciado. No representan la
                distancia total del despacho. Puede confirmar con advertencia.
              </p>
            </div>
          ) : null}

          <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4 text-xs">
            <Metric
              label={isPartial ? "Km (parcial)" : "Km carretera"}
              value={
                metrics?.ors_metrics_calculated && metrics.distance_km != null
                  ? `${Number(metrics.distance_km).toFixed(1)} km`
                  : "No disponible"
              }
            />
            <Metric
              label={isPartial ? "Duración (parcial)" : "Duración est."}
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
              label={isPartial ? "Costo (no final)" : "Costo transporte"}
              value={
                metrics?.ors_metrics_calculated && metrics.total_route_cost_clp != null
                  ? formatClp(metrics.total_route_cost_clp)
                  : "No disponible"
              }
            />
          </div>
          {metrics?.ors_metrics_calculated ? (
            <p className="text-[11px] text-muted-foreground">
              {isPartial
                ? "Ruta parcial ORS con orden de visita preservado. El margen comercial no depende de ORS; el resultado post-transporte solo aplica con ruta completa."
                : "Ruta completa ORS, orden de visita preservado. Margen comercial y post-transporte se muestran en el detalle tras confirmar."}
            </p>
          ) : (
            <p className="text-[11px] text-muted-foreground">
              Opcional: puede confirmar sin calcular ruta. Km y costos quedarán como no
              calculados. La falta de georreferencia no bloquea la planificación.
            </p>
          )}
          {error ? <p className="text-xs text-destructive">{error}</p> : null}
          {preview && !preview.ok && preview.message ? (
            <p className="text-xs text-amber-800 dark:text-amber-200">{preview.message}</p>
          ) : null}
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
