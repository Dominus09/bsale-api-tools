"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import dynamic from "next/dynamic"
import {
  Crosshair,
  Loader2,
  MapPin,
  Navigation,
  Search,
  LocateFixed,
} from "lucide-react"
import type { Map as LeafletMap } from "leaflet"

import {
  formatKm,
  googleMapsNavUrl,
  googleMapsSearchUrl,
  haversineKm,
  setDeliveryStopStatus,
  type DeliveryMapPayload,
  type DeliveryMapStop,
} from "@/lib/delivery-map-api"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { cn } from "@/lib/utils"

const DeliveryLeafletMap = dynamic(() => import("./delivery-leaflet-map"), {
  ssr: false,
  loading: () => (
    <div className="flex h-full items-center justify-center bg-slate-100 text-sm text-muted-foreground">
      Cargando mapa…
    </div>
  ),
})

type Props = {
  initial: DeliveryMapPayload
  loadId?: number | null
}

type MyPos = { lat: number; lng: number }

function formatClp(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "—"
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  }).format(n)
}

export function DeliveryMapClient({ initial, loadId = null }: Props) {
  const [data, setData] = useState(initial)
  const [myPos, setMyPos] = useState<MyPos | null>(null)
  const [geoError, setGeoError] = useState<string | null>(null)
  const [geoLoading, setGeoLoading] = useState(false)
  const [selectedKey, setSelectedKey] = useState<string | null>(null)
  const [query, setQuery] = useState("")
  const [sortNear, setSortNear] = useState(false)
  const [busyKey, setBusyKey] = useState<string | null>(null)
  const [focusToken, setFocusToken] = useState(0)
  const mapRef = useRef<LeafletMap | null>(null)

  const planId = data.load.plan_id
  const summary = data.summary

  const withDistance = useMemo(() => {
    return data.stops.map((s) => {
      let distance_km: number | null = null
      if (
        myPos &&
        s.has_coordinates &&
        s.latitude != null &&
        s.longitude != null
      ) {
        distance_km = haversineKm(
          myPos.lat,
          myPos.lng,
          s.latitude,
          s.longitude,
        )
      }
      return { ...s, distance_km }
    })
  }, [data.stops, myPos])

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    let list = withDistance
    if (q) {
      list = list.filter((s) => (s.search_text || "").includes(q) ||
        s.customer_name.toLowerCase().includes(q) ||
        (s.address || "").toLowerCase().includes(q) ||
        (s.documents || []).some((d) => d.includes(q)))
    }
    if (sortNear && myPos) {
      list = [...list].sort((a, b) => {
        const da = a.distance_km ?? 1e9
        const db = b.distance_km ?? 1e9
        return da - db
      })
    }
    return list
  }, [withDistance, query, sortNear, myPos])

  const pending = filtered.filter((s) => s.status !== "delivered")
  const delivered = filtered.filter((s) => s.status === "delivered")
  const noGps = filtered.filter((s) => !s.has_coordinates)
  const pendingWithGps = pending.filter((s) => s.has_coordinates)

  const selected = withDistance.find((s) => s.customer_key === selectedKey) || null

  const requestGeo = useCallback(() => {
    setGeoLoading(true)
    setGeoError(null)
    if (!navigator.geolocation) {
      setGeoError("GPS no disponible en este dispositivo")
      setGeoLoading(false)
      return
    }
    navigator.geolocation.getCurrentPosition(
      (pos) => {
        setMyPos({ lat: pos.coords.latitude, lng: pos.coords.longitude })
        setGeoLoading(false)
      },
      (err) => {
        setGeoError(
          err.code === 1
            ? "Permiso de ubicación denegado"
            : "No se pudo obtener ubicación",
        )
        setGeoLoading(false)
      },
      { enableHighAccuracy: true, timeout: 15000, maximumAge: 10000 },
    )
  }, [])

  useEffect(() => {
    requestGeo()
  }, [requestGeo])

  const focusStop = useCallback((stop: DeliveryMapStop) => {
    setSelectedKey(stop.customer_key)
    setFocusToken((t) => t + 1)
  }, [])

  const goNearest = useCallback(() => {
    if (!myPos) {
      requestGeo()
      setGeoError("Activa Mi ubicación para calcular el más cercano")
      return
    }
    const candidates = pendingWithGps
      .map((s) => ({
        s,
        d: haversineKm(myPos.lat, myPos.lng, s.latitude!, s.longitude!),
      }))
      .sort((a, b) => a.d - b.d)
    if (!candidates.length) {
      setGeoError("No hay pendientes con GPS")
      return
    }
    focusStop(candidates[0].s)
  }, [myPos, pendingWithGps, focusStop, requestGeo])

  const toggleStatus = async (stop: DeliveryMapStop) => {
    const next = stop.status === "delivered" ? "pending" : "delivered"
    setBusyKey(stop.customer_key)
    try {
      const updated = await setDeliveryStopStatus({
        planId,
        customerKey: stop.customer_key,
        status: next,
        loadId,
      })
      setData(updated)
    } catch (e) {
      setGeoError(e instanceof Error ? e.message : "Error al guardar estado")
    } finally {
      setBusyKey(null)
    }
  }

  return (
    <div className="mx-auto flex min-h-[100dvh] max-w-lg flex-col bg-background">
      <header className="sticky top-0 z-20 space-y-2 border-b bg-background/95 px-3 py-3 backdrop-blur">
        <div className="flex items-start justify-between gap-2">
          <div>
            <h1 className="text-xl font-bold tracking-tight">
              {data.load.picking_number}
            </h1>
            <p className="text-sm text-muted-foreground">
              Entregas: {summary.delivered} / {summary.customers}
              {data.load.truck_name ? ` · ${data.load.truck_name}` : ""}
            </p>
          </div>
        </div>
        <div className="grid grid-cols-2 gap-2">
          <Button
            type="button"
            variant="secondary"
            className="h-12 text-sm"
            onClick={requestGeo}
            disabled={geoLoading}
          >
            {geoLoading ? (
              <Loader2 className="mr-2 size-4 animate-spin" />
            ) : (
              <LocateFixed className="mr-2 size-4" />
            )}
            Mi ubicación
          </Button>
          <Button
            type="button"
            className="h-12 text-sm"
            onClick={goNearest}
          >
            <Crosshair className="mr-2 size-4" />
            Más cercano
          </Button>
        </div>
        {geoError ? (
          <p className="text-xs text-amber-700">{geoError}</p>
        ) : myPos ? (
          <p className="text-xs text-emerald-700">GPS activo</p>
        ) : null}
      </header>

      <div className="relative h-[42vh] min-h-[220px] w-full shrink-0 border-b">
        <DeliveryLeafletMap
          stops={withDistance}
          myPos={myPos}
          selectedKey={selectedKey}
          focusToken={focusToken}
          onSelect={(key) => setSelectedKey(key)}
          mapRef={mapRef}
        />
      </div>

      {selected ? (
        <div className="border-b bg-card px-3 py-3 shadow-sm">
          <div className="flex items-start justify-between gap-2">
            <div className="min-w-0">
              <p className="font-semibold leading-tight">{selected.customer_name}</p>
              {selected.fantasy_name ? (
                <p className="text-xs text-muted-foreground">{selected.fantasy_name}</p>
              ) : null}
              <p className="mt-1 text-sm text-muted-foreground">
                {[selected.address, selected.city].filter(Boolean).join(", ") ||
                  "Sin dirección"}
              </p>
              <p className="mt-1 text-xs">
                OC {selected.documents.join(", ") || "—"} ·{" "}
                {formatClp(selected.amount)} · {selected.items_count} doc.
                {selected.distance_km != null
                  ? ` · ${formatKm(selected.distance_km)}`
                  : ""}
              </p>
              <p
                className={cn(
                  "mt-1 text-xs font-medium",
                  selected.status === "delivered"
                    ? "text-emerald-700"
                    : "text-amber-700",
                )}
              >
                {selected.status === "delivered" ? "Entregado" : "Pendiente"}
                {!selected.has_coordinates ? " · Sin GPS" : ""}
              </p>
            </div>
          </div>
          <div className="mt-3 grid grid-cols-2 gap-2">
            {selected.has_coordinates &&
            selected.latitude != null &&
            selected.longitude != null ? (
              <Button asChild className="h-11">
                <a
                  href={googleMapsNavUrl(selected.latitude, selected.longitude)}
                  target="_blank"
                  rel="noreferrer"
                >
                  <Navigation className="mr-2 size-4" />
                  Ir
                </a>
              </Button>
            ) : (
              <Button asChild variant="secondary" className="h-11">
                <a
                  href={googleMapsSearchUrl(
                    [selected.address, selected.city, selected.customer_name]
                      .filter(Boolean)
                      .join(", "),
                  )}
                  target="_blank"
                  rel="noreferrer"
                >
                  <Search className="mr-2 size-4" />
                  Buscar dirección
                </a>
              </Button>
            )}
            <Button
              type="button"
              variant={selected.status === "delivered" ? "outline" : "default"}
              className={cn(
                "h-11",
                selected.status === "delivered" && "border-emerald-600 text-emerald-800",
              )}
              disabled={busyKey === selected.customer_key}
              onClick={() => void toggleStatus(selected)}
            >
              {busyKey === selected.customer_key ? (
                <Loader2 className="mr-2 size-4 animate-spin" />
              ) : null}
              {selected.status === "delivered" ? "Revertir" : "Marcar entregado"}
            </Button>
          </div>
        </div>
      ) : null}

      <div className="space-y-3 px-3 py-3 pb-24">
        <div className="flex gap-2">
          <div className="relative flex-1">
            <Search className="pointer-events-none absolute left-2.5 top-3 size-4 text-muted-foreground" />
            <Input
              className="h-11 pl-9"
              placeholder="Buscar cliente, dirección u OC…"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
            />
          </div>
          <Button
            type="button"
            variant={sortNear ? "default" : "outline"}
            className="h-11 shrink-0 px-3 text-xs"
            onClick={() => setSortNear((v) => !v)}
          >
            Cercanos
          </Button>
        </div>

        <StopSection
          title={`Pendientes (${pending.filter((s) => s.has_coordinates).length})`}
          stops={pending.filter((s) => s.has_coordinates)}
          selectedKey={selectedKey}
          onSelect={focusStop}
        />
        <StopSection
          title={`Entregados (${delivered.length})`}
          stops={delivered}
          selectedKey={selectedKey}
          onSelect={focusStop}
          tone="done"
        />
        <StopSection
          title={`Sin ubicación (${noGps.length})`}
          stops={noGps}
          selectedKey={selectedKey}
          onSelect={focusStop}
          tone="warn"
        />
      </div>
    </div>
  )
}

function StopSection({
  title,
  stops,
  selectedKey,
  onSelect,
  tone,
}: {
  title: string
  stops: Array<DeliveryMapStop & { distance_km?: number | null }>
  selectedKey: string | null
  onSelect: (s: DeliveryMapStop) => void
  tone?: "done" | "warn"
}) {
  if (!stops.length) return null
  return (
    <section>
      <h2
        className={cn(
          "mb-2 text-xs font-semibold uppercase tracking-wide",
          tone === "done" && "text-emerald-700",
          tone === "warn" && "text-amber-700",
          !tone && "text-muted-foreground",
        )}
      >
        {tone === "warn" ? `⚠️ ${title}` : title}
      </h2>
      <ul className="space-y-2">
        {stops.map((s) => (
          <li key={s.customer_key}>
            <button
              type="button"
              onClick={() => onSelect(s)}
              className={cn(
                "flex w-full items-start gap-3 rounded-xl border px-3 py-3 text-left transition",
                selectedKey === s.customer_key
                  ? "border-blue-500 bg-blue-50"
                  : "border-border bg-card",
                s.status === "delivered" && "border-emerald-200 bg-emerald-50/60",
              )}
            >
              <MapPin
                className={cn(
                  "mt-0.5 size-5 shrink-0",
                  s.status === "delivered"
                    ? "text-emerald-600"
                    : s.has_coordinates
                      ? "text-blue-600"
                      : "text-amber-600",
                )}
              />
              <div className="min-w-0 flex-1">
                <p className="font-medium leading-tight">{s.customer_name}</p>
                <p className="mt-0.5 truncate text-sm text-muted-foreground">
                  {[s.address, s.city].filter(Boolean).join(", ") || "Sin dirección"}
                </p>
                <p className="mt-1 text-xs text-muted-foreground">
                  {s.distance_km != null ? formatKm(s.distance_km) : "—"} ·{" "}
                  {s.status === "delivered" ? "Entregado" : "Pendiente"}
                  {s.documents?.length ? ` · OC ${s.documents.join(", ")}` : ""}
                </p>
              </div>
            </button>
          </li>
        ))}
      </ul>
    </section>
  )
}
