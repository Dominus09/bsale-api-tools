"use client"

import { useEffect, useMemo, useState } from "react"
import dynamic from "next/dynamic"
import { Loader2, MapPinned } from "lucide-react"

import {
  getDistribuidoraMapa,
  type DistribuidoraMapaCliente,
  type DistribuidoraPuntoBase,
} from "@/lib/api"

import "leaflet/dist/leaflet.css"
import "react-leaflet-cluster/dist/assets/MarkerCluster.css"
import "react-leaflet-cluster/dist/assets/MarkerCluster.Default.css"

const MapContainer = dynamic(
  () => import("react-leaflet").then((m) => m.MapContainer),
  { ssr: false },
)
const TileLayer = dynamic(
  () => import("react-leaflet").then((m) => m.TileLayer),
  { ssr: false },
)
const CircleMarker = dynamic(
  () => import("react-leaflet").then((m) => m.CircleMarker),
  { ssr: false },
)
const Popup = dynamic(
  () => import("react-leaflet").then((m) => m.Popup),
  { ssr: false },
)
const Marker = dynamic(
  () => import("react-leaflet").then((m) => m.Marker),
  { ssr: false },
)
const Tooltip = dynamic(
  () => import("react-leaflet").then((m) => m.Tooltip),
  { ssr: false },
)
const MarkerClusterGroup = dynamic(
  () => import("react-leaflet-cluster").then((m) => m.default),
  { ssr: false },
)

const DIAS_ORDEN = [
  "Lunes",
  "Martes",
  "Miércoles",
  "Jueves",
  "Viernes",
  "Sábado",
] as const

function getColorByDay(dia: string | null | undefined): string {
  switch (dia) {
    case "Lunes":
      return "#2563eb"
    case "Martes":
      return "#16a34a"
    case "Miércoles":
      return "#ea580c"
    case "Jueves":
      return "#dc2626"
    case "Viernes":
      return "#9333ea"
    case "Sábado":
      return "#ca8a04"
    default:
      return "#6b7280"
  }
}

function nombreCliente(c: DistribuidoraMapaCliente): string {
  const fan = c.nombre_fantasia?.trim()
  if (fan) return fan
  const fn = c.first_name?.trim() ?? ""
  const ln = c.last_name?.trim() ?? ""
  const full = `${fn} ${ln}`.trim()
  return full || `Cliente #${c.bsale_id}`
}

/** Icono grande: relleno rojo fuerte, borde negro (no depende de filtros de clientes). */
function usePuntoBaseIcon() {
  return useMemo(() => {
    if (typeof window === "undefined") return null
    const L = require("leaflet") as typeof import("leaflet")
    return L.divIcon({
      className: "distribuidora-punto-base-icon",
      html: `<div style="width:34px;height:34px;border-radius:50%;background:#b91c1c;border:4px solid #0a0a0a;box-shadow:0 3px 12px rgba(0,0,0,0.5);box-sizing:border-box"></div>`,
      iconSize: [34, 34],
      iconAnchor: [17, 17],
      popupAnchor: [0, -14],
    })
  }, [])
}

function labelsBase(b: DistribuidoraPuntoBase): { nombre: string; vendedor: string } {
  return {
    nombre: b.nombre?.trim() || "Base",
    vendedor: b.vendedor?.trim() || "—",
  }
}

export default function DistribuidoraMapaPage() {
  const [clientes, setClientes] = useState<DistribuidoraMapaCliente[]>([])
  const [bases, setBases] = useState<DistribuidoraPuntoBase[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [vendedor, setVendedor] = useState("todos")
  const [dia, setDia] = useState("todos")

  useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        const data = await getDistribuidoraMapa()
        if (cancelled) return
        setClientes(Array.isArray(data.clientes) ? data.clientes : [])
        setBases(Array.isArray(data.bases) ? data.bases : [])
      } catch (e) {
        if (!cancelled) {
          setError(e instanceof Error ? e.message : "Error al cargar el mapa")
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    })()
    return () => {
      cancelled = true
    }
  }, [])

  const opcionesVendedor = useMemo(() => {
    const s = new Set<string>()
    for (const c of clientes) {
      const v = c.vendedor?.trim()
      if (v) s.add(v)
    }
    return Array.from(s).sort((a, b) => a.localeCompare(b, "es"))
  }, [clientes])

  const opcionesDia = useMemo(() => {
    const present = new Set(
      clientes.map((c) => c.dia_atencion?.trim()).filter(Boolean) as string[],
    )
    return DIAS_ORDEN.filter((d) => present.has(d))
  }, [clientes])

  const filtrados = useMemo(() => {
    return clientes.filter((c) => {
      if (!Number.isFinite(c.lat) || !Number.isFinite(c.lon)) return false
      if (vendedor !== "todos" && (c.vendedor?.trim() ?? "") !== vendedor) return false
      if (dia !== "todos" && (c.dia_atencion?.trim() ?? "") !== dia) return false
      return true
    })
  }, [clientes, vendedor, dia])

  /** Siempre desde `bases` completo: no aplica filtros de vendedor/día del rutero. */
  const basesOk = useMemo(
    () =>
      bases.filter(
        (b) =>
          Number.isFinite(b.lat) &&
          Number.isFinite(b.lon) &&
          (b.nombre?.trim() || b.vendedor?.trim()),
      ),
    [bases],
  )

  const iconoBase = usePuntoBaseIcon()

  if (loading) {
    return (
      <div className="flex min-h-[60vh] items-center justify-center rounded-xl border border-border bg-muted/20">
        <Loader2 className="h-10 w-10 animate-spin text-primary" aria-hidden />
      </div>
    )
  }

  if (error) {
    return (
      <div className="rounded-xl border border-destructive/30 bg-destructive/5 p-6 text-center text-sm text-destructive">
        {error}
      </div>
    )
  }

  return (
    <div className="flex h-[calc(100dvh-5.5rem)] min-h-[480px] flex-col gap-4">
      <header className="flex shrink-0 flex-col gap-3 rounded-xl border border-border bg-card px-4 py-3 shadow-sm sm:flex-row sm:items-center sm:justify-between">
        <div className="flex items-center gap-2">
          <MapPinned className="h-5 w-5 shrink-0 text-primary" aria-hidden />
          <h1 className="text-lg font-semibold tracking-tight text-foreground">
            Mapa Rutero
          </h1>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <label className="flex items-center gap-2 text-sm text-muted-foreground">
            <span className="whitespace-nowrap">Vendedor</span>
            <select
              value={vendedor}
              onChange={(e) => setVendedor(e.target.value)}
              className="rounded-md border border-input bg-background px-3 py-2 text-sm text-foreground shadow-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              <option value="todos">Todos</option>
              {opcionesVendedor.map((v) => (
                <option key={v} value={v}>
                  {v}
                </option>
              ))}
            </select>
          </label>
          <label className="flex items-center gap-2 text-sm text-muted-foreground">
            <span className="whitespace-nowrap">Día</span>
            <select
              value={dia}
              onChange={(e) => setDia(e.target.value)}
              className="rounded-md border border-input bg-background px-3 py-2 text-sm text-foreground shadow-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              <option value="todos">Todos</option>
              {opcionesDia.map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
            </select>
          </label>
          <span className="rounded-md bg-muted px-3 py-2 text-sm font-medium text-foreground">
            Clientes visibles: {filtrados.length}
          </span>
        </div>
      </header>

      <div className="relative min-h-0 flex-1 overflow-hidden rounded-xl border border-border bg-muted/10 shadow-inner">
        <MapContainer
          center={[-42.6, -73.8]}
          zoom={9}
          className="z-0 size-full [&_.leaflet-container]:size-full [&_.leaflet-container]:cursor-grab [&_.leaflet-container]:font-sans"
          style={{ height: "100%", width: "100%", minHeight: "min(90vh, 720px)" }}
          scrollWheelZoom
        >
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />

          <MarkerClusterGroup
            chunkedLoading
            showCoverageOnHover={false}
            maxClusterRadius={56}
            spiderfyOnMaxZoom
          >
            {filtrados.map((c) => {
              const telefonico =
                (c.tipo_atencion ?? "").toLowerCase().trim() === "telefonico"
              const stroke = telefonico ? "#64748b" : getColorByDay(c.dia_atencion)
              const fill = telefonico ? "#94a3b8" : getColorByDay(c.dia_atencion)
              return (
                <CircleMarker
                  key={c.bsale_id}
                  center={[c.lat, c.lon]}
                  radius={telefonico ? 6 : 8}
                  pathOptions={{
                    color: stroke,
                    fillColor: fill,
                    fillOpacity: telefonico ? 0.75 : 0.88,
                    weight: 2,
                  }}
                >
                  <Popup>
                    <div className="min-w-[10rem] space-y-1.5 text-sm text-foreground">
                      <p className="font-semibold leading-tight">{nombreCliente(c)}</p>
                      {telefonico ? (
                        <p className="text-xs font-medium text-muted-foreground">
                          Atención telefónica
                        </p>
                      ) : null}
                      <dl className="space-y-1 text-xs text-muted-foreground">
                        <div className="flex justify-between gap-3">
                          <dt>Vendedor</dt>
                          <dd className="text-right text-foreground">
                            {c.vendedor ?? "—"}
                          </dd>
                        </div>
                        <div className="flex justify-between gap-3">
                          <dt>Día</dt>
                          <dd className="text-right text-foreground">
                            {c.dia_atencion ?? "—"}
                          </dd>
                        </div>
                        <div className="flex justify-between gap-3">
                          <dt>Teléfono</dt>
                          <dd className="text-right text-foreground">
                            {c.phone?.trim() || "—"}
                          </dd>
                        </div>
                        <div className="flex justify-between gap-3">
                          <dt>Comuna</dt>
                          <dd className="text-right text-foreground">
                            {c.municipality ?? "—"}
                          </dd>
                        </div>
                        <div className="flex justify-between gap-3">
                          <dt>Tipo</dt>
                          <dd className="text-right capitalize text-foreground">
                            {c.tipo_atencion ?? "—"}
                          </dd>
                        </div>
                      </dl>
                    </div>
                  </Popup>
                </CircleMarker>
              )
            })}
          </MarkerClusterGroup>

          {iconoBase
            ? basesOk.map((b, i) => {
                const { nombre, vendedor: vBase } = labelsBase(b)
                return (
                  <Marker
                    key={`base-${i}-${vBase}-${nombre}`}
                    position={[b.lat, b.lon]}
                    icon={iconoBase}
                    zIndexOffset={800}
                  >
                    <Tooltip
                      permanent
                      direction="top"
                      offset={[0, -10]}
                      opacity={1}
                      className="!rounded-md !border !border-neutral-800 !bg-neutral-950 !px-2.5 !py-1.5 !text-[11px] !leading-snug !text-white !shadow-lg [&_.leaflet-tooltip-content]:!m-0"
                    >
                      <div className="font-semibold">{nombre}</div>
                      <div className="mt-0.5 font-normal opacity-90">{vBase}</div>
                    </Tooltip>
                    <Popup>
                      <div className="text-sm">
                        <p className="font-semibold text-foreground">{nombre}</p>
                        <p className="text-xs text-muted-foreground">{vBase}</p>
                      </div>
                    </Popup>
                  </Marker>
                )
              })
            : null}
        </MapContainer>
      </div>
    </div>
  )
}
