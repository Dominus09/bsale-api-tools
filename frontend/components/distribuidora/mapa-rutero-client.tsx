"use client"

import { useCallback, useEffect, useMemo, useRef, useState, type ChangeEvent } from "react"
import dynamic from "next/dynamic"
import L from "leaflet"
import { Loader2 } from "lucide-react"

import {
  getDistribuidoraMapa,
  type DistribuidoraMapaCliente,
  type DistribuidoraPuntoBase,
} from "@/lib/api"

import "leaflet/dist/leaflet.css"
import "react-leaflet-cluster/dist/assets/MarkerCluster.css"
import "react-leaflet-cluster/dist/assets/MarkerCluster.Default.css"

const MapContainer = dynamic(() => import("react-leaflet").then((m) => m.MapContainer), { ssr: false })
const TileLayer = dynamic(() => import("react-leaflet").then((m) => m.TileLayer), { ssr: false })
const Marker = dynamic(() => import("react-leaflet").then((m) => m.Marker), { ssr: false })
const Popup = dynamic(() => import("react-leaflet").then((m) => m.Popup), { ssr: false })
const MarkerClusterGroup = dynamic(() => import("react-leaflet-cluster").then((m) => m.default), {
  ssr: false,
})

const MAP_CENTER: [number, number] = [-42.6, -73.8]
const MAP_ZOOM = 9

const FILTER_ALL = "__all__"

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
      return "#64748b"
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

const MARKER_PX = 14
const MARKER_BORDER = 2

const clienteIconCache = new Map<string, L.DivIcon>()

function getClienteDivIcon(fillColor: string): L.DivIcon {
  const key = fillColor
  let icon = clienteIconCache.get(key)
  if (!icon) {
    const size = MARKER_PX
    const b = MARKER_BORDER
    icon = L.divIcon({
      className: "mapa-rutero-cliente-icon",
      html: `<div style="width:${size}px;height:${size}px;border-radius:9999px;background:${fillColor};border:${b}px solid #ffffff;box-shadow:0 2px 8px rgba(15,23,42,0.28);box-sizing:content-box;"></div>`,
      iconSize: [size + b * 2, size + b * 2],
      iconAnchor: [(size + b * 2) / 2, (size + b * 2) / 2],
      popupAnchor: [0, -10],
    })
    clienteIconCache.set(key, icon)
  }
  return icon
}

let baseIconSingleton: L.DivIcon | null = null
function getBaseDivIcon(): L.DivIcon {
  if (!baseIconSingleton) {
    const s = 12
    const b = 2
    baseIconSingleton = L.divIcon({
      className: "mapa-rutero-base-icon",
      html: `<div style="width:${s}px;height:${s}px;border-radius:4px;background:#1d4ed8;border:${b}px solid #ffffff;box-shadow:0 2px 8px rgba(15,23,42,0.28);box-sizing:content-box;"></div>`,
      iconSize: [s + b * 2, s + b * 2],
      iconAnchor: [(s + b * 2) / 2, (s + b * 2) / 2],
      popupAnchor: [0, -8],
    })
  }
  return baseIconSingleton
}

const SELECT_CLASS =
  "h-9 min-w-[140px] rounded-md border border-input bg-background px-3 text-sm shadow-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"

export default function MapaRuteroClient() {
  const [clientes, setClientes] = useState<DistribuidoraMapaCliente[]>([])
  const [bases, setBases] = useState<DistribuidoraPuntoBase[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState("")
  const [vendedorFilter, setVendedorFilter] = useState(FILTER_ALL)
  const [diaFilter, setDiaFilter] = useState(FILTER_ALL)
  const mounted = useRef(true)

  useEffect(() => {
    mounted.current = true
    setLoading(true)
    setError("")
    getDistribuidoraMapa()
      .then((data) => {
        if (!mounted.current) return
        setClientes(Array.isArray(data.clientes) ? data.clientes : [])
        setBases(Array.isArray(data.bases) ? data.bases : [])
      })
      .catch((e: unknown) => {
        if (!mounted.current) return
        setError(e instanceof Error ? e.message : "Error al cargar el mapa")
      })
      .finally(() => {
        if (mounted.current) setLoading(false)
      })
    return () => {
      mounted.current = false
    }
  }, [])

  const vendedorOptions = useMemo(() => {
    const set = new Set<string>()
    for (const c of clientes) {
      const v = c.vendedor?.trim()
      if (v) set.add(v)
    }
    return Array.from(set).sort((a, b) => a.localeCompare(b, "es"))
  }, [clientes])

  const diaOptions = useMemo(() => {
    const set = new Set<string>()
    for (const c of clientes) {
      const d = c.dia_atencion?.trim()
      if (d) set.add(d)
    }
    return Array.from(set).sort((a, b) => a.localeCompare(b, "es"))
  }, [clientes])

  const clientesVisibles = useMemo(() => {
    return clientes.filter((c) => {
      if (vendedorFilter !== FILTER_ALL) {
        const v = c.vendedor?.trim() ?? ""
        if (v !== vendedorFilter) return false
      }
      if (diaFilter !== FILTER_ALL) {
        const d = c.dia_atencion?.trim() ?? ""
        if (d !== diaFilter) return false
      }
      return true
    })
  }, [clientes, vendedorFilter, diaFilter])

  const onVendedorChange = useCallback((e: ChangeEvent<HTMLSelectElement>) => {
    setVendedorFilter(e.target.value)
  }, [])

  const onDiaChange = useCallback((e: ChangeEvent<HTMLSelectElement>) => {
    setDiaFilter(e.target.value)
  }, [])

  return (
    <div className="p-4">
      <div className="rounded-xl border border-border bg-white p-4 shadow-md dark:bg-card">
        <div className="mb-3 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h1 className="text-lg font-semibold tracking-tight text-foreground">Mapa Rutero</h1>
            <p className="text-sm text-muted-foreground">
              Clientes visibles:{" "}
              <span className="font-medium tabular-nums text-foreground">{clientesVisibles.length}</span>
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <select
              className={SELECT_CLASS}
              value={vendedorFilter}
              onChange={onVendedorChange}
              aria-label="Filtrar por vendedor"
            >
              <option value={FILTER_ALL}>Todos los vendedores</option>
              {vendedorOptions.map((v) => (
                <option key={v} value={v}>
                  {v}
                </option>
              ))}
            </select>
            <select
              className={SELECT_CLASS}
              value={diaFilter}
              onChange={onDiaChange}
              aria-label="Filtrar por día"
            >
              <option value={FILTER_ALL}>Todos los días</option>
              {diaOptions.map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
            </select>
          </div>
        </div>

        <div className="h-[75vh] overflow-hidden rounded-lg border border-border bg-muted/30">
          {loading ? (
            <div className="flex h-full items-center justify-center gap-2 text-muted-foreground">
              <Loader2 className="h-6 w-6 animate-spin" />
              Cargando mapa…
            </div>
          ) : error ? (
            <div className="flex h-full items-center justify-center p-4 text-center text-sm text-destructive">
              {error}
            </div>
          ) : (
            <MapContainer
              center={MAP_CENTER}
              zoom={MAP_ZOOM}
              className="mapa-rutero-leaflet z-0 h-full w-full"
              scrollWheelZoom
              attributionControl
            >
              <TileLayer
                attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/attributions">CARTO</a>'
                url="https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png"
              />
              <MarkerClusterGroup chunkedLoading showCoverageOnHover={false}>
                {clientesVisibles.map((c) => (
                  <Marker
                    key={c.bsale_id}
                    position={[c.lat, c.lon]}
                    icon={getClienteDivIcon(getColorByDay(c.dia_atencion))}
                  >
                    <Popup>
                      <div className="mapa-rutero-popup-inner p-3 text-sm">
                        <p className="font-semibold leading-snug text-foreground">{nombreCliente(c)}</p>
                        <dl className="mt-2 space-y-1.5 text-muted-foreground">
                          <div className="grid grid-cols-[5.5rem_1fr] gap-x-2 gap-y-1">
                            <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground/80">
                              Vendedor
                            </dt>
                            <dd className="text-foreground">{c.vendedor?.trim() || "—"}</dd>
                            <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground/80">
                              Día
                            </dt>
                            <dd className="text-foreground">{c.dia_atencion?.trim() || "—"}</dd>
                            <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground/80">
                              Teléfono
                            </dt>
                            <dd className="text-foreground">{c.phone?.trim() || "—"}</dd>
                            <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground/80">
                              Municipio
                            </dt>
                            <dd className="text-foreground">{c.municipality?.trim() || "—"}</dd>
                          </div>
                        </dl>
                      </div>
                    </Popup>
                  </Marker>
                ))}
              </MarkerClusterGroup>
              {bases.map((b, i) => {
                const key = `${b.vendedor ?? "b"}-${b.lat}-${b.lon}-${i}`
                return (
                  <Marker key={key} position={[b.lat, b.lon]} icon={getBaseDivIcon()}>
                    <Popup>
                      <div className="mapa-rutero-popup-inner p-2 text-sm">
                        <p className="font-semibold text-foreground">{b.nombre?.trim() || "Punto base"}</p>
                        <p className="text-muted-foreground">{b.vendedor?.trim() || "—"}</p>
                      </div>
                    </Popup>
                  </Marker>
                )
              })}
            </MapContainer>
          )}
        </div>
      </div>
    </div>
  )
}
