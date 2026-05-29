"use client"

import dynamic from "next/dynamic"
import { useEffect, useMemo, useState } from "react"
import { MapContainer, Marker, Polyline, Popup, TileLayer, useMap } from "react-leaflet"
import L from "leaflet"
import "leaflet/dist/leaflet.css"
import "react-leaflet-cluster/dist/assets/MarkerCluster.css"
import "react-leaflet-cluster/dist/assets/MarkerCluster.Default.css"

const MarkerClusterGroup = dynamic(
  () => import("react-leaflet-cluster").then((m) => m.default),
  { ssr: false },
)

import {
  getOperacionesVendedorRecorrido,
  type RecorridoPunto,
  type VendedorRecorridoResponse,
} from "@/services/operaciones"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Checkbox } from "@/components/ui/checkbox"
import { Label } from "@/components/ui/label"
import { Skeleton } from "@/components/ui/skeleton"
import { cn } from "@/lib/utils"

function recorridoClusterIcon(cluster: { getChildCount(): number }) {
  const count = cluster.getChildCount()
  return L.divIcon({
    className: "",
    html: `<div style="width:24px;height:24px;border-radius:50%;background:#6366f1;color:#fff;font-size:10px;font-weight:700;display:flex;align-items:center;justify-content:center;border:2px solid #fff;box-shadow:0 1px 4px rgba(0,0,0,.35)">${count}</div>`,
    iconSize: [24, 24],
    iconAnchor: [12, 12],
  })
}

function numberedIcon(n: number, bg: string) {
  return new L.DivIcon({
    className: "",
    html: `<div style="width:20px;height:20px;border-radius:50%;background:${bg};color:#fff;font-size:9px;font-weight:700;display:flex;align-items:center;justify-content:center;border:2px solid #fff;box-shadow:0 1px 4px rgba(0,0,0,.35)">${n}</div>`,
    iconSize: [20, 20],
    iconAnchor: [10, 10],
  })
}

const iconInicio = new L.DivIcon({
  className: "",
  html: '<div style="width:16px;height:16px;border-radius:50%;background:#22c55e;border:3px solid #fff"></div>',
  iconSize: [16, 16],
  iconAnchor: [8, 8],
})

const iconUltima = new L.DivIcon({
  className: "",
  html: '<div style="width:18px;height:18px;border-radius:50%;background:#ef4444;border:3px solid #fff"></div>',
  iconSize: [18, 18],
  iconAnchor: [9, 9],
})

function puntoColor(tipo: string) {
  if (tipo === "incidencia") return "#eab308"
  return "#3b82f6"
}

function FlyTo({ lat, lon }: { lat: number; lon: number }) {
  const map = useMap()
  useEffect(() => {
    map.flyTo([lat, lon], 16, { duration: 0.6 })
  }, [lat, lon, map])
  return null
}

function formatHora(ts: string | null | undefined) {
  if (!ts) return "—"
  return new Date(ts).toLocaleTimeString("es-CL", { hour: "2-digit", minute: "2-digit" })
}

type ModoMapa = "resumido" | "completo"

export default function RecorridoVendedorMapa({
  codigo,
  fecha,
}: {
  codigo: string
  fecha: string
}) {
  const [data, setData] = useState<VendedorRecorridoResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [focus, setFocus] = useState<{ lat: number; lon: number } | null>(null)
  const [modo, setModo] = useState<ModoMapa>("resumido")
  const [showVisitas, setShowVisitas] = useState(true)
  const [showIncidencias, setShowIncidencias] = useState(true)
  const [showGps, setShowGps] = useState(false)
  const [showHeartbeat, setShowHeartbeat] = useState(false)

  useEffect(() => {
    setLoading(true)
    getOperacionesVendedorRecorrido(codigo, fecha)
      .then(setData)
      .catch((e) => setError(e instanceof Error ? e.message : "Error"))
      .finally(() => setLoading(false))
  }, [codigo, fecha])

  const puntosVisibles = useMemo(() => {
    if (!data?.puntos) return []
    return data.puntos.filter((p) => {
      if (p.tipo === "visita" && !showVisitas) return false
      if (p.tipo === "incidencia" && !showIncidencias) return false
      return true
    })
  }, [data, showVisitas, showIncidencias])

  const center = useMemo((): [number, number] => {
    if (focus) return [focus.lat, focus.lon]
    const p = puntosVisibles[0] ?? data?.puntos?.[0]
    if (p) return [p.lat, p.lon]
    if (data?.ultima_posicion?.lat != null && data.ultima_posicion.lon != null) {
      return [data.ultima_posicion.lat, data.ultima_posicion.lon]
    }
    return [-33.45, -70.65]
  }, [data, focus, puntosVisibles])

  const lineas = useMemo(() => {
    const out: { pts: [number, number][]; color: string; weight: number }[] = []
    if (modo === "completo" && showGps && data?.linea_gps?.length) {
      out.push({
        pts: data.linea_gps.map((g) => [g.lat, g.lon] as [number, number]),
        color: "#6366f1",
        weight: 3,
      })
    }
    if (modo === "completo" && showHeartbeat && data?.linea_heartbeat?.length) {
      out.push({
        pts: data.linea_heartbeat.map((g) => [g.lat, g.lon] as [number, number]),
        color: "#a855f7",
        weight: 2,
      })
    }
    if (modo === "resumido" && puntosVisibles.length > 1) {
      out.push({
        pts: puntosVisibles.map((p) => [p.lat, p.lon] as [number, number]),
        color: "#0ea5e9",
        weight: 4,
      })
    }
    return out
  }, [data, modo, showGps, showHeartbeat, puntosVisibles])

  if (loading) return <Skeleton className="h-[420px] w-full rounded-xl" />
  if (error || !data) {
    return (
      <Card>
        <CardContent className="py-8 text-center text-muted-foreground">
          {error || "Sin recorrido"}
        </CardContent>
      </Card>
    )
  }

  const secuencia: { hora: string; label: string; tipo: string; punto?: RecorridoPunto }[] = []
  if (data.inicio?.timestamp) {
    secuencia.push({
      hora: formatHora(data.inicio.timestamp),
      label: "Inicio jornada",
      tipo: "inicio",
    })
  }
  for (const p of puntosVisibles) {
    secuencia.push({
      hora: formatHora(p.timestamp),
      label: p.cliente || p.tipo,
      tipo: p.tipo,
      punto: p,
    })
  }

  const desv =
    data.desviacion_km >= 0 ? `+${data.desviacion_km}` : String(data.desviacion_km)

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap gap-4 items-center text-sm">
        <span>
          GPS: <strong>{data.km_gps}</strong> km · Planificado:{" "}
          <strong>{data.km_ruta_planificada}</strong> km · Desviación:{" "}
          <strong className={data.desviacion_km > 5 ? "text-amber-700" : ""}>{desv}</strong> km
        </span>
        {data.promedio_minutos_entre_visitas != null ? (
          <span>
            Promedio entre visitas: <strong>{data.promedio_minutos_entre_visitas}</strong> min
          </span>
        ) : null}
      </div>

      <div className="flex flex-wrap gap-4 items-center">
        <div className="flex gap-3">
          <button
            type="button"
            className={cn(
              "rounded-md border px-3 py-1 text-sm",
              modo === "resumido" && "bg-primary text-primary-foreground",
            )}
            onClick={() => setModo("resumido")}
          >
            Modo resumido
          </button>
          <button
            type="button"
            className={cn(
              "rounded-md border px-3 py-1 text-sm",
              modo === "completo" && "bg-primary text-primary-foreground",
            )}
            onClick={() => setModo("completo")}
          >
            Modo completo
          </button>
        </div>
        <div className="flex flex-wrap gap-3">
          <label className="flex items-center gap-2 text-sm">
            <Checkbox checked={showVisitas} onCheckedChange={(c) => setShowVisitas(c === true)} />
            Visitas
          </label>
          <label className="flex items-center gap-2 text-sm">
            <Checkbox
              checked={showIncidencias}
              onCheckedChange={(c) => setShowIncidencias(c === true)}
            />
            Incidencias
          </label>
          <label className="flex items-center gap-2 text-sm">
            <Checkbox
              checked={showGps}
              disabled={modo === "resumido"}
              onCheckedChange={(c) => setShowGps(c === true)}
            />
            GPS track ({data.linea_gps?.length ?? 0})
          </label>
          <label className="flex items-center gap-2 text-sm">
            <Checkbox
              checked={showHeartbeat}
              disabled={modo === "resumido"}
              onCheckedChange={(c) => setShowHeartbeat(c === true)}
            />
            Heartbeat ({data.linea_heartbeat?.length ?? 0})
          </label>
        </div>
      </div>

      <div className="grid gap-4 lg:grid-cols-[280px_1fr]">
        <Card className="lg:max-h-[520px] overflow-hidden flex flex-col">
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Secuencia del día</CardTitle>
          </CardHeader>
          <CardContent className="flex-1 overflow-y-auto space-y-1 pr-2">
            {secuencia.map((s, i) => (
              <button
                key={`${s.tipo}-${i}`}
                type="button"
                className={cn(
                  "w-full text-left rounded-md border px-2 py-2 text-sm hover:bg-muted/80",
                  s.tipo === "incidencia" && "border-amber-200 bg-amber-50/50",
                )}
                onClick={() => s.punto && setFocus({ lat: s.punto.lat, lon: s.punto.lon })}
              >
                <span className="font-mono text-xs text-muted-foreground">{s.hora}</span>
                <span className="mx-1">→</span>
                {s.label}
              </button>
            ))}
            {data.intervalos_entre_visitas?.length ? (
              <div className="mt-3 border-t pt-2 space-y-1">
                <p className="text-xs font-medium text-muted-foreground">Tiempo entre visitas</p>
                {data.intervalos_entre_visitas.map((iv, i) => (
                  <p key={i} className="text-xs">
                    {formatHora(iv.desde_ts)} → {formatHora(iv.hasta_ts)}:{" "}
                    <strong>{iv.minutos} min</strong> ({iv.desde_cliente} → {iv.hasta_cliente})
                  </p>
                ))}
              </div>
            ) : null}
          </CardContent>
        </Card>

        <Card>
          <CardContent className="pt-4">
            <div className="h-[420px] overflow-hidden rounded-lg ring-1 ring-border">
              <MapContainer center={center} zoom={13} className="h-full w-full" scrollWheelZoom>
                <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
                {focus ? <FlyTo lat={focus.lat} lon={focus.lon} /> : null}
                {lineas.map((ln, i) =>
                  ln.pts.length > 1 ? (
                    <Polyline
                      key={i}
                      positions={ln.pts}
                      pathOptions={{ color: ln.color, weight: ln.weight, opacity: 0.75 }}
                    />
                  ) : null,
                )}
                {modo === "resumido" || showVisitas || showIncidencias ? (
                  <>
                    {data.inicio?.lat != null && data.inicio.lon != null ? (
                      <Marker position={[data.inicio.lat, data.inicio.lon]} icon={iconInicio}>
                        <Popup>Inicio</Popup>
                      </Marker>
                    ) : null}
                    <MarkerClusterGroup
                      chunkedLoading
                      showCoverageOnHover={false}
                      iconCreateFunction={recorridoClusterIcon}
                    >
                      {puntosVisibles.map((p) => (
                        <Marker
                          key={`${p.orden}-${p.visita_id}`}
                          position={[p.lat, p.lon]}
                          icon={numberedIcon(p.orden, puntoColor(p.tipo))}
                        >
                          <Popup>
                            <strong>{p.cliente}</strong>
                            <br />
                            {formatHora(p.timestamp)} — {p.tipo}
                          </Popup>
                        </Marker>
                      ))}
                    </MarkerClusterGroup>
                    {data.ultima_posicion?.lat != null && data.ultima_posicion.lon != null ? (
                      <Marker
                        position={[data.ultima_posicion.lat, data.ultima_posicion.lon]}
                        icon={iconUltima}
                      >
                        <Popup>Última posición</Popup>
                      </Marker>
                    ) : null}
                  </>
                ) : null}
              </MapContainer>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
