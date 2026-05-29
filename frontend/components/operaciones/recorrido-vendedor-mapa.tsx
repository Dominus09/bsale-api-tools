"use client"

import { useEffect, useMemo, useState } from "react"
import { MapContainer, Marker, Polyline, Popup, TileLayer, useMap } from "react-leaflet"
import L from "leaflet"
import "leaflet/dist/leaflet.css"

import {
  getOperacionesVendedorRecorrido,
  type RecorridoPunto,
  type VendedorRecorridoResponse,
} from "@/services/operaciones"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Skeleton } from "@/components/ui/skeleton"
import { cn } from "@/lib/utils"

function numberedIcon(n: number, bg: string) {
  return new L.DivIcon({
    className: "",
    html: `<div style="width:26px;height:26px;border-radius:50%;background:${bg};color:#fff;font-size:12px;font-weight:700;display:flex;align-items:center;justify-content:center;border:2px solid #fff;box-shadow:0 1px 4px rgba(0,0,0,.35)">${n}</div>`,
    iconSize: [26, 26],
    iconAnchor: [13, 13],
  })
}

const iconInicio = new L.DivIcon({
  className: "",
  html: '<div style="width:16px;height:16px;border-radius:50%;background:#22c55e;border:3px solid #fff;box-shadow:0 1px 4px rgba(0,0,0,.35)"></div>',
  iconSize: [16, 16],
  iconAnchor: [8, 8],
})

const iconUltima = new L.DivIcon({
  className: "",
  html: '<div style="width:18px;height:18px;border-radius:50%;background:#ef4444;border:3px solid #fff;box-shadow:0 2px 6px rgba(0,0,0,.4)"></div>',
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

  useEffect(() => {
    setLoading(true)
    getOperacionesVendedorRecorrido(codigo, fecha)
      .then(setData)
      .catch((e) => setError(e instanceof Error ? e.message : "Error"))
      .finally(() => setLoading(false))
  }, [codigo, fecha])

  const center = useMemo((): [number, number] => {
    if (focus) return [focus.lat, focus.lon]
    const p = data?.puntos?.[0]
    if (p) return [p.lat, p.lon]
    if (data?.ultima_posicion?.lat != null && data.ultima_posicion.lon != null) {
      return [data.ultima_posicion.lat, data.ultima_posicion.lon]
    }
    return [-33.45, -70.65]
  }, [data, focus])

  const linea = useMemo(() => {
    const pts: [number, number][] = []
    if (data?.linea_gps?.length) {
      for (const g of data.linea_gps) pts.push([g.lat, g.lon])
    } else if (data?.puntos?.length) {
      for (const p of data.puntos) pts.push([p.lat, p.lon])
    }
    return pts
  }, [data])

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
  for (const p of data.puntos) {
    secuencia.push({
      hora: formatHora(p.timestamp),
      label: p.cliente || p.tipo,
      tipo: p.tipo,
      punto: p,
    })
  }

  return (
    <div className="grid gap-4 lg:grid-cols-[280px_1fr]">
      <Card className="lg:max-h-[520px] overflow-hidden flex flex-col">
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Secuencia del día</CardTitle>
          <p className="text-xs text-muted-foreground">Clic para centrar el mapa</p>
        </CardHeader>
        <CardContent className="flex-1 overflow-y-auto space-y-1 pr-2">
          {secuencia.length === 0 ? (
            <p className="text-sm text-muted-foreground">Sin eventos con ubicación.</p>
          ) : (
            secuencia.map((s, i) => (
              <button
                key={`${s.tipo}-${i}`}
                type="button"
                className={cn(
                  "w-full text-left rounded-md border px-2 py-2 text-sm hover:bg-muted/80 transition-colors",
                  s.tipo === "incidencia" && "border-amber-200 bg-amber-50/50",
                )}
                onClick={() => s.punto && setFocus({ lat: s.punto.lat, lon: s.punto.lon })}
              >
                <span className="font-mono text-xs text-muted-foreground">{s.hora}</span>
                <span className="mx-1">→</span>
                <span>{s.label}</span>
                {s.tipo === "incidencia" ? (
                  <span className="ml-1 text-xs text-amber-700">(incidencia)</span>
                ) : null}
              </button>
            ))
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Recorrido en mapa</CardTitle>
          <p className="text-xs text-muted-foreground">
            Verde inicio · Azul visitas · Amarillo incidencias · Rojo última posición · Km GPS:{" "}
            {data.km_recorridos}
          </p>
        </CardHeader>
        <CardContent>
          <div className="h-[420px] overflow-hidden rounded-lg ring-1 ring-border">
            <MapContainer center={center} zoom={13} className="h-full w-full" scrollWheelZoom>
              <TileLayer
                attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a>'
                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
              />
              {focus ? <FlyTo lat={focus.lat} lon={focus.lon} /> : null}
              {linea.length > 1 ? (
                <Polyline positions={linea} pathOptions={{ color: "#6366f1", weight: 4, opacity: 0.7 }} />
              ) : null}
              {data.inicio?.lat != null && data.inicio.lon != null ? (
                <Marker position={[data.inicio.lat, data.inicio.lon]} icon={iconInicio}>
                  <Popup>Inicio ({data.inicio.fuente})</Popup>
                </Marker>
              ) : null}
              {data.puntos.map((p) => (
                <Marker
                  key={`${p.orden}-${p.visita_id}`}
                  position={[p.lat, p.lon]}
                  icon={numberedIcon(p.orden, puntoColor(p.tipo))}
                >
                  <Popup>
                    <strong>{p.cliente}</strong>
                    <br />
                    {formatHora(p.timestamp)} — {p.tipo}
                    {p.detalle ? <br /> : null}
                    {p.detalle}
                  </Popup>
                </Marker>
              ))}
              {data.ultima_posicion?.lat != null && data.ultima_posicion.lon != null ? (
                <Marker
                  position={[data.ultima_posicion.lat, data.ultima_posicion.lon]}
                  icon={iconUltima}
                >
                  <Popup>Última posición ({data.ultima_posicion.fuente})</Popup>
                </Marker>
              ) : null}
            </MapContainer>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}
