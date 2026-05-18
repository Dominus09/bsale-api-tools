"use client"

import { useEffect, useMemo, useState } from "react"
import { useSearchParams } from "next/navigation"
import { MapContainer, Marker, Popup, TileLayer } from "react-leaflet"
import L from "leaflet"
import "leaflet/dist/leaflet.css"

import { getOperacionesRuta, type MarcadorMapa } from "@/services/operaciones"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Skeleton } from "@/components/ui/skeleton"

const iconVisitado = new L.DivIcon({
  className: "",
  html: '<div style="width:14px;height:14px;border-radius:50%;background:#22c55e;border:2px solid #fff;box-shadow:0 1px 4px rgba(0,0,0,.35)"></div>',
  iconSize: [14, 14],
  iconAnchor: [7, 7],
})
const iconPendiente = new L.DivIcon({
  className: "",
  html: '<div style="width:14px;height:14px;border-radius:50%;background:#eab308;border:2px solid #fff;box-shadow:0 1px 4px rgba(0,0,0,.35)"></div>',
  iconSize: [14, 14],
  iconAnchor: [7, 7],
})
const iconIncidencia = new L.DivIcon({
  className: "",
  html: '<div style="width:14px;height:14px;border-radius:50%;background:#ef4444;border:2px solid #fff;box-shadow:0 1px 4px rgba(0,0,0,.35)"></div>',
  iconSize: [14, 14],
  iconAnchor: [7, 7],
})
const iconVendedor = new L.DivIcon({
  className: "",
  html: '<div style="width:18px;height:18px;border-radius:50%;background:#3b82f6;border:3px solid #fff;box-shadow:0 2px 6px rgba(0,0,0,.4)"></div>',
  iconSize: [18, 18],
  iconAnchor: [9, 9],
})

function markerIcon(estado: MarcadorMapa["estado"]) {
  if (estado === "visitado") return iconVisitado
  if (estado === "incidencia") return iconIncidencia
  return iconPendiente
}

export default function MapaOperacionalClient() {
  const searchParams = useSearchParams()
  const rutaParam = searchParams.get("ruta")
  const rutaId = rutaParam ? parseInt(rutaParam, 10) : null

  const [data, setData] = useState<Awaited<ReturnType<typeof getOperacionesRuta>> | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!rutaId || Number.isNaN(rutaId)) {
      setLoading(false)
      setError("Indique ?ruta=<id> en la URL (desde tabla vendedores).")
      return
    }
    setLoading(true)
    getOperacionesRuta(rutaId)
      .then(setData)
      .catch((e) => setError(e instanceof Error ? e.message : "Error"))
      .finally(() => setLoading(false))
  }, [rutaId])

  const center = useMemo((): [number, number] => {
    if (data?.vendedor_ubicacion) return [data.vendedor_ubicacion.lat, data.vendedor_ubicacion.lon]
    const m = data?.marcadores?.[0]
    if (m) return [m.lat, m.lon]
    return [-33.45, -70.65]
  }, [data])

  if (loading) {
    return <Skeleton className="h-[75vh] w-full rounded-xl" />
  }

  if (error || !data) {
    return (
      <Card>
        <CardContent className="py-12 text-center text-muted-foreground">{error || "Sin datos"}</CardContent>
      </Card>
    )
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle>
          Mapa — {data.vendedor_nombre || data.vendedor} ({data.fecha})
        </CardTitle>
        <p className="text-sm text-muted-foreground">
          Verde visitado · Amarillo pendiente · Rojo incidencia · Azul última posición vendedor
        </p>
      </CardHeader>
      <CardContent>
        <div className="h-[75vh] overflow-hidden rounded-lg ring-1 ring-border">
          <MapContainer center={center} zoom={12} className="h-full w-full" scrollWheelZoom>
            <TileLayer
              attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a>'
              url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
            />
            {data.marcadores.map((m) => (
              <Marker key={m.visita_id} position={[m.lat, m.lon]} icon={markerIcon(m.estado)}>
                <Popup>
                  <strong>{m.nombre_fantasia || m.cliente_id}</strong>
                  <br />
                  {m.estado}
                  {m.tipo_incidencia ? ` — ${m.tipo_incidencia}` : ""}
                </Popup>
              </Marker>
            ))}
            {data.vendedor_ubicacion ? (
              <Marker
                position={[data.vendedor_ubicacion.lat, data.vendedor_ubicacion.lon]}
                icon={iconVendedor}
              >
                <Popup>Vendedor: {data.vendedor_ubicacion.nombre}</Popup>
              </Marker>
            ) : null}
          </MapContainer>
        </div>
      </CardContent>
    </Card>
  )
}
