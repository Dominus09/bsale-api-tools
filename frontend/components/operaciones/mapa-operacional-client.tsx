"use client"

import { useEffect, useMemo, useState } from "react"
import Link from "next/link"
import { useSearchParams } from "next/navigation"
import { MapContainer, Marker, Popup, TileLayer } from "react-leaflet"
import L from "leaflet"
import "leaflet/dist/leaflet.css"

import {
  getOperacionesMapaGlobal,
  getOperacionesRuta,
  localIsoDate,
  type MapaGlobalVendedor,
  type MarcadorMapa,
} from "@/services/operaciones"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Skeleton } from "@/components/ui/skeleton"
import { Button } from "@/components/ui/button"

const iconVisitado = new L.DivIcon({
  className: "",
  html: '<div style="width:14px;height:14px;border-radius:50%;background:#22c55e;border:2px solid #fff"></div>',
  iconSize: [14, 14],
  iconAnchor: [7, 7],
})
const iconPendiente = new L.DivIcon({
  className: "",
  html: '<div style="width:14px;height:14px;border-radius:50%;background:#eab308;border:2px solid #fff"></div>',
  iconSize: [14, 14],
  iconAnchor: [7, 7],
})
const iconIncidencia = new L.DivIcon({
  className: "",
  html: '<div style="width:14px;height:14px;border-radius:50%;background:#ef4444;border:2px solid #fff"></div>',
  iconSize: [14, 14],
  iconAnchor: [7, 7],
})

function vendedorIcon(color: string) {
  return new L.DivIcon({
    className: "",
    html: `<div style="width:20px;height:20px;border-radius:50%;background:${color};border:3px solid #fff;box-shadow:0 2px 6px rgba(0,0,0,.35)"></div>`,
    iconSize: [20, 20],
    iconAnchor: [10, 10],
  })
}

function markerIcon(estado: MarcadorMapa["estado"]) {
  if (estado === "visitado") return iconVisitado
  if (estado === "incidencia") return iconIncidencia
  return iconPendiente
}

export default function MapaOperacionalClient() {
  const searchParams = useSearchParams()
  const rutaParam = searchParams.get("ruta")
  const rutaId = rutaParam ? parseInt(rutaParam, 10) : null

  const [fecha, setFecha] = useState(localIsoDate())
  const [global, setGlobal] = useState<Awaited<ReturnType<typeof getOperacionesMapaGlobal>> | null>(null)
  const [rutaData, setRutaData] = useState<Awaited<ReturnType<typeof getOperacionesRuta>> | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    if (rutaId && !Number.isNaN(rutaId)) {
      getOperacionesRuta(rutaId)
        .then(setRutaData)
        .catch((e) => setError(e instanceof Error ? e.message : "Error"))
        .finally(() => setLoading(false))
      return
    }
    getOperacionesMapaGlobal(fecha)
      .then(setGlobal)
      .catch((e) => setError(e instanceof Error ? e.message : "Error"))
      .finally(() => setLoading(false))
  }, [rutaId, fecha])

  const center = useMemo((): [number, number] => {
    if (rutaData?.vendedor_ubicacion) {
      return [rutaData.vendedor_ubicacion.lat, rutaData.vendedor_ubicacion.lon]
    }
    const g = global?.vendedores?.[0]
    if (g) return [g.lat, g.lon]
    const m = rutaData?.marcadores?.[0]
    if (m) return [m.lat, m.lon]
    return [-33.45, -70.65]
  }, [global, rutaData])

  if (loading) return <Skeleton className="h-[75vh] w-full rounded-xl" />

  if (error) {
    return (
      <Card>
        <CardContent className="py-12 text-center text-muted-foreground">{error}</CardContent>
      </Card>
    )
  }

  if (rutaId && rutaData) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>
            Ruta #{rutaId} — {rutaData.vendedor_nombre || rutaData.vendedor}
          </CardTitle>
          <Button variant="link" className="h-auto p-0" asChild>
            <Link href="/operaciones/mapa">← Mapa global</Link>
          </Button>
        </CardHeader>
        <CardContent>
          <div className="h-[75vh] overflow-hidden rounded-lg ring-1 ring-border">
            <MapContainer center={center} zoom={12} className="h-full w-full" scrollWheelZoom>
              <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
              {rutaData.marcadores.map((m) => (
                <Marker key={m.visita_id} position={[m.lat, m.lon]} icon={markerIcon(m.estado)}>
                  <Popup>
                    <strong>{m.nombre_fantasia || m.cliente_id}</strong>
                    <br />
                    {m.estado}
                  </Popup>
                </Marker>
              ))}
            </MapContainer>
          </div>
        </CardContent>
      </Card>
    )
  }

  const vendedores = global?.vendedores ?? []

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-end gap-4">
        <div className="space-y-2">
          <Label htmlFor="mapa-fecha">Fecha</Label>
          <Input
            id="mapa-fecha"
            type="date"
            value={fecha}
            onChange={(e) => setFecha(e.target.value)}
            className="w-[160px]"
          />
        </div>
        <p className="text-sm text-muted-foreground pb-2">
          {vendedores.length} vendedor(es) con ubicación reportada
        </p>
      </div>

      <Card>
        <CardContent className="pt-6">
          {vendedores.length === 0 ? (
            <p className="py-12 text-center text-muted-foreground">
              Sin posiciones GPS para esta fecha.
            </p>
          ) : (
            <div className="h-[75vh] overflow-hidden rounded-lg ring-1 ring-border">
              <MapContainer center={center} zoom={11} className="h-full w-full" scrollWheelZoom>
                <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
                {vendedores.map((v: MapaGlobalVendedor) => (
                  <Marker key={v.codigo} position={[v.lat, v.lon]} icon={vendedorIcon(v.color)}>
                    <Popup>
                      <strong>{v.nombre}</strong>
                      <br />
                      Código: {v.codigo}
                      <br />
                      Último envío:{" "}
                      {v.ultima_sync
                        ? new Date(v.ultima_sync).toLocaleString("es-CL")
                        : "—"}
                      <br />
                      Batería: {v.bateria_pct != null ? `${v.bateria_pct}%` : "—"}
                      <br />
                      Visitados: {v.visitas_realizadas} · Incidencias: {v.incidencias}
                      <br />
                      Km GPS: {v.km_gps}
                      <br />
                      <Link
                        href={`/operaciones/vendedor/${encodeURIComponent(v.codigo)}`}
                        className="text-primary underline text-sm"
                      >
                        Ver detalle
                      </Link>
                    </Popup>
                  </Marker>
                ))}
              </MapContainer>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
