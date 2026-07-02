"use client"

import dynamic from "next/dynamic"
import { useEffect, useMemo, useState } from "react"
import { Loader2 } from "lucide-react"

import { getCommercialMap, type CommercialAnalyticsParams, type CommercialMapPoint } from "@/lib/api"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"

const MapContainer = dynamic(() => import("react-leaflet").then((m) => m.MapContainer), { ssr: false })
const TileLayer = dynamic(() => import("react-leaflet").then((m) => m.TileLayer), { ssr: false })
const CircleMarker = dynamic(() => import("react-leaflet").then((m) => m.CircleMarker), { ssr: false })
const Popup = dynamic(() => import("react-leaflet").then((m) => m.Popup), { ssr: false })

import "leaflet/dist/leaflet.css"

const MAP_CENTER: [number, number] = [-41.47, -72.94]

const ESTADO_COLORS: Record<string, string> = {
  saludable: "#22c55e",
  riesgo: "#f59e0b",
  perdido: "#ef4444",
  nuevo: "#3b82f6",
  recuperado: "#8b5cf6",
}

function formatCLP(n: number): string {
  return n.toLocaleString("es-CL", { style: "currency", currency: "CLP", maximumFractionDigits: 0 })
}

export function CommercialMapClient({
  params,
  onClientClick,
  height = 420,
}: {
  params: CommercialAnalyticsParams
  onClientClick?: (clientId: number) => void
  height?: number
}) {
  const [points, setPoints] = useState<CommercialMapPoint[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    void getCommercialMap({ ...params, limit: 500 })
      .then((r) => setPoints(r.items))
      .catch((e: unknown) => setError(e instanceof Error ? e.message : "Error al cargar mapa"))
      .finally(() => setLoading(false))
  }, [params])

  const center = useMemo((): [number, number] => {
    if (points.length === 0) return MAP_CENTER
    const lat = points.reduce((s, p) => s + p.lat, 0) / points.length
    const lng = points.reduce((s, p) => s + p.lng, 0) / points.length
    return [lat, lng]
  }, [points])

  return (
    <Card className="overflow-hidden">
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <CardTitle className="text-base">Mapa Comercial</CardTitle>
          <Badge variant="secondary">{points.length} con georef</Badge>
        </div>
      </CardHeader>
      <CardContent className="p-0">
        {loading ? (
          <div className="flex items-center justify-center" style={{ height }}>
            <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
          </div>
        ) : error ? (
          <p className="p-6 text-center text-sm text-destructive">{error}</p>
        ) : points.length === 0 ? (
          <p className="p-6 text-center text-sm text-muted-foreground">Sin clientes georreferenciados en el período</p>
        ) : (
          <div style={{ height }} className="w-full">
            <MapContainer center={center} zoom={11} className="h-full w-full z-0" scrollWheelZoom>
              <TileLayer
                attribution='&copy; CARTO'
                url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
              />
              {points.map((p) => (
                <CircleMarker
                  key={p.client_id}
                  center={[p.lat, p.lng]}
                  radius={p.cliente_vip ? 10 : 7}
                  pathOptions={{
                    color: ESTADO_COLORS[p.estado] ?? "#64748b",
                    fillColor: ESTADO_COLORS[p.estado] ?? "#64748b",
                    fillOpacity: 0.75,
                    weight: p.prioridad === "alta" ? 2 : 1,
                  }}
                  eventHandlers={{
                    click: () => onClientClick?.(p.client_id),
                  }}
                >
                  <Popup>
                    <div className="min-w-[180px] space-y-1 text-sm">
                      <p className="font-semibold">{p.nombre}</p>
                      <p className="text-muted-foreground">{p.vendedor}</p>
                      <div className="flex flex-wrap gap-1">
                        <Badge variant="outline">{p.estado}</Badge>
                        {p.cliente_vip && <Badge>VIP</Badge>}
                        <Badge variant="secondary">Score {p.score}</Badge>
                      </div>
                      <p>Potencial: {formatCLP(p.potencial)}</p>
                      <p className="text-xs">Prob. compra: {p.purchase_probability}%</p>
                    </div>
                  </Popup>
                </CircleMarker>
              ))}
            </MapContainer>
          </div>
        )}
        <div className="flex flex-wrap gap-3 border-t px-4 py-2 text-xs text-muted-foreground">
          {Object.entries(ESTADO_COLORS).map(([k, c]) => (
            <span key={k} className="flex items-center gap-1">
              <span className="inline-block h-2.5 w-2.5 rounded-full" style={{ backgroundColor: c }} />
              {k}
            </span>
          ))}
        </div>
      </CardContent>
    </Card>
  )
}
