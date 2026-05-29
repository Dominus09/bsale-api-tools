"use client"

import dynamic from "next/dynamic"
import { useCallback, useEffect, useState } from "react"
import Link from "next/link"
import { useParams } from "next/navigation"
import { ArrowLeft, Loader2 } from "lucide-react"

import { EstadoConexionBadge } from "@/components/operaciones/estado-badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { getOperacionesVendedor, localIsoDate, type VendedorDetalleResponse } from "@/services/operaciones"

const RecorridoVendedorMapa = dynamic(
  () => import("@/components/operaciones/recorrido-vendedor-mapa"),
  { ssr: false, loading: () => <div className="h-[420px] animate-pulse rounded-xl bg-muted" /> },
)

export default function OperacionesVendedorDetallePage() {
  const params = useParams()
  const codigo = decodeURIComponent(String(params.codigo ?? ""))
  const [fecha, setFecha] = useState(localIsoDate())
  const [data, setData] = useState<VendedorDetalleResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    try {
      const res = await getOperacionesVendedor(codigo, fecha)
      setData(res)
      setError(null)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error")
      setData(null)
    } finally {
      setLoading(false)
    }
  }, [codigo, fecha])

  useEffect(() => {
    void load()
  }, [load])

  const m = data?.metricas

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center gap-3">
        <Button variant="ghost" size="sm" asChild>
          <Link href="/operaciones/vendedores">
            <ArrowLeft className="mr-1 h-4 w-4" />
            Vendedores
          </Link>
        </Button>
        <Input type="date" value={fecha} onChange={(e) => setFecha(e.target.value)} className="w-[160px]" />
      </div>

      {loading ? (
        <div className="flex justify-center py-16">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : error ? (
        <p className="text-destructive">{error}</p>
      ) : data ? (
        <>
          <div className="flex flex-wrap items-center gap-3">
            <h1 className="text-2xl font-bold">{data.nombre}</h1>
            <EstadoConexionBadge estado={data.estado_conexion} />
            <span className="text-sm text-muted-foreground">{data.codigo}</span>
          </div>

          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm text-muted-foreground">Cumplimiento</CardTitle>
              </CardHeader>
              <CardContent className="text-2xl font-bold">{data.porcentaje_cumplimiento}%</CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm text-muted-foreground">Km recorridos (GPS)</CardTitle>
              </CardHeader>
              <CardContent className="text-2xl font-bold">{data.kilometros_recorridos}</CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm text-muted-foreground">Visitados</CardTitle>
              </CardHeader>
              <CardContent className="text-2xl font-bold">
                {m?.visitados ?? "—"} / {m?.clientes_asignados ?? "—"}
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm text-muted-foreground">Puntos GPS</CardTitle>
              </CardHeader>
              <CardContent className="text-2xl font-bold">{m?.gps_puntos_recibidos ?? 0}</CardContent>
            </Card>
          </div>

          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm text-muted-foreground">Incidencias</CardTitle>
              </CardHeader>
              <CardContent className="text-xl font-semibold">{m?.incidencias ?? data.incidencias.length}</CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm text-muted-foreground">Primera visita</CardTitle>
              </CardHeader>
              <CardContent className="text-sm">
                {m?.primera_visita
                  ? new Date(m.primera_visita).toLocaleTimeString("es-CL")
                  : data.hora_inicio
                    ? new Date(data.hora_inicio).toLocaleString("es-CL")
                    : "—"}
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm text-muted-foreground">Última visita</CardTitle>
              </CardHeader>
              <CardContent className="text-sm">
                {m?.ultima_visita
                  ? new Date(m.ultima_visita).toLocaleTimeString("es-CL")
                  : "—"}
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm text-muted-foreground">Tiempo activo</CardTitle>
              </CardHeader>
              <CardContent className="text-sm">
                {m?.tiempo_activo_minutos != null ? `${m.tiempo_activo_minutos} min` : "—"}
              </CardContent>
            </Card>
          </div>

          <RecorridoVendedorMapa codigo={codigo} fecha={fecha} />

          <Card>
            <CardHeader>
              <CardTitle>Timeline del día</CardTitle>
            </CardHeader>
            <CardContent className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>#</TableHead>
                    <TableHead>Cliente</TableHead>
                    <TableHead>Estado</TableHead>
                    <TableHead>Hora</TableHead>
                    <TableHead>Sync</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {data.timeline.map((v) => (
                    <TableRow key={v.id}>
                      <TableCell>{v.orden_ruta}</TableCell>
                      <TableCell>{v.nombre_fantasia || v.cliente_id}</TableCell>
                      <TableCell>{v.estado}</TableCell>
                      <TableCell>
                        {v.fecha_hora_visita
                          ? new Date(v.fecha_hora_visita).toLocaleTimeString("es-CL")
                          : "—"}
                      </TableCell>
                      <TableCell className="text-xs">{v.sync_status}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>

          {data.incidencias.length > 0 ? (
            <Card>
              <CardHeader>
                <CardTitle>Incidencias ({data.incidencias.length})</CardTitle>
              </CardHeader>
              <CardContent>
                <ul className="space-y-2 text-sm">
                  {data.incidencias.map((inc) => (
                    <li key={inc.id} className="rounded-md border p-3">
                      <strong>{inc.tipo_incidencia}</strong> — {inc.nombre_fantasia || inc.cliente_id}
                      {inc.observacion ? <p className="text-muted-foreground">{inc.observacion}</p> : null}
                    </li>
                  ))}
                </ul>
              </CardContent>
            </Card>
          ) : null}
        </>
      ) : null}
    </div>
  )
}
