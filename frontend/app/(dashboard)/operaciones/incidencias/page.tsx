"use client"

import { useCallback, useState } from "react"
import { Loader2 } from "lucide-react"

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
import { IncidenciaFoto } from "@/components/operaciones/incidencia-foto"
import { useOperacionesPoll } from "@/hooks/use-operaciones-poll"
import { getOperacionesIncidencias, localIsoDate } from "@/services/operaciones"

export default function OperacionesIncidenciasPage() {
  const [fecha, setFecha] = useState(localIsoDate())
  const [vendedor, setVendedor] = useState("")
  const loader = useCallback(
    () => getOperacionesIncidencias({ fecha, vendedor: vendedor || undefined }),
    [fecha, vendedor],
  )
  const { data, loading, error } = useOperacionesPoll(loader, [fecha, vendedor])

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Incidencias</h1>
        <p className="text-sm text-muted-foreground">Visitas con estado incidencia del día</p>
      </div>
      <div className="flex flex-wrap gap-2">
        <Input type="date" value={fecha} onChange={(e) => setFecha(e.target.value)} className="w-[160px]" />
        <Input
          placeholder="Filtrar vendedor (código)"
          value={vendedor}
          onChange={(e) => setVendedor(e.target.value)}
          className="max-w-xs"
        />
      </div>
      {error ? <p className="text-sm text-destructive">{error}</p> : null}
      <Card>
        <CardHeader>
          <CardTitle>Registros ({data?.total ?? 0})</CardTitle>
        </CardHeader>
        <CardContent>
          {loading && !data ? (
            <div className="flex justify-center py-12">
              <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Vendedor</TableHead>
                  <TableHead>Cliente</TableHead>
                  <TableHead>Comuna</TableHead>
                  <TableHead>Tipo</TableHead>
                  <TableHead>Comentario</TableHead>
                  <TableHead>Hora</TableHead>
                  <TableHead>Foto</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {(data?.items ?? []).map((row) => (
                  <TableRow key={row.id}>
                    <TableCell>
                      {row.vendedor_nombre || row.vendedor}
                      <div className="text-xs text-muted-foreground">{row.vendedor}</div>
                    </TableCell>
                    <TableCell>{row.nombre_fantasia || row.cliente_id}</TableCell>
                    <TableCell>{row.comuna || "—"}</TableCell>
                    <TableCell>{row.tipo_incidencia || "—"}</TableCell>
                    <TableCell className="max-w-[240px] truncate">{row.observacion || "—"}</TableCell>
                    <TableCell className="text-sm whitespace-nowrap">
                      {row.fecha_hora_visita
                        ? new Date(row.fecha_hora_visita).toLocaleString("es-CL")
                        : "—"}
                    </TableCell>
                    <TableCell>
                      <IncidenciaFoto
                        visitaId={row.id}
                        fotoUrl={row.foto_url}
                        tieneFoto={row.tiene_foto}
                        alt={row.nombre_fantasia || row.cliente_id}
                      />
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
