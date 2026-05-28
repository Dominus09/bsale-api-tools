"use client"

import { useCallback, useState } from "react"
import { Loader2 } from "lucide-react"

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
import { useOperacionesPoll } from "@/hooks/use-operaciones-poll"
import {
  getOperacionesGeorefPendientes,
  patchOperacionesGeorefEstado,
  type ClienteGeorefRow,
} from "@/services/operaciones"

function GeorefEstadoBadge({ estado }: { estado: string }) {
  const e = estado.toLowerCase()
  const cls =
    e === "aplicada"
      ? "bg-emerald-100 text-emerald-800 dark:bg-emerald-950 dark:text-emerald-200"
      : e === "capturada"
        ? "bg-sky-100 text-sky-800 dark:bg-sky-950 dark:text-sky-200"
        : "bg-amber-100 text-amber-900 dark:bg-amber-950 dark:text-amber-200"
  return (
    <span className={`inline-flex rounded px-2 py-0.5 text-xs font-medium ${cls}`}>
      {estado}
    </span>
  )
}

function formatCoords(row: ClienteGeorefRow) {
  if (row.lat == null || row.lon == null) return "—"
  return `${row.lat.toFixed(6)}, ${row.lon.toFixed(6)}`
}

export default function OperacionesGeorreferenciasPage() {
  const [vendedor, setVendedor] = useState("")
  const [busyId, setBusyId] = useState<number | null>(null)
  const [actionError, setActionError] = useState<string | null>(null)

  const loader = useCallback(
    () => getOperacionesGeorefPendientes({ vendedor: vendedor || undefined, vista: "erp" }),
    [vendedor],
  )
  const { data, loading, error, refresh } = useOperacionesPoll(loader, [vendedor])

  async function onEstado(row: ClienteGeorefRow, estado: "pendiente" | "aplicada") {
    setActionError(null)
    setBusyId(row.ruta_id)
    try {
      await patchOperacionesGeorefEstado(row.ruta_id, estado)
      await refresh()
    } catch (e) {
      setActionError(e instanceof Error ? e.message : "Error al actualizar estado")
    } finally {
      setBusyId(null)
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Georreferencias</h1>
        <p className="text-sm text-muted-foreground">
          Capa operacional <code className="rounded bg-muted px-1 text-xs">bsale.rutero</code> — la app
          guarda en <code className="rounded bg-muted px-1 text-xs">lat_operacional</code>; las columnas
          mostradas son coordenadas efectivas (operacional o réplica BSALE). Marcar aplicada tras actualizar
          BSALE manualmente.
        </p>
      </div>

      <div className="flex flex-wrap gap-2">
        <Input
          placeholder="Filtrar vendedor (código)"
          value={vendedor}
          onChange={(e) => setVendedor(e.target.value)}
          className="max-w-xs"
        />
      </div>

      {error ? <p className="text-sm text-destructive">{error}</p> : null}
      {actionError ? <p className="text-sm text-destructive">{actionError}</p> : null}

      <Card>
        <CardHeader>
          <CardTitle>Clientes ({data?.total ?? 0})</CardTitle>
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
                  <TableHead>Cliente</TableHead>
                  <TableHead>Vendedor</TableHead>
                  <TableHead>Dirección</TableHead>
                  <TableHead>Estado</TableHead>
                  <TableHead>Coordenadas</TableHead>
                  <TableHead>Actualizado</TableHead>
                  <TableHead className="text-right">Acciones</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {(data?.items ?? []).map((row) => (
                  <TableRow key={row.ruta_id}>
                    <TableCell>
                      <div className="font-medium">{row.cliente_nombre}</div>
                      <div className="text-xs text-muted-foreground">{row.cliente_codigo}</div>
                    </TableCell>
                    <TableCell>{row.vendedor_codigo || "—"}</TableCell>
                    <TableCell className="max-w-[220px] truncate">{row.direccion || "—"}</TableCell>
                    <TableCell>
                      <GeorefEstadoBadge estado={row.georef_estado} />
                    </TableCell>
                    <TableCell className="font-mono text-xs whitespace-nowrap">
                      {formatCoords(row)}
                    </TableCell>
                    <TableCell className="text-xs text-muted-foreground whitespace-nowrap">
                      {row.georef_actualizada_at
                        ? new Date(row.georef_actualizada_at).toLocaleString("es-CL")
                        : "—"}
                      {row.georef_actualizada_por ? (
                        <div>{row.georef_actualizada_por}</div>
                      ) : null}
                    </TableCell>
                    <TableCell className="text-right space-x-1">
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={busyId === row.ruta_id || row.georef_estado === "aplicada"}
                        onClick={() => onEstado(row, "aplicada")}
                      >
                        Marcar aplicada
                      </Button>
                      <Button
                        size="sm"
                        variant="ghost"
                        disabled={busyId === row.ruta_id || row.georef_estado === "pendiente"}
                        onClick={() => onEstado(row, "pendiente")}
                      >
                        Volver pendiente
                      </Button>
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
