"use client"

import { useCallback, useEffect, useState } from "react"
import { Download, Loader2, RefreshCw } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  downloadOperacionesGeorefExport,
  getOperacionesGeorefPendientes,
  patchOperacionesGeorefEstado,
  type ClienteGeorefRow,
  type GeorefPendientesResponse,
} from "@/services/operaciones"
import { cn } from "@/lib/utils"

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

function isPendienteOperacional(row: ClienteGeorefRow) {
  const sinCoords = row.lat == null || row.lon == null
  return sinCoords || String(row.georef_estado).toLowerCase() === "pendiente"
}

function KpiCard({ label, value, accent }: { label: string; value: number; accent?: string }) {
  return (
    <div className="rounded-lg border bg-card px-4 py-3">
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className={cn("text-2xl font-semibold tabular-nums", accent)}>{value}</p>
    </div>
  )
}

export default function OperacionesGeorreferenciasPage() {
  const [vendedor, setVendedor] = useState("")
  const [soloPendientes, setSoloPendientes] = useState(true)
  const [data, setData] = useState<GeorefPendientesResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [busyId, setBusyId] = useState<number | null>(null)
  const [exporting, setExporting] = useState(false)
  const [actionError, setActionError] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await getOperacionesGeorefPendientes({
        vendedor: vendedor || undefined,
        vista: "erp",
        solo_pendientes: soloPendientes,
      })
      setData(res)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al cargar georreferencias")
    } finally {
      setLoading(false)
    }
  }, [vendedor, soloPendientes])

  useEffect(() => {
    void load()
  }, [load])

  async function onEstado(row: ClienteGeorefRow, estado: "pendiente" | "aplicada") {
    setActionError(null)
    setBusyId(row.ruta_id)
    try {
      await patchOperacionesGeorefEstado(row.ruta_id, estado)
      await load()
    } catch (e) {
      setActionError(e instanceof Error ? e.message : "Error al actualizar estado")
    } finally {
      setBusyId(null)
    }
  }

  async function onExport() {
    setExporting(true)
    setActionError(null)
    try {
      await downloadOperacionesGeorefExport({
        vendedor: vendedor || undefined,
        solo_pendientes: soloPendientes,
      })
    } catch (e) {
      setActionError(e instanceof Error ? e.message : "Error al exportar")
    } finally {
      setExporting(false)
    }
  }

  const resumen = data?.resumen

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Georreferencias</h1>
          <p className="text-sm text-muted-foreground max-w-2xl">
            Gestión operacional de clientes sin georef efectiva. Coordenadas mostradas =
            operacional o réplica BSALE.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button variant="outline" size="sm" onClick={() => void load()} disabled={loading}>
            <RefreshCw className={cn("mr-2 h-4 w-4", loading && "animate-spin")} />
            Actualizar
          </Button>
          <Button size="sm" onClick={() => void onExport()} disabled={exporting || loading}>
            {exporting ? (
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
            ) : (
              <Download className="mr-2 h-4 w-4" />
            )}
            {soloPendientes ? "Descargar pendientes" : "Descargar CSV"}
          </Button>
        </div>
      </div>

      {resumen ? (
        <div className="grid gap-3 grid-cols-2 sm:grid-cols-4">
          <KpiCard label="Total clientes" value={resumen.total} />
          <KpiCard label="Pendientes georef" value={resumen.pendientes} accent="text-amber-700" />
          <KpiCard label="Capturados" value={resumen.capturados} accent="text-sky-700" />
          <KpiCard label="Aplicados" value={resumen.aplicados} accent="text-emerald-700" />
        </div>
      ) : null}

      <div className="flex flex-wrap items-end gap-4">
        <div className="space-y-2">
          <Label htmlFor="georef-vendedor">Vendedor</Label>
          <Input
            id="georef-vendedor"
            placeholder="Código (vacío = todos)"
            value={vendedor}
            onChange={(e) => setVendedor(e.target.value)}
            className="max-w-xs"
          />
        </div>
        <div className="flex items-center gap-2 pb-2">
          <Checkbox
            id="solo-pendientes"
            checked={soloPendientes}
            onCheckedChange={(c) => setSoloPendientes(c === true)}
          />
          <Label htmlFor="solo-pendientes" className="cursor-pointer font-normal">
            Mostrar solo pendientes georreferencia
          </Label>
        </div>
      </div>

      {error ? <p className="text-sm text-destructive">{error}</p> : null}
      {actionError ? <p className="text-sm text-destructive">{actionError}</p> : null}

      <Card>
        <CardHeader>
          <CardTitle>
            {soloPendientes ? "Pendientes" : "Seguimiento georef"} ({data?.total ?? 0})
          </CardTitle>
        </CardHeader>
        <CardContent>
          {loading && !data ? (
            <div className="flex justify-center py-12">
              <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
          ) : (data?.items ?? []).length === 0 ? (
            <p className="py-8 text-center text-sm text-muted-foreground">
              No hay clientes con el filtro actual.
            </p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Vendedor</TableHead>
                  <TableHead>Cliente</TableHead>
                  <TableHead>Dirección</TableHead>
                  <TableHead>Comuna</TableHead>
                  <TableHead>Estado</TableHead>
                  <TableHead className="text-right">Acciones</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {(data?.items ?? []).map((row) => {
                  const pendiente = isPendienteOperacional(row)
                  const estado = String(row.georef_estado).toLowerCase()
                  return (
                    <TableRow
                      key={row.ruta_id}
                      className={pendiente ? "bg-amber-50/80 dark:bg-amber-950/20" : undefined}
                    >
                      <TableCell className="font-mono text-xs whitespace-nowrap">
                        {row.vendedor_codigo || "—"}
                      </TableCell>
                      <TableCell>
                        <div className="font-medium">{row.cliente_nombre}</div>
                        <div className="text-xs text-muted-foreground">
                          {row.cliente_codigo} · #{row.ruta_id}
                        </div>
                      </TableCell>
                      <TableCell className="max-w-[200px] truncate text-sm">
                        {row.direccion || "—"}
                      </TableCell>
                      <TableCell className="text-sm">{row.comuna || "—"}</TableCell>
                      <TableCell>
                        <GeorefEstadoBadge estado={row.georef_estado} />
                      </TableCell>
                      <TableCell className="text-right space-x-1">
                        {estado !== "aplicada" ? (
                          <Button
                            size="sm"
                            variant="outline"
                            disabled={busyId === row.ruta_id}
                            onClick={() => onEstado(row, "aplicada")}
                          >
                            Marcar aplicada
                          </Button>
                        ) : null}
                        {estado !== "pendiente" ? (
                          <Button
                            size="sm"
                            variant="ghost"
                            disabled={busyId === row.ruta_id}
                            onClick={() => onEstado(row, "pendiente")}
                          >
                            Volver pendiente
                          </Button>
                        ) : null}
                      </TableCell>
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
