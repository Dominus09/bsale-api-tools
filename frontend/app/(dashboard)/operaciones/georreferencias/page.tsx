"use client"

import { useCallback, useEffect, useState } from "react"
import { ClipboardCopy, Download, ExternalLink, Loader2, RefreshCw } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  coordsGeorefText,
  downloadOperacionesGeorefExport,
  getOperacionesGeorefPendientes,
  googleMapsUrl,
  patchOperacionesGeorefEstado,
  tieneGeorefEfectiva,
  type ClienteGeorefRow,
  type GeorefEstado,
  type GeorefPendientesResponse,
} from "@/services/operaciones"
import { cn } from "@/lib/utils"

function GeorefEstadoBadge({ estado }: { estado: string }) {
  const e = estado.toLowerCase()
  const cls =
    e === "rechazada"
      ? "bg-red-100 text-red-800 dark:bg-red-950 dark:text-red-200"
      : e === "aplicada"
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

function isPendienteGeoref(row: ClienteGeorefRow) {
  return !tieneGeorefEfectiva(row)
}

function KpiCard({ label, value, accent }: { label: string; value: number; accent?: string }) {
  return (
    <div className="rounded-lg border bg-card px-4 py-3">
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className={cn("text-2xl font-semibold tabular-nums", accent)}>{value}</p>
    </div>
  )
}

function formatCoord(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "—"
  return n.toFixed(6)
}

export default function OperacionesGeorreferenciasPage() {
  const [vendedor, setVendedor] = useState("")
  const [comuna, setComuna] = useState("")
  const [filtroEstado, setFiltroEstado] = useState<string>("")
  const [soloPendientes, setSoloPendientes] = useState(true)
  const [data, setData] = useState<GeorefPendientesResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [busyId, setBusyId] = useState<number | null>(null)
  const [exporting, setExporting] = useState(false)
  const [actionError, setActionError] = useState<string | null>(null)
  const [rechazoId, setRechazoId] = useState<number | null>(null)
  const [motivoRechazo, setMotivoRechazo] = useState("")

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await getOperacionesGeorefPendientes({
        vendedor: vendedor || undefined,
        vista: "erp",
        solo_pendientes: filtroEstado ? false : soloPendientes,
        estado: (filtroEstado as GeorefEstado) || undefined,
        comuna: comuna || undefined,
      })
      setData(res)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al cargar georreferencias")
    } finally {
      setLoading(false)
    }
  }, [vendedor, soloPendientes, filtroEstado, comuna])

  useEffect(() => {
    void load()
  }, [load])

  async function onEstado(row: ClienteGeorefRow, estado: GeorefEstado | "capturada") {
    setActionError(null)
    if (estado === "aplicada" && !tieneGeorefEfectiva(row)) {
      setActionError("No se puede marcar aplicada sin georreferencia.")
      return
    }
    if (estado === "rechazada") {
      setRechazoId(row.ruta_id)
      return
    }
    setBusyId(row.ruta_id)
    try {
      await patchOperacionesGeorefEstado(row.ruta_id, estado)
      if (estado === "pendiente") setSoloPendientes(false)
      await load()
    } catch (e) {
      setActionError(e instanceof Error ? e.message : "Error al actualizar estado")
    } finally {
      setBusyId(null)
    }
  }

  async function confirmarRechazo(row: ClienteGeorefRow) {
    if (!motivoRechazo.trim()) {
      setActionError("Indique el motivo de rechazo.")
      return
    }
    setBusyId(row.ruta_id)
    try {
      await patchOperacionesGeorefEstado(row.ruta_id, "rechazada", {
        motivo_rechazo: motivoRechazo.trim(),
      })
      setRechazoId(null)
      setMotivoRechazo("")
      setFiltroEstado("rechazada")
      await load()
    } catch (e) {
      setActionError(e instanceof Error ? e.message : "Error al rechazar")
    } finally {
      setBusyId(null)
    }
  }

  async function copiarCoords(row: ClienteGeorefRow) {
    const t = coordsGeorefText(row)
    if (!t) return
    try {
      await navigator.clipboard.writeText(t)
    } catch {
      setActionError("No se pudo copiar al portapapeles")
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
            Coordenadas efectivas = COALESCE(lat_operacional, lat). Volver a pendiente conserva
            coordenadas; use filtro de estado o desactive “solo pendientes”.
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
            Exportar CSV
          </Button>
        </div>
      </div>

      {resumen ? (
        <div className="grid gap-3 grid-cols-2 sm:grid-cols-5">
          <KpiCard label="Total clientes" value={resumen.total} />
          <KpiCard label="Pendientes georef" value={resumen.pendientes} accent="text-amber-700" />
          <KpiCard label="Capturados" value={resumen.capturados} accent="text-sky-700" />
          <KpiCard label="Rechazados" value={resumen.rechazados ?? 0} accent="text-red-700" />
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
        <div className="space-y-2">
          <Label htmlFor="georef-comuna">Comuna</Label>
          <Input
            id="georef-comuna"
            placeholder="Filtrar comuna"
            value={comuna}
            onChange={(e) => setComuna(e.target.value)}
            className="max-w-xs"
          />
        </div>
        <div className="space-y-2">
          <Label>Estado</Label>
          <Select
            value={filtroEstado || "todos"}
            onValueChange={(v) => setFiltroEstado(v === "todos" ? "" : v)}
          >
            <SelectTrigger className="w-[180px]">
              <SelectValue placeholder="Todos" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="todos">Todos (seguimiento)</SelectItem>
              <SelectItem value="pendiente">Solo pendientes</SelectItem>
              <SelectItem value="capturada">Solo capturadas</SelectItem>
              <SelectItem value="rechazada">Solo rechazadas</SelectItem>
              <SelectItem value="aplicada">Solo aplicadas</SelectItem>
            </SelectContent>
          </Select>
        </div>
        {!filtroEstado ? (
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
        ) : null}
      </div>

      {error ? <p className="text-sm text-destructive">{error}</p> : null}
      {actionError ? <p className="text-sm text-destructive">{actionError}</p> : null}

      <Card>
        <CardHeader>
          <CardTitle>Listado ({data?.total ?? 0})</CardTitle>
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
                  <TableHead>Latitud</TableHead>
                  <TableHead>Longitud</TableHead>
                  <TableHead>Estado</TableHead>
                  <TableHead className="text-right">Acciones</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {(data?.items ?? []).map((row) => {
                  const pendiente = isPendienteGeoref(row)
                  const estado = String(row.georef_estado).toLowerCase()
                  const puedeAplicada = tieneGeorefEfectiva(row)
                  const coords = coordsGeorefText(row)
                  return (
                    <TableRow
                      key={row.ruta_id}
                      className={cn(
                        pendiente && "bg-amber-50/80 dark:bg-amber-950/20",
                        estado === "rechazada" && "bg-red-50/60 dark:bg-red-950/20",
                      )}
                    >
                      <TableCell className="font-mono text-xs whitespace-nowrap">
                        {row.vendedor_codigo || "—"}
                      </TableCell>
                      <TableCell>
                        <div className="font-medium">{row.cliente_nombre}</div>
                        <div className="text-xs text-muted-foreground">
                          {row.cliente_codigo} · #{row.ruta_id}
                        </div>
                        {row.motivo_rechazo ? (
                          <p className="text-xs text-red-700 mt-1">Rechazo: {row.motivo_rechazo}</p>
                        ) : null}
                      </TableCell>
                      <TableCell className="max-w-[160px] truncate text-sm">
                        {row.direccion || "—"}
                        {row.comuna ? (
                          <span className="block text-xs text-muted-foreground">{row.comuna}</span>
                        ) : null}
                      </TableCell>
                      <TableCell className="font-mono text-xs">{formatCoord(row.lat)}</TableCell>
                      <TableCell className="font-mono text-xs">{formatCoord(row.lon)}</TableCell>
                      <TableCell>
                        <GeorefEstadoBadge estado={row.georef_estado} />
                      </TableCell>
                      <TableCell className="text-right">
                        <div className="flex flex-wrap justify-end gap-1">
                          {coords ? (
                            <>
                              <Button
                                size="sm"
                                variant="outline"
                                title="Copiar coordenadas"
                                onClick={() => void copiarCoords(row)}
                              >
                                <ClipboardCopy className="h-3.5 w-3.5" />
                              </Button>
                              <Button size="sm" variant="outline" asChild>
                                <a
                                  href={googleMapsUrl(row.lat!, row.lon!)}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  title="Ver en Google Maps"
                                >
                                  <ExternalLink className="h-3.5 w-3.5" />
                                </a>
                              </Button>
                            </>
                          ) : null}
                          {estado !== "aplicada" ? (
                            <Button
                              size="sm"
                              variant="outline"
                              disabled={busyId === row.ruta_id || !puedeAplicada}
                              onClick={() => onEstado(row, "aplicada")}
                            >
                              Aplicada
                            </Button>
                          ) : null}
                          {estado === "rechazada" ? (
                            <Button
                              size="sm"
                              variant="outline"
                              disabled={busyId === row.ruta_id || !puedeAplicada}
                              onClick={() => onEstado(row, "capturada")}
                            >
                              Rehabilitar
                            </Button>
                          ) : null}
                          {estado !== "rechazada" && estado !== "pendiente" && puedeAplicada ? (
                            <Button
                              size="sm"
                              variant="ghost"
                              disabled={busyId === row.ruta_id}
                              onClick={() => onEstado(row, "rechazada")}
                            >
                              Rechazar
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
                        </div>
                        {rechazoId === row.ruta_id ? (
                          <div className="mt-2 flex gap-1 justify-end">
                            <Input
                              className="h-8 max-w-[200px] text-xs"
                              placeholder="Motivo rechazo"
                              value={motivoRechazo}
                              onChange={(e) => setMotivoRechazo(e.target.value)}
                            />
                            <Button
                              size="sm"
                              variant="destructive"
                              disabled={busyId === row.ruta_id}
                              onClick={() => void confirmarRechazo(row)}
                            >
                              OK
                            </Button>
                            <Button
                              size="sm"
                              variant="ghost"
                              onClick={() => {
                                setRechazoId(null)
                                setMotivoRechazo("")
                              }}
                            >
                              ×
                            </Button>
                          </div>
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
