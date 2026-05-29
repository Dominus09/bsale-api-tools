"use client"

import { useCallback, useEffect, useState } from "react"
import { Download, Loader2, RefreshCw } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
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
  coordsGeorefText,
  downloadOperacionesGeorefExport,
  getOperacionesGeorefPendientes,
  googleMapsUrl,
  patchOperacionesGeorefEstado,
  tieneGeorefEfectiva,
  type ClienteGeorefRow,
  type GeorefEstado,
  type GeorefFiltroEstado,
  type GeorefPendientesResponse,
} from "@/services/operaciones"
import { cn } from "@/lib/utils"

const FILTROS: { id: GeorefFiltroEstado; label: string }[] = [
  { id: "todas", label: "Todas" },
  { id: "pendiente", label: "Pendientes" },
  { id: "capturada", label: "Capturadas" },
  { id: "aplicada", label: "Aplicadas" },
]

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

function isPendienteGeoref(row: ClienteGeorefRow) {
  return !tieneGeorefEfectiva(row)
}

function KpiCard({
  label,
  value,
  accent,
  active,
  onClick,
}: {
  label: string
  value: number
  accent?: string
  active?: boolean
  onClick: () => void
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        "rounded-lg border bg-card px-4 py-3 text-left transition-colors hover:bg-muted/50",
        active && "ring-2 ring-primary border-primary",
      )}
    >
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className={cn("text-2xl font-semibold tabular-nums", accent)}>{value}</p>
    </button>
  )
}

function formatCoord(n: number | null | undefined) {
  if (n == null || !Number.isFinite(n)) return "—"
  return n.toFixed(6)
}

export default function OperacionesGeorreferenciasPage() {
  const [vendedor, setVendedor] = useState("")
  const [filtroEstado, setFiltroEstado] = useState<GeorefFiltroEstado>("todas")
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
        estado: filtroEstado,
      })
      setData(res)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al cargar georreferencias")
    } finally {
      setLoading(false)
    }
  }, [vendedor, filtroEstado])

  useEffect(() => {
    void load()
  }, [load])

  async function onEstado(row: ClienteGeorefRow, estado: "pendiente" | "aplicada") {
    setActionError(null)
    if (estado === "aplicada" && !tieneGeorefEfectiva(row)) {
      setActionError("No se puede marcar aplicada sin georreferencia.")
      return
    }
    setBusyId(row.ruta_id)
    try {
      await patchOperacionesGeorefEstado(row.ruta_id, estado)
      if (estado === "pendiente") setFiltroEstado("todas")
      else if (estado === "aplicada") setFiltroEstado("aplicada")
      await load()
    } catch (e) {
      setActionError(e instanceof Error ? e.message : "Error al actualizar estado")
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
        estado: filtroEstado,
      })
    } catch (e) {
      setActionError(e instanceof Error ? e.message : "Error al exportar")
    } finally {
      setExporting(false)
    }
  }

  const resumen = data?.resumen
  const kpiFiltro = (est: GeorefEstado) => setFiltroEstado(est)

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Georreferencias</h1>
          <p className="text-sm text-muted-foreground max-w-2xl">
            Flujo operacional: pendientes, capturadas y aplicadas. Volver a pendiente conserva
            coordenadas.
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
        <div className="grid gap-3 grid-cols-1 sm:grid-cols-3">
          <KpiCard
            label="Pendientes"
            value={resumen.pendientes}
            accent="text-amber-700"
            active={filtroEstado === "pendiente"}
            onClick={() => kpiFiltro("pendiente")}
          />
          <KpiCard
            label="Capturadas"
            value={resumen.capturados}
            accent="text-sky-700"
            active={filtroEstado === "capturada"}
            onClick={() => kpiFiltro("capturada")}
          />
          <KpiCard
            label="Aplicadas"
            value={resumen.aplicados}
            accent="text-emerald-700"
            active={filtroEstado === "aplicada"}
            onClick={() => kpiFiltro("aplicada")}
          />
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
          <Label>Estado</Label>
          <div className="flex flex-wrap gap-1">
            {FILTROS.map((f) => (
              <Button
                key={f.id}
                type="button"
                size="sm"
                variant={filtroEstado === f.id ? "default" : "outline"}
                onClick={() => setFiltroEstado(f.id)}
              >
                {f.label}
              </Button>
            ))}
          </div>
        </div>
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
                  <TableHead>Cliente</TableHead>
                  <TableHead>Dirección</TableHead>
                  <TableHead>Comuna</TableHead>
                  <TableHead>Estado</TableHead>
                  <TableHead>Latitud</TableHead>
                  <TableHead>Longitud</TableHead>
                  <TableHead className="text-right">Acciones</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {(data?.items ?? []).map((row) => {
                  const pendiente = isPendienteGeoref(row)
                  const estado = String(row.georef_estado).toLowerCase()
                  const puedeAplicada = tieneGeorefEfectiva(row)
                  const coords = coordsGeorefText(row)
                  const muestraCoords =
                    coords && (estado === "capturada" || estado === "aplicada" || puedeAplicada)
                  return (
                    <TableRow
                      key={row.ruta_id}
                      className={cn(pendiente && "bg-amber-50/80 dark:bg-amber-950/20")}
                    >
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
                      <TableCell className="font-mono text-xs">
                        {muestraCoords ? formatCoord(row.lat) : "—"}
                      </TableCell>
                      <TableCell className="font-mono text-xs">
                        {muestraCoords ? formatCoord(row.lon) : "—"}
                      </TableCell>
                      <TableCell className="text-right">
                        <div className="flex flex-wrap justify-end gap-1">
                          {coords ? (
                            <>
                              <Button
                                size="sm"
                                variant="outline"
                                onClick={() => void copiarCoords(row)}
                              >
                                📋 Copiar coordenadas
                              </Button>
                              <Button size="sm" variant="outline" asChild>
                                <a
                                  href={googleMapsUrl(row.lat!, row.lon!)}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                >
                                  🗺 Google Maps
                                </a>
                              </Button>
                            </>
                          ) : null}
                          {estado !== "aplicada" ? (
                            <Button
                              size="sm"
                              variant="outline"
                              disabled={busyId === row.ruta_id || !puedeAplicada}
                              onClick={() => void onEstado(row, "aplicada")}
                            >
                              Aplicada
                            </Button>
                          ) : null}
                          {(estado === "capturada" ||
                            estado === "aplicada" ||
                            (estado === "pendiente" && puedeAplicada)) ? (
                            <Button
                              size="sm"
                              variant="ghost"
                              disabled={busyId === row.ruta_id}
                              onClick={() => void onEstado(row, "pendiente")}
                            >
                              Volver pendiente
                            </Button>
                          ) : null}
                        </div>
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
