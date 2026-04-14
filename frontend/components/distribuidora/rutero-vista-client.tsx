"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { AlertTriangle, Copy, ExternalLink, Loader2 } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Textarea } from "@/components/ui/textarea"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import {
  getDistribuidoraMapa,
  getDistribuidoraRutero,
  postDistribuidoraObservacionRutero,
  type DistribuidoraRuteroFila,
} from "@/lib/api"
import { cn } from "@/lib/utils"

const SELECT_CLASS =
  "h-9 min-w-[140px] rounded-md border border-input bg-background px-3 text-sm shadow-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"

function uniqueSorted(vals: (string | null | undefined)[]): string[] {
  return [...new Set(vals.map((v) => (v ?? "").trim()).filter(Boolean))].sort((a, b) =>
    a.localeCompare(b, "es"),
  )
}

function esTipoTelefonico(row: DistribuidoraRuteroFila): boolean {
  const t = String(row.tipo_atencion ?? "terreno").toLowerCase()
  return t.includes("telefon")
}

function tieneGeorefMapa(row: DistribuidoraRuteroFila): boolean {
  if (esTipoTelefonico(row)) return true
  const lat = Number(row.lat)
  const lon = Number(row.lon)
  return Number.isFinite(lat) && Number.isFinite(lon) && lat !== 0 && lon !== 0
}

function alertasOperativas(row: DistribuidoraRuteroFila): string[] {
  const a: string[] = []
  if (!String(row.rut ?? "").trim()) a.push("Sin RUT")
  if (!String(row.direccion ?? "").trim()) a.push("Sin dirección")
  if (!esTipoTelefonico(row) && !tieneGeorefMapa(row)) a.push("Sin georef")
  return a
}

function urlClienteBsale(bsaleId: number | null | undefined): string | null {
  if (bsaleId == null || !Number.isFinite(Number(bsaleId))) return null
  const tpl = process.env.NEXT_PUBLIC_BSALE_CLIENT_URL_TEMPLATE?.trim()
  if (!tpl) return null
  const id = String(bsaleId)
  return tpl.replaceAll("{bsale_id}", id).replaceAll("{id}", id)
}

function ordenCelda(row: DistribuidoraRuteroFila): { texto: string; title?: string } {
  const om = row.orden_manual
  if (om != null && Number.isFinite(Number(om))) {
    return { texto: String(om) }
  }
  const orr = row.orden_ruta
  if (orr != null && Number.isFinite(Number(orr))) {
    return { texto: "—", title: `Sin orden manual (orden_ruta ${orr})` }
  }
  return { texto: "—", title: "Sin orden manual" }
}

export default function RuteroVistaClient() {
  const [vendedor, setVendedor] = useState("")
  const [dia, setDia] = useState("")
  const [vendedorOptions, setVendedorOptions] = useState<string[]>([])
  const [diaOptions, setDiaOptions] = useState<string[]>([])
  const [mapLoading, setMapLoading] = useState(true)
  const [mapError, setMapError] = useState("")

  const [filas, setFilas] = useState<DistribuidoraRuteroFila[]>([])
  const [tablaLoading, setTablaLoading] = useState(false)
  const [tablaError, setTablaError] = useState("")
  const [savingId, setSavingId] = useState<number | null>(null)
  const [copiadoKey, setCopiadoKey] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    setMapLoading(true)
    setMapError("")
    getDistribuidoraMapa()
      .then((data) => {
        if (cancelled) return
        const clientes = Array.isArray(data.clientes) ? data.clientes : []
        setVendedorOptions(uniqueSorted(clientes.map((c) => c.vendedor)))
        setDiaOptions(uniqueSorted(clientes.map((c) => c.dia_atencion)))
      })
      .catch((e: unknown) => {
        if (!cancelled) setMapError(e instanceof Error ? e.message : "Error al cargar opciones")
      })
      .finally(() => {
        if (!cancelled) setMapLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [])

  const puedeCargar = vendedor.trim() !== "" && dia.trim() !== ""

  const cargarTabla = useCallback(async () => {
    if (!puedeCargar) {
      setFilas([])
      return
    }
    setTablaLoading(true)
    setTablaError("")
    try {
      const data = await getDistribuidoraRutero(vendedor, dia)
      setFilas(data)
    } catch (e: unknown) {
      setFilas([])
      setTablaError(e instanceof Error ? e.message : "Error al cargar el rutero")
    } finally {
      setTablaLoading(false)
    }
  }, [puedeCargar, vendedor, dia])

  useEffect(() => {
    void cargarTabla()
  }, [cargarTabla])

  const onObsBlur = useCallback(
    async (row: DistribuidoraRuteroFila, value: string) => {
      const id = Number(row.id)
      if (!Number.isFinite(id)) return
      const next = value.trim()
      const prev = String(row.observaciones ?? "").trim()
      if (next === prev) return
      setSavingId(id)
      setTablaError("")
      try {
        const updated = await postDistribuidoraObservacionRutero({
          cliente_id: id,
          observaciones: next === "" ? null : next,
        })
        setFilas((rows) => rows.map((r) => (Number(r.id) === id ? { ...r, ...updated } : r)))
      } catch (e: unknown) {
        setTablaError(e instanceof Error ? e.message : "Error al guardar observaciones")
      } finally {
        setSavingId(null)
      }
    },
    [],
  )

  const tituloOrden = useMemo(
    () =>
      "Con orden_manual definido se muestra el número; si no, se listan igual al final (NULLS LAST) con guía orden_ruta en tooltip.",
    [],
  )

  const copiarRut = useCallback(async (row: DistribuidoraRuteroFila) => {
    const rut = String(row.rut ?? "").trim()
    if (!rut) return
    const key = `rut-${row.id}`
    try {
      await navigator.clipboard.writeText(rut)
      setCopiadoKey(key)
      window.setTimeout(() => setCopiadoKey((k) => (k === key ? null : k)), 2000)
    } catch {
      setTablaError("No se pudo copiar el RUT al portapapeles.")
    }
  }, [])

  return (
    <div className="space-y-4 p-4 md:p-6">
      <div>
        <h1 className="text-xl font-semibold tracking-tight text-foreground">Rutero</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Listado por vendedor y día. Las observaciones se guardan al salir del cuadro de texto. RUT e ID Bsale
          permiten cruzar con Bsale; enlace opcional vía{" "}
          <code className="rounded bg-muted px-1 text-xs">NEXT_PUBLIC_BSALE_CLIENT_URL_TEMPLATE</code>{" "}
          (placeholders <code className="text-xs">{"{bsale_id}"}</code> o <code className="text-xs">{"{id}"}</code>
          ).
        </p>
      </div>

      {mapError ? (
        <p className="text-sm text-destructive" role="alert">
          {mapError}
        </p>
      ) : null}

      <div className="flex flex-wrap items-end gap-3">
        <div className="flex flex-col gap-1">
          <label htmlFor="rutero-vendedor" className="text-sm font-medium text-foreground">
            Vendedor
          </label>
          <select
            id="rutero-vendedor"
            className={SELECT_CLASS}
            value={vendedor}
            onChange={(e) => setVendedor(e.target.value)}
            disabled={mapLoading}
            aria-label="Vendedor"
          >
            <option value="">—</option>
            {vendedorOptions.map((v) => (
              <option key={v} value={v}>
                {v}
              </option>
            ))}
          </select>
        </div>
        <div className="flex flex-col gap-1">
          <label htmlFor="rutero-dia" className="text-sm font-medium text-foreground">
            Día
          </label>
          <select
            id="rutero-dia"
            className={SELECT_CLASS}
            value={dia}
            onChange={(e) => setDia(e.target.value)}
            disabled={mapLoading}
            aria-label="Día de atención"
          >
            <option value="">—</option>
            {diaOptions.map((d) => (
              <option key={d} value={d}>
                {d}
              </option>
            ))}
          </select>
        </div>
        <Button
          type="button"
          variant="outline"
          size="sm"
          disabled={!puedeCargar || tablaLoading}
          onClick={() => void cargarTabla()}
        >
          Actualizar
        </Button>
      </div>

      {tablaError ? (
        <p className="text-sm text-destructive" role="alert">
          {tablaError}
        </p>
      ) : null}

      {!puedeCargar ? (
        <p className="text-sm text-muted-foreground">Elige vendedor y día para ver el listado.</p>
      ) : tablaLoading ? (
        <div className="flex items-center gap-2 text-muted-foreground">
          <Loader2 className="h-5 w-5 animate-spin" aria-hidden />
          Cargando rutero…
        </div>
      ) : filas.length === 0 ? (
        <p className="text-sm text-muted-foreground">No hay clientes activos para ese vendedor y día.</p>
      ) : (
        <div className="max-h-[min(75vh,800px)] overflow-auto rounded-md border border-border">
          <table className="w-full min-w-[1100px] border-collapse text-left text-sm">
            <thead className="sticky top-0 z-10 border-b border-border bg-muted/90 backdrop-blur">
              <tr>
                <th className="whitespace-nowrap px-3 py-2 font-medium" title={tituloOrden}>
                  Orden
                </th>
                <th className="whitespace-nowrap px-3 py-2 font-medium">RUT</th>
                <th className="min-w-[200px] px-3 py-2 font-medium">Cliente</th>
                <th className="min-w-[220px] px-3 py-2 font-medium">Dirección</th>
                <th className="whitespace-nowrap px-3 py-2 font-medium">Comuna</th>
                <th className="whitespace-nowrap px-3 py-2 font-medium">Teléfono</th>
                <th className="whitespace-nowrap px-3 py-2 font-medium">Tipo</th>
                <th className="whitespace-nowrap px-3 py-2 font-medium">Georef</th>
                <th className="min-w-[200px] px-3 py-2 font-medium">Observaciones</th>
                <th className="whitespace-nowrap px-3 py-2 font-medium">Acciones</th>
              </tr>
            </thead>
            <tbody>
              {filas.map((row) => {
                const ord = ordenCelda(row)
                const alertas = alertasOperativas(row)
                const rutStr = String(row.rut ?? "").trim()
                const dirStr = String(row.direccion ?? "").trim()
                const razon = String(row.razon_social ?? "").trim()
                const bsaleId = row.bsale_id
                const bsaleUrl = urlClienteBsale(bsaleId)
                const nombreMostrar = row.cliente_nombre ?? "—"
                return (
                  <tr
                    key={row.id}
                    className={cn(
                      "border-b border-border/70 last:border-0",
                      alertas.length > 0 &&
                        "border-l-4 border-l-amber-500 bg-amber-50/70 dark:bg-amber-950/25",
                    )}
                  >
                    <td className="whitespace-nowrap px-3 py-2 tabular-nums" title={ord.title}>
                      {ord.texto}
                    </td>
                    <td className="whitespace-nowrap px-3 py-2 font-mono text-xs tabular-nums">
                      {rutStr || <span className="text-destructive">—</span>}
                    </td>
                    <td className="max-w-[260px] px-3 py-2">
                      <div className="flex items-start gap-1.5">
                        {alertas.length > 0 ? (
                          <span className="mt-0.5 shrink-0 text-amber-600" title={alertas.join(" · ")}>
                            <AlertTriangle className="h-4 w-4" aria-hidden />
                          </span>
                        ) : null}
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <span className="cursor-help font-medium text-foreground underline decoration-dotted decoration-muted-foreground/60 underline-offset-2">
                              {nombreMostrar}
                            </span>
                          </TooltipTrigger>
                          <TooltipContent side="right" className="max-w-xs text-xs">
                            <div className="space-y-1.5">
                              <p>
                                <span className="text-muted-foreground">RUT:</span> {rutStr || "—"}
                              </p>
                              <p>
                                <span className="text-muted-foreground">ID Bsale:</span>{" "}
                                {bsaleId != null ? String(bsaleId) : "—"}
                              </p>
                              <p>
                                <span className="text-muted-foreground">Dirección:</span> {dirStr || "—"}
                              </p>
                              {razon ? (
                                <p>
                                  <span className="text-muted-foreground">Nombre / razón:</span> {razon}
                                </p>
                              ) : null}
                              {row.nombre_fantasia?.trim() ? (
                                <p>
                                  <span className="text-muted-foreground">Nombre fantasía:</span>{" "}
                                  {row.nombre_fantasia.trim()}
                                </p>
                              ) : null}
                            </div>
                          </TooltipContent>
                        </Tooltip>
                      </div>
                    </td>
                    <td className="max-w-[280px] px-3 py-2 text-muted-foreground">{dirStr || "—"}</td>
                    <td className="whitespace-nowrap px-3 py-2 text-muted-foreground">
                      {row.municipality?.trim() || "—"}
                    </td>
                    <td className="whitespace-nowrap px-3 py-2">{row.telefono?.trim() || "—"}</td>
                    <td className="whitespace-nowrap px-3 py-2">
                      {esTipoTelefonico(row) ? (
                        <Badge variant="outline">Teléfono</Badge>
                      ) : (
                        <Badge variant="secondary">Terreno</Badge>
                      )}
                    </td>
                    <td className="whitespace-nowrap px-3 py-2">
                      {esTipoTelefonico(row) ? (
                        <Badge variant="outline" className="text-muted-foreground">
                          N/A
                        </Badge>
                      ) : tieneGeorefMapa(row) ? (
                        <Badge className="border-transparent bg-emerald-600 text-white hover:bg-emerald-600/90">
                          OK
                        </Badge>
                      ) : (
                        <Badge variant="destructive">Sin coordenadas</Badge>
                      )}
                    </td>
                    <td className="min-w-[200px] px-2 py-1.5 align-top">
                      <Textarea
                        key={`obs-${row.id}-${row.observaciones ?? ""}`}
                        className="min-h-[4.5rem] resize-y text-sm"
                        defaultValue={row.observaciones ?? ""}
                        disabled={savingId === row.id}
                        rows={3}
                        aria-label={`Observaciones ${row.cliente_nombre ?? row.id}`}
                        onBlur={(e) => void onObsBlur(row, e.target.value)}
                      />
                    </td>
                    <td className="whitespace-nowrap px-2 py-2">
                      <div className="flex flex-col gap-1 sm:flex-row sm:flex-wrap">
                        {bsaleUrl ? (
                          <Button variant="outline" size="sm" className="h-8 gap-1 px-2 text-xs" asChild>
                            <a href={bsaleUrl} target="_blank" rel="noopener noreferrer">
                              <ExternalLink className="h-3.5 w-3.5" aria-hidden />
                              Bsale
                            </a>
                          </Button>
                        ) : null}
                        <Button
                          type="button"
                          variant="secondary"
                          size="sm"
                          className="h-8 gap-1 px-2 text-xs"
                          disabled={!rutStr}
                          title={rutStr ? "Copiar RUT al portapapeles" : "Sin RUT para copiar"}
                          onClick={() => void copiarRut(row)}
                        >
                          <Copy className="h-3.5 w-3.5" aria-hidden />
                          {copiadoKey === `rut-${row.id}` ? "Copiado" : "Copiar RUT"}
                        </Button>
                      </div>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
