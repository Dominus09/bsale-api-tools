"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { AlertTriangle, Copy, ExternalLink, Loader2 } from "lucide-react"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Label } from "@/components/ui/label"
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group"
import { Textarea } from "@/components/ui/textarea"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import {
  getDistribuidoraMapa,
  getDistribuidoraRutero,
  patchDistribuidoraRuteroTipoAtencion,
  postDistribuidoraObservacionRutero,
  type DistribuidoraMapaCliente,
  type DistribuidoraPuntoBase,
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

/** Valor Bsale en `dia_atencion` para clientes solo telefónicos (no rutas). */
function esDiaAtencionTelefonico(value: string | null | undefined): boolean {
  return String(value ?? "").trim().toLowerCase() === "telefonico"
}

function diasCatalogoDesdeMapaResp(data: {
  clientes?: unknown
  dias_atencion?: unknown
}): string[] {
  const set = new Set<string>()
  if (Array.isArray(data.dias_atencion)) {
    for (const x of data.dias_atencion) {
      const d = String(x).trim()
      if (d) set.add(d)
    }
  }
  const arr = Array.isArray(data.clientes) ? (data.clientes as DistribuidoraMapaCliente[]) : []
  for (const c of arr) {
    const d = c.dia_atencion?.trim()
    if (d) set.add(d)
  }
  return Array.from(set).sort((a, b) => a.localeCompare(b, "es"))
}

function esTipoTelefonico(row: DistribuidoraRuteroFila): boolean {
  const t = String(row.tipo_atencion ?? "terreno").toLowerCase()
  return t.includes("telefon")
}

type TipoAtencionUi = "TERRENO" | "TELEFONICO"

function tipoAtencionValorUi(row: DistribuidoraRuteroFila): TipoAtencionUi {
  return esTipoTelefonico(row) ? "TELEFONICO" : "TERRENO"
}

/** Coordenadas usables en mapa (no null, finitas, no (0,0)). */
function tieneCoordsValidas(row: DistribuidoraRuteroFila): boolean {
  const lat = Number(row.lat)
  const lon = Number(row.lon)
  return Number.isFinite(lat) && Number.isFinite(lon) && !(lat === 0 && lon === 0)
}

/** Cliente que entraría a ruta en mapa (excl. tipo/día telefónico en Bsale). */
function requiereGeorefEnMapa(row: DistribuidoraRuteroFila): boolean {
  if (esTipoTelefonico(row)) return false
  if (esDiaAtencionTelefonico(row.dia_atencion)) return false
  return true
}

function esTelefonicoVisual(row: DistribuidoraRuteroFila): boolean {
  return esTipoTelefonico(row) || esDiaAtencionTelefonico(row.dia_atencion)
}

function sinDiaAsignado(row: DistribuidoraRuteroFila): boolean {
  return !String(row.dia_atencion ?? "").trim()
}

function alertasOperativas(row: DistribuidoraRuteroFila): string[] {
  const a: string[] = []
  if (!String(row.rut ?? "").trim()) a.push("Sin RUT")
  if (!String(row.direccion ?? "").trim()) a.push("Sin dirección")
  if (requiereGeorefEnMapa(row) && !tieneCoordsValidas(row)) a.push("Sin georef")
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

type FiltroTipoAtencion = "all" | "terreno" | "telefonico"
type FiltroGeo = "all" | "con" | "sin"
type FiltroDiaAsignado = "all" | "con" | "sin"

export default function RuteroVistaClient() {
  const [vendedor, setVendedor] = useState("")
  const [dia, setDia] = useState("")
  const [filtroTipo, setFiltroTipo] = useState<FiltroTipoAtencion>("all")
  const [filtroGeo, setFiltroGeo] = useState<FiltroGeo>("all")
  const [filtroDiaAsignado, setFiltroDiaAsignado] = useState<FiltroDiaAsignado>("all")
  const [vendedorOptions, setVendedorOptions] = useState<string[]>([])
  const [diaOptions, setDiaOptions] = useState<string[]>([])
  const [mapLoading, setMapLoading] = useState(true)
  const [mapError, setMapError] = useState("")

  const [filas, setFilas] = useState<DistribuidoraRuteroFila[]>([])
  const [tablaLoading, setTablaLoading] = useState(false)
  const [tablaError, setTablaError] = useState("")
  const [savingId, setSavingId] = useState<number | null>(null)
  const [savingTipoId, setSavingTipoId] = useState<number | null>(null)
  const [copiadoKey, setCopiadoKey] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    setMapLoading(true)
    setMapError("")
    getDistribuidoraMapa()
      .then((data) => {
        if (cancelled) return
        const clientes = Array.isArray(data.clientes) ? data.clientes : []
        const bases = Array.isArray(data.bases) ? (data.bases as DistribuidoraPuntoBase[]) : []
        const vs = new Set<string>()
        if (Array.isArray(data.vendedores)) {
          for (const x of data.vendedores) {
            const v = String(x ?? "").trim()
            if (v) vs.add(v)
          }
        }
        for (const v0 of uniqueSorted(clientes.map((c) => c.vendedor))) {
          const v = v0?.trim()
          if (v) vs.add(v)
        }
        for (const b of bases) {
          const v = b.vendedor?.trim()
          if (v) vs.add(v)
        }
        setVendedorOptions(Array.from(vs).sort((a, b) => a.localeCompare(b, "es")))
        setDiaOptions(diasCatalogoDesdeMapaResp(data))
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

  const cargarTabla = useCallback(async () => {
    if (mapLoading) return
    setTablaLoading(true)
    setTablaError("")
    try {
      const data = await getDistribuidoraRutero({
        vendedor: vendedor.trim() || undefined,
        dia: filtroDiaAsignado === "sin" ? undefined : dia.trim() || undefined,
        tipo: filtroTipo === "all" ? undefined : filtroTipo,
        geo: filtroGeo === "all" ? undefined : filtroGeo,
        dia_estado: filtroDiaAsignado === "all" ? undefined : filtroDiaAsignado,
      })
      setFilas(data)
    } catch (e: unknown) {
      setFilas([])
      setTablaError(e instanceof Error ? e.message : "Error al cargar el rutero")
    } finally {
      setTablaLoading(false)
    }
  }, [mapLoading, vendedor, dia, filtroTipo, filtroGeo, filtroDiaAsignado])

  useEffect(() => {
    void cargarTabla()
  }, [cargarTabla])

  const contadores = useMemo(() => {
    let telefonicos = 0
    let sinGeoref = 0
    let sinDia = 0
    for (const row of filas) {
      if (esTelefonicoVisual(row)) telefonicos += 1
      if (sinDiaAsignado(row)) sinDia += 1
      if (requiereGeorefEnMapa(row) && !tieneCoordsValidas(row)) sinGeoref += 1
    }
    return { total: filas.length, telefonicos, sinGeoref, sinDia }
  }, [filas])

  const onTipoAtencionChange = useCallback(
    async (row: DistribuidoraRuteroFila, next: TipoAtencionUi) => {
      const id = Number(row.id)
      if (!Number.isFinite(id)) return
      if (tipoAtencionValorUi(row) === next) return
      setSavingTipoId(id)
      setTablaError("")
      try {
        const updated = await patchDistribuidoraRuteroTipoAtencion(id, { tipo_atencion: next })
        setFilas((rows) => rows.map((r) => (Number(r.id) === id ? { ...r, ...updated } : r)))
      } catch (e: unknown) {
        setTablaError(e instanceof Error ? e.message : "Error al guardar tipo de atención")
      } finally {
        setSavingTipoId(null)
      }
    },
    [],
  )

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
          Listado completo de <code className="rounded bg-muted px-1 text-xs">bsale.rutero</code> (activos): incluye
          telefónicos, sin georef y sin día. Usa los filtros para acotar. El mapa de rutas sigue mostrando solo
          terreno con coordenadas y día asignado. Tipo (terreno / telefónico) se guarda en el desplegable; las
          observaciones al salir del cuadro de texto. RUT e ID Bsale para cruzar con Bsale; enlace opcional vía{" "}
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

      <div className="space-y-4 rounded-lg border border-border bg-card p-4 shadow-sm">
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
              <option value="">Todos</option>
              {vendedorOptions.map((v) => (
                <option key={v} value={v}>
                  {v}
                </option>
              ))}
            </select>
          </div>
          <div className="flex flex-col gap-1">
            <label htmlFor="rutero-dia" className="text-sm font-medium text-foreground">
              Día de atención
            </label>
            <select
              id="rutero-dia"
              className={SELECT_CLASS}
              value={dia}
              onChange={(e) => setDia(e.target.value)}
              disabled={mapLoading || filtroDiaAsignado === "sin"}
              aria-label="Día de atención (valor en rutero)"
              title={
                filtroDiaAsignado === "sin"
                  ? "Con filtro «Sin día» no aplica un día concreto."
                  : "Filtra por valor de dia_atencion (ej. lunes). «Todos» no acota por día."
              }
            >
              <option value="">Todos</option>
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
            disabled={mapLoading || tablaLoading}
            onClick={() => void cargarTabla()}
          >
            Actualizar
          </Button>
        </div>

        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
          <div className="space-y-2 rounded-md border border-border/80 bg-muted/20 p-3">
            <p className="text-xs font-medium text-muted-foreground">Tipo atención</p>
            <RadioGroup
              value={filtroTipo}
              onValueChange={(v) => setFiltroTipo(v as FiltroTipoAtencion)}
              className="flex flex-col gap-2"
            >
              <div className="flex items-center gap-2">
                <RadioGroupItem value="all" id="rut-ft-all" />
                <Label htmlFor="rut-ft-all" className="cursor-pointer text-sm font-normal">
                  Todos
                </Label>
              </div>
              <div className="flex items-center gap-2">
                <RadioGroupItem value="terreno" id="rut-ft-terreno" />
                <Label htmlFor="rut-ft-terreno" className="cursor-pointer text-sm font-normal">
                  Terreno
                </Label>
              </div>
              <div className="flex items-center gap-2">
                <RadioGroupItem value="telefonico" id="rut-ft-tel" />
                <Label htmlFor="rut-ft-tel" className="cursor-pointer text-sm font-normal">
                  Telefónico
                </Label>
              </div>
            </RadioGroup>
          </div>
          <div className="space-y-2 rounded-md border border-border/80 bg-muted/20 p-3">
            <p className="text-xs font-medium text-muted-foreground">Georreferencia</p>
            <RadioGroup
              value={filtroGeo}
              onValueChange={(v) => setFiltroGeo(v as FiltroGeo)}
              className="flex flex-col gap-2"
            >
              <div className="flex items-center gap-2">
                <RadioGroupItem value="all" id="rut-fg-all" />
                <Label htmlFor="rut-fg-all" className="cursor-pointer text-sm font-normal">
                  Todos
                </Label>
              </div>
              <div className="flex items-center gap-2">
                <RadioGroupItem value="con" id="rut-fg-con" />
                <Label htmlFor="rut-fg-con" className="cursor-pointer text-sm font-normal">
                  Con georef
                </Label>
              </div>
              <div className="flex items-center gap-2">
                <RadioGroupItem value="sin" id="rut-fg-sin" />
                <Label htmlFor="rut-fg-sin" className="cursor-pointer text-sm font-normal">
                  Sin georef
                </Label>
              </div>
            </RadioGroup>
          </div>
          <div className="space-y-2 rounded-md border border-border/80 bg-muted/20 p-3">
            <p className="text-xs font-medium text-muted-foreground">Día asignado</p>
            <RadioGroup
              value={filtroDiaAsignado}
              onValueChange={(v) => setFiltroDiaAsignado(v as FiltroDiaAsignado)}
              className="flex flex-col gap-2"
            >
              <div className="flex items-center gap-2">
                <RadioGroupItem value="all" id="rut-fd-all" />
                <Label htmlFor="rut-fd-all" className="cursor-pointer text-sm font-normal">
                  Todos
                </Label>
              </div>
              <div className="flex items-center gap-2">
                <RadioGroupItem value="con" id="rut-fd-con" />
                <Label htmlFor="rut-fd-con" className="cursor-pointer text-sm font-normal">
                  Con día
                </Label>
              </div>
              <div className="flex items-center gap-2">
                <RadioGroupItem value="sin" id="rut-fd-sin" />
                <Label htmlFor="rut-fd-sin" className="cursor-pointer text-sm font-normal">
                  Sin día
                </Label>
              </div>
            </RadioGroup>
          </div>
        </div>

        {!mapLoading && !tablaLoading ? (
          <div className="flex flex-wrap gap-2 text-sm" aria-live="polite">
            <Badge variant="secondary" className="font-normal">
              Total {contadores.total}
            </Badge>
            <Badge variant="outline" className="border-slate-300 bg-slate-100 font-normal text-slate-800">
              Telefónicos {contadores.telefonicos}
            </Badge>
            <Badge variant="outline" className="border-red-200 bg-red-50 font-normal text-red-900">
              Sin georef {contadores.sinGeoref}
            </Badge>
            <Badge variant="outline" className="border-amber-300 bg-amber-50 font-normal text-amber-950">
              Sin día {contadores.sinDia}
            </Badge>
          </div>
        ) : null}
      </div>

      {tablaError ? (
        <p className="text-sm text-destructive" role="alert">
          {tablaError}
        </p>
      ) : null}

      {mapLoading ? (
        <div className="flex items-center gap-2 text-muted-foreground">
          <Loader2 className="h-5 w-5 animate-spin" aria-hidden />
          Cargando opciones de vendedor y día…
        </div>
      ) : tablaLoading ? (
        <div className="flex items-center gap-2 text-muted-foreground">
          <Loader2 className="h-5 w-5 animate-spin" aria-hidden />
          Cargando rutero…
        </div>
      ) : filas.length === 0 ? (
        <p className="text-sm text-muted-foreground">No hay filas que coincidan con los filtros seleccionados.</p>
      ) : (
        <div className="max-h-[min(75vh,800px)] overflow-auto rounded-md border border-border">
          <table className="w-full min-w-[1240px] border-collapse text-left text-sm">
            <thead className="sticky top-0 z-10 border-b border-border bg-muted/90 backdrop-blur">
              <tr>
                <th className="whitespace-nowrap px-2 py-2 font-medium" title="Resumen visual">
                  Indic.
                </th>
                <th className="whitespace-nowrap px-3 py-2 font-medium" title={tituloOrden}>
                  Orden
                </th>
                <th className="whitespace-nowrap px-3 py-2 font-medium">RUT</th>
                <th className="min-w-[200px] px-3 py-2 font-medium">Cliente</th>
                <th className="min-w-[220px] px-3 py-2 font-medium">Dirección</th>
                <th className="whitespace-nowrap px-3 py-2 font-medium">Comuna</th>
                <th className="whitespace-nowrap px-3 py-2 font-medium">Día</th>
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
                const tipoUi = tipoAtencionValorUi(row)
                const telVis = esTelefonicoVisual(row)
                const sinDia = sinDiaAsignado(row)
                const sinGeoTerreno = requiereGeorefEnMapa(row) && !tieneCoordsValidas(row)
                const tituloIndic = [
                  telVis ? "Telefónico (tipo o día)" : null,
                  sinDia ? "Sin día de atención asignado" : null,
                  sinGeoTerreno ? "Sin georreferencia (terreno en mapa)" : null,
                ]
                  .filter(Boolean)
                  .join(" · ")
                return (
                  <tr
                    key={row.id}
                    className={cn(
                      "border-b border-border/70 last:border-0",
                      telVis && "text-slate-800 dark:text-slate-100",
                      telVis && "bg-slate-100/80 dark:bg-slate-900/45",
                      !telVis && sinDia && "bg-amber-50/70 dark:bg-amber-950/25",
                      !telVis && !sinDia && sinGeoTerreno && "bg-red-50/60 dark:bg-red-950/20",
                      alertas.length > 0 && "border-l-4 border-l-amber-500",
                    )}
                  >
                    <td
                      className="whitespace-nowrap px-2 py-2 text-center text-base leading-none"
                      title={tituloIndic || undefined}
                    >
                      <div className="flex flex-col items-center gap-0.5">
                        {telVis ? (
                          <span className="text-slate-600 dark:text-slate-300" aria-hidden>
                            📞
                          </span>
                        ) : null}
                        {sinDia ? (
                          <span className="text-amber-700 dark:text-amber-400" aria-hidden>
                            📅
                          </span>
                        ) : null}
                        {sinGeoTerreno ? (
                          <span className="text-red-600 dark:text-red-400" aria-hidden>
                            ⚠️
                          </span>
                        ) : null}
                        {!telVis && !sinDia && !sinGeoTerreno ? (
                          <span className="text-xs text-muted-foreground">—</span>
                        ) : null}
                      </div>
                    </td>
                    <td className="whitespace-nowrap px-3 py-2 tabular-nums" title={ord.title}>
                      <span>{ord.texto}</span>
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
                    <td className="max-w-[140px] truncate px-3 py-2 text-muted-foreground" title={row.dia_atencion ?? ""}>
                      {row.dia_atencion?.trim() || "—"}
                    </td>
                    <td className="whitespace-nowrap px-3 py-2">{row.telefono?.trim() || "—"}</td>
                    <td className="whitespace-nowrap px-2 py-2">
                      <div
                        className={cn(
                          "inline-flex max-w-full items-center gap-1.5 rounded-md border px-2 py-1",
                          tipoUi === "TERRENO"
                            ? "border-emerald-200 bg-emerald-50 text-emerald-950 dark:border-emerald-800/70 dark:bg-emerald-950/40 dark:text-emerald-50"
                            : "border-slate-200 bg-slate-100 text-slate-700 dark:border-slate-600 dark:bg-slate-800/80 dark:text-slate-200",
                        )}
                      >
                        <span className="select-none text-base leading-none" aria-hidden title="Tipo de atención">
                          {tipoUi === "TERRENO" ? "📍" : "📞"}
                        </span>
                        <select
                          className={cn(
                            "h-8 min-w-[9.5rem] max-w-[11rem] cursor-pointer rounded border-0 bg-transparent py-0 pl-0.5 pr-6 text-sm font-medium shadow-none focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-0",
                            savingTipoId === row.id && "pointer-events-none opacity-60",
                          )}
                          aria-label={`Estado atención ${nombreMostrar}`}
                          value={tipoUi}
                          disabled={savingTipoId === row.id}
                          onChange={(e) => {
                            const v = e.target.value as TipoAtencionUi
                            if (v === "TERRENO" || v === "TELEFONICO") void onTipoAtencionChange(row, v)
                          }}
                        >
                          <option value="TERRENO">Terreno</option>
                          <option value="TELEFONICO">Telefónico</option>
                        </select>
                      </div>
                    </td>
                    <td className="whitespace-nowrap px-3 py-2">
                      {esTipoTelefonico(row) ? (
                        <Badge variant="outline" className="text-muted-foreground">
                          N/A
                        </Badge>
                      ) : esDiaAtencionTelefonico(row.dia_atencion) ? (
                        <Badge variant="outline" className="text-muted-foreground">
                          Día tel.
                        </Badge>
                      ) : tieneCoordsValidas(row) ? (
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
