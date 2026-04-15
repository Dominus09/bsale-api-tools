"use client"

import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type MutableRefObject,
} from "react"
import dynamic from "next/dynamic"
import L from "leaflet"
import { useMap } from "react-leaflet"
import { Loader2 } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
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
  getDistribuidoraMapa,
  getDistribuidoraResumenVendedor,
  type DistribuidoraMapaCliente,
  type DistribuidoraResumenDiaJson,
  type DistribuidoraResumenVendedorJson,
} from "@/lib/api"
import { geometryToLatLngs } from "@/lib/distribuidora-resumen-geometry"
import {
  buildConsolidatedSemanaClientRows,
  buildDiasCargaSummaryLines,
} from "@/lib/resumen-vendedor-pdf-consolidado"
import {
  safeBuildOperationalInsights,
  safeClasificarEficiencia,
} from "@/lib/resumen-vendedor-analisis"
import { fitMapToResumenBounds } from "@/lib/resumen-vendedor-map-fit"
import {
  captureMapElementJpeg,
  chunkDias,
  exportResumenVendedorPdf,
} from "@/lib/resumen-vendedor-pdf"
import {
  RESUMEN_VENDEDOR_PRINT_POPUP_BLOCKED,
  writeResumenVendedorPrintToWindow,
} from "@/lib/resumen-vendedor-print-html"
import { cn } from "@/lib/utils"

import "leaflet/dist/leaflet.css"

const MapContainer = dynamic(() => import("react-leaflet").then((m) => m.MapContainer), { ssr: false })
const TileLayer = dynamic(() => import("react-leaflet").then((m) => m.TileLayer), { ssr: false })
const Marker = dynamic(() => import("react-leaflet").then((m) => m.Marker), { ssr: false })
const Popup = dynamic(() => import("react-leaflet").then((m) => m.Popup), { ssr: false })

const CARTO_LIGHT = "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
const MAP_CENTER: [number, number] = [-33.0, -71.5]
const MAP_ZOOM = 10

/** Mínimos para simulación de combustible (evita /0 y valores absurdos). */
const MIN_RENDIMIENTO_KM_L = 0.1
const MIN_PRECIO_COMBUSTIBLE_CLP = 1

function parseNumInputLoose(raw: string): number | null {
  const t = raw.trim().replace(/\s/g, "").replace(",", ".")
  if (!t) return null
  const n = Number(t)
  return Number.isFinite(n) ? n : null
}

function formatViaticoCLP(value: number): string {
  return Math.round(value).toLocaleString("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  })
}

/** Texto seguro para tarjetas cuando el backend envía null o no numérico. */
function fmtMetric(v: unknown): string {
  const n = Number(v)
  return Number.isFinite(n) ? String(n) : "—"
}

function diaTieneGeometriaRuta(d: DistribuidoraResumenDiaJson | null | undefined): boolean {
  if (!d) return false
  try {
    return geometryToLatLngs(d.geometry).length >= 2
  } catch {
    return false
  }
}

/** Marcador base (sin iconUrl de Leaflet / bundler → evita “Mark” e imagen rota). */
let resumenBaseIconSingleton: L.DivIcon | null = null
function getResumenVendedorBaseIcon(): L.DivIcon {
  if (!resumenBaseIconSingleton) {
    resumenBaseIconSingleton = L.divIcon({
      className: "base-icon",
      html: "<span style=\"font:700 12px system-ui,sans-serif\">B</span>",
      iconSize: [30, 30],
      iconAnchor: [15, 30],
      popupAnchor: [0, -28],
    })
  }
  return resumenBaseIconSingleton
}

function ResumenMapInvalidate() {
  const map = useMap()
  useEffect(() => {
    const t = window.setTimeout(() => map.invalidateSize(), 200)
    return () => window.clearTimeout(t)
  }, [map])
  return null
}

type RutasCapasProps = {
  resumen: DistribuidoraResumenVendedorJson | null
  visibleDias: Set<string>
  focoDia: string | null
  viewNonce: number
}

const RUTA_WEIGHT_BASE = 5
const RUTA_WEIGHT_FOCO = 6
const RUTA_OPACITY_NORMAL = 0.8
const RUTA_OPACITY_ATENUADA = 0.36
const RUTA_ANIM_MS = 380
const RUTA_ANIM_STAGGER_MS = 55

/** Polilíneas por día: grosor, opacidad 0.8, bordes suaves, animación ligera al aparecer. */
function RutasSemanaCapas({ resumen, visibleDias, focoDia, viewNonce }: RutasCapasProps) {
  const map = useMap()
  const layersRef = useRef<L.Polyline[]>([])
  const animRef = useRef<{ cancel: () => void } | null>(null)

  useEffect(() => {
    animRef.current?.cancel()
    for (const pl of layersRef.current) {
      map.removeLayer(pl)
    }
    layersRef.current = []
    if (!resumen?.dias?.length) return

    let cancelled = false
    const timeouts: ReturnType<typeof setTimeout>[] = []

    const cancel = () => {
      cancelled = true
      for (const t of timeouts) window.clearTimeout(t)
    }
    animRef.current = { cancel }

    let animIndex = 0
    for (const d of resumen.dias) {
      if (!visibleDias.has(String(d.dia))) continue
      let latlngs: L.LatLngTuple[] = []
      try {
        latlngs = geometryToLatLngs(d.geometry)
      } catch {
        latlngs = []
      }
      if (latlngs.length < 2) continue
      const esFoco = focoDia === String(d.dia)
      const atenuar = Boolean(focoDia && focoDia !== d.dia)
      const targetOpacity = atenuar ? RUTA_OPACITY_ATENUADA : RUTA_OPACITY_NORMAL
      const weight = esFoco ? RUTA_WEIGHT_FOCO : RUTA_WEIGHT_BASE

      const pl = L.polyline(latlngs, {
        color: typeof d.color === "string" && d.color ? d.color : "#2563eb",
        weight,
        opacity: 0,
        lineJoin: "round",
        lineCap: "round",
      })
      pl.addTo(map)
      pl.bindPopup(
        `<strong>${String(d.dia ?? "—")}</strong><br/>${fmtMetric(d.km_totales)} km · ${fmtMetric(d.clientes_count)} clientes`,
      )
      layersRef.current.push(pl)

      const stagger = animIndex * RUTA_ANIM_STAGGER_MS
      animIndex += 1

      const t = window.setTimeout(() => {
        if (cancelled) return
        const start = performance.now()
        const tick = (now: number) => {
          if (cancelled) return
          const p = Math.min(1, (now - start) / RUTA_ANIM_MS)
          const ease = 1 - (1 - p) ** 2
          pl.setStyle({ opacity: targetOpacity * ease })
          if (p < 1) window.requestAnimationFrame(tick)
        }
        window.requestAnimationFrame(tick)
      }, stagger)
      timeouts.push(t)
    }

    return () => {
      cancel()
      for (const pl of layersRef.current) {
        map.removeLayer(pl)
      }
      layersRef.current = []
      animRef.current = null
    }
  }, [map, resumen, visibleDias, focoDia, viewNonce])

  return null
}

function sortedClientesForMap(dia: DistribuidoraResumenDiaJson): Record<string, unknown>[] {
  try {
    const raw = dia?.clientes
    if (!Array.isArray(raw)) return []
    return [...(raw as Record<string, unknown>[])].sort(
      (a, b) =>
        (Number(a.orden_manual ?? a.orden_visita) || 0) -
        (Number(b.orden_manual ?? b.orden_visita) || 0),
    )
  } catch {
    return []
  }
}

/** Marcadores con número de orden (misma vista que se captura en el PDF). */
function ResumenClienteMarkersVisita({ dias }: { dias: DistribuidoraResumenDiaJson[] }) {
  const markers = useMemo(() => {
    try {
      return dias.flatMap((d) =>
        sortedClientesForMap(d)
          .map((c, i) => {
            const lat = Number(c.lat)
            const lon = Number(c.lon)
            if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null
            const ov = Number(c.orden_manual ?? c.orden_visita) || i + 1
            const col = /^#([0-9A-Fa-f]{6}|[0-9A-Fa-f]{3})$/.test(String(d.color || "").trim())
              ? String(d.color).trim()
              : "#2563eb"
            const icon = L.divIcon({
              className: "resumen-vendedor-visit-pin",
              html: `<div style="width:22px;height:22px;border-radius:999px;border:2.5px solid ${col};background:#fff;color:#0f172a;display:flex;align-items:center;justify-content:center;font:700 11px system-ui,-apple-system,sans-serif;line-height:1;box-shadow:0 1px 4px rgba(15,23,42,0.2);">${ov}</div>`,
              iconSize: [22, 22],
              iconAnchor: [11, 22],
              popupAnchor: [0, -20],
            })
            const nombre = String(
              c.cliente_nombre ?? c.nombre_fantasia ?? c.nombre ?? "Cliente",
            ).trim()
            const diaKey = String(d?.dia ?? i)
            return (
              <Marker key={`${diaKey}-${String(c.bsale_id ?? i)}`} position={[lat, lon]} icon={icon}>
                <Popup>
                  <span className="text-sm">
                    <strong>{ov}.</strong> {nombre}
                    <br />
                    <span className="text-muted-foreground">{diaKey}</span>
                  </span>
                </Popup>
              </Marker>
            )
          })
          .filter(Boolean),
      )
    } catch (e) {
      if (process.env.NODE_ENV === "development") {
        console.error("[resumen-vendedor] ResumenClienteMarkersVisita", e)
      }
      return []
    }
  }, [dias])
  return <>{markers}</>
}

function BaseMarkerResumen({ resumen }: { resumen: DistribuidoraResumenVendedorJson | null }) {
  const base = resumen?.dias?.[0]?.base as Record<string, unknown> | undefined
  const lat = base?.lat != null ? Number(base.lat) : null
  const lon = base?.lon != null ? Number(base.lon) : null
  const nombre = (base?.nombre as string) || "Base"
  if (lat == null || lon == null || Number.isNaN(lat) || Number.isNaN(lon)) return null
  return (
    <Marker position={[lat, lon]} icon={getResumenVendedorBaseIcon()}>
      <Popup>{nombre}</Popup>
    </Marker>
  )
}

/** Registra instancia Leaflet y ajusta zoom al bloque de días (PDF + pantalla). */
function BloqueMapFit({
  resumen,
  visibleSet,
  blockIndex,
  mapsRef,
}: {
  resumen: DistribuidoraResumenVendedorJson
  visibleSet: Set<string>
  blockIndex: number
  mapsRef: MutableRefObject<(L.Map | null)[]>
}) {
  const map = useMap()
  useEffect(() => {
    mapsRef.current[blockIndex] = map
    return () => {
      mapsRef.current[blockIndex] = null
    }
  }, [map, blockIndex, mapsRef])

  useEffect(() => {
    if (!resumen?.dias?.length || visibleSet.size === 0) return
    const t = window.setTimeout(() => {
      try {
        fitMapToResumenBounds(map, resumen, { visibleDias: visibleSet, padding: [44, 44] })
        map.invalidateSize({ animate: false })
      } catch {
        /* */
      }
    }, 520)
    return () => window.clearTimeout(t)
  }, [map, resumen, visibleSet])

  return null
}

function MapaBloqueResumen({
  resumen,
  chunk,
  blockIndex,
  mapsRef,
}: {
  resumen: DistribuidoraResumenVendedorJson
  chunk: DistribuidoraResumenDiaJson[]
  blockIndex: number
  mapsRef: MutableRefObject<(L.Map | null)[]>
}) {
  const visibleSet = useMemo(
    () => new Set(chunk.map((d) => String(d.dia ?? "")).filter(Boolean)),
    [chunk],
  )
  const titulo = useMemo(
    () => chunk.map((d) => String(d.dia ?? "—").trim()).filter(Boolean).join(" · ") || "Bloque",
    [chunk],
  )

  return (
    <Card className="overflow-hidden border shadow-sm">
      <CardHeader className="border-b bg-muted/40 py-3">
        <CardTitle className="text-base font-semibold tracking-tight">Rutas: {titulo}</CardTitle>
        <CardDescription className="text-xs">
          Mapa centrado en estos días; colores y numeración de visitas coinciden con la leyenda de la
          semana.
        </CardDescription>
      </CardHeader>
      <CardContent className="p-0">
        <div className="resumen-pdf-mapa-bloque relative h-[min(52vh,440px)] w-full min-h-[300px]">
          <MapContainer center={MAP_CENTER} zoom={MAP_ZOOM} className="h-full w-full z-0" style={{ minHeight: 300 }}>
            <BloqueMapFit resumen={resumen} visibleSet={visibleSet} blockIndex={blockIndex} mapsRef={mapsRef} />
            <TileLayer attribution="&copy; CARTO" url={CARTO_LIGHT} />
            <ResumenMapInvalidate />
            <RutasSemanaCapas
              resumen={resumen}
              visibleDias={visibleSet}
              focoDia={null}
              viewNonce={blockIndex}
            />
            <BaseMarkerResumen resumen={resumen} />
            <ResumenClienteMarkersVisita dias={chunk} />
          </MapContainer>
        </div>
      </CardContent>
    </Card>
  )
}

export default function ResumenVendedorClient() {
  const [mapaClientes, setMapaClientes] = useState<DistribuidoraMapaCliente[]>([])
  const [vendedoresMapa, setVendedoresMapa] = useState<string[]>([])
  const [cargandoMapa, setCargandoMapa] = useState(true)
  const [vendedor, setVendedor] = useState<string>("")
  const [resumen, setResumen] = useState<DistribuidoraResumenVendedorJson | null>(null)
  const [cargandoResumen, setCargandoResumen] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [rendimientoKmL, setRendimientoKmL] = useState("")
  const [precioCombustible, setPrecioCombustible] = useState("")
  const [vistaImpresionError, setVistaImpresionError] = useState<string | null>(null)
  const [pdfGenerando, setPdfGenerando] = useState(false)
  const [pdfError, setPdfError] = useState<string | null>(null)
  /** Una entrada Leaflet por bloque de mapa (mismo orden que chunkDias). */
  const blockMapsRef = useRef<(L.Map | null)[]>([])

  useEffect(() => {
    let cancel = false
    ;(async () => {
      try {
        const data = await getDistribuidoraMapa()
        if (cancel) return
        setMapaClientes(data.clientes ?? [])
        setVendedoresMapa(
          Array.isArray(data.vendedores)
            ? data.vendedores.map((x) => String(x ?? "").trim()).filter(Boolean)
            : [],
        )
      } catch (e) {
        if (!cancel) setError(e instanceof Error ? e.message : "Error al cargar vendedores")
      } finally {
        if (!cancel) setCargandoMapa(false)
      }
    })()
    return () => {
      cancel = true
    }
  }, [])

  const vendedores = useMemo(() => {
    const s = new Set<string>()
    for (const v0 of vendedoresMapa) {
      if (v0) s.add(v0)
    }
    for (const c of mapaClientes) {
      const v = (c.vendedor ?? "").trim()
      if (v) s.add(v)
    }
    return [...s].sort((a, b) => a.localeCompare(b, "es"))
  }, [vendedoresMapa, mapaClientes])

  const cargarResumen = useCallback(async (v: string) => {
    if (!v) return
    setCargandoResumen(true)
    setError(null)
    setVistaImpresionError(null)
    setPdfError(null)
    try {
      const data = await getDistribuidoraResumenVendedor(v)
      setResumen(data)
    } catch (e) {
      setResumen(null)
      setError(e instanceof Error ? e.message : "Error al cargar resumen")
    } finally {
      setCargandoResumen(false)
    }
  }, [])

  /** Debe declararse antes de `descargarAnalisisPdf` (evita TDZ: "Cannot access … before initialization"). */
  const viaticoEstimado = useMemo(() => {
    if (!resumen) return null
    const km = Number(resumen.km_total_semana)
    if (!Number.isFinite(km) || km <= 0) return null
    const rend = parseNumInputLoose(rendimientoKmL)
    const precio = parseNumInputLoose(precioCombustible)
    if (rend == null || precio == null) return null
    if (rend < MIN_RENDIMIENTO_KM_L || precio < MIN_PRECIO_COMBUSTIBLE_CLP) return null
    const litros = km / rend
    const clp = litros * precio
    if (!Number.isFinite(clp) || clp < 0) return null
    return Math.round(clp)
  }, [resumen, rendimientoKmL, precioCombustible])

  const diasLista = useMemo(() => resumen?.dias ?? [], [resumen])
  const diasChunks = useMemo(() => chunkDias(diasLista, 2), [diasLista])
  const filasClientesSemana = useMemo(
    () => (resumen ? buildConsolidatedSemanaClientRows(resumen) : []),
    [resumen],
  )
  const lineasResumenDia = useMemo(
    () => (resumen ? buildDiasCargaSummaryLines(resumen) : []),
    [resumen],
  )
  const analisisUi = useMemo(() => safeBuildOperationalInsights(resumen), [resumen])
  const eficienciaUi = useMemo(() => safeClasificarEficiencia(resumen), [resumen])

  const descargarAnalisisPdf = useCallback(async () => {
    if (!resumen) return
    setPdfError(null)
    setPdfGenerando(true)
    const diasSafe = diasLista
    try {
      const chunks = chunkDias(diasSafe, 2)
      const roots = document.querySelectorAll<HTMLElement>(".resumen-pdf-mapa-bloque")
      const mapBlocks: { title: string; dataUrl: string }[] = []

      for (let i = 0; i < chunks.length; i++) {
        const chunk = chunks[i]
        const ids = new Set(chunk.map((d) => String(d.dia ?? "")).filter(Boolean))
        if (ids.size === 0) continue
        const map = blockMapsRef.current[i]
        if (map) {
          try {
            fitMapToResumenBounds(map, resumen, { visibleDias: ids, padding: [40, 40] })
            map.invalidateSize({ animate: false })
          } catch {
            /* */
          }
        }
        await new Promise((r) => setTimeout(r, 750))
        const el = roots[i]
        if (!el) continue
        try {
          const dataUrl = await captureMapElementJpeg(el)
          const title =
            chunk
              .map((d) => String(d.dia ?? "—").trim())
              .filter(Boolean)
              .join(" y ") || "Bloque"
          mapBlocks.push({ title, dataUrl })
        } catch {
          /* omitir bloque */
        }
      }

      const rend = parseNumInputLoose(rendimientoKmL)
      const precio = parseNumInputLoose(precioCombustible)
      await exportResumenVendedorPdf({
        resumen,
        mapBlocks,
        viaticoClp: viaticoEstimado,
        rendimientoKmL: rend,
        precioCombustibleClp: precio,
      })
    } catch (e) {
      setPdfError(e instanceof Error ? e.message : "No se pudo generar el PDF.")
    } finally {
      setPdfGenerando(false)
    }
  }, [resumen, diasLista, viaticoEstimado, rendimientoKmL, precioCombustible])

  return (
    <div className="space-y-4 p-4">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Resumen semanal por vendedor</h1>
          <p className="text-sm text-muted-foreground">
            Informe semanal: mapas por bloques de días, listado único de clientes y resumen compacto por
            jornada. El análisis operativo va al final.
          </p>
        </div>
        <div className="flex w-full max-w-sm flex-col gap-1">
          <span className="text-xs font-medium text-muted-foreground">Vendedor</span>
          <Select
            value={vendedor || undefined}
            onValueChange={(v) => {
              setVendedor(v)
              void cargarResumen(v)
            }}
            disabled={cargandoMapa || vendedores.length === 0}
          >
            <SelectTrigger>
              <SelectValue placeholder={cargandoMapa ? "Cargando…" : "Seleccione vendedor"} />
            </SelectTrigger>
            <SelectContent>
              {vendedores.map((v) => (
                <SelectItem key={v} value={v}>
                  {v}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>

      {error ? (
        <div className="rounded-lg border border-destructive/40 bg-destructive/10 px-3 py-2 text-sm text-destructive">
          {error}
        </div>
      ) : null}

      {cargandoResumen ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" />
          Cargando rutas…
        </div>
      ) : null}

      {resumen && (
        <>
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">Km semana</CardTitle>
              </CardHeader>
              <CardContent className="text-2xl font-semibold">{fmtMetric(resumen.km_total_semana)}</CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">Tiempo (min)</CardTitle>
              </CardHeader>
              <CardContent className="text-2xl font-semibold">{fmtMetric(resumen.min_total_semana)}</CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">Clientes</CardTitle>
              </CardHeader>
              <CardContent className="text-2xl font-semibold">{fmtMetric(resumen.clientes_total_semana)}</CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">Prom. km / día</CardTitle>
              </CardHeader>
              <CardContent className="text-2xl font-semibold">{fmtMetric(resumen.promedio_km_por_dia)}</CardContent>
            </Card>
          </div>

          <div className="grid gap-4 lg:grid-cols-[1fr_280px]">
            <div className="grid gap-3 sm:grid-cols-2">
              <div className="space-y-2">
                <Label htmlFor="resumen-rendimiento">Rendimiento vehículo (km/l)</Label>
                <Input
                  id="resumen-rendimiento"
                  type="number"
                  inputMode="decimal"
                  min={MIN_RENDIMIENTO_KM_L}
                  step={0.1}
                  placeholder="Ej. 12"
                  value={rendimientoKmL}
                  onChange={(e) => setRendimientoKmL(e.target.value)}
                />
                <p className="text-xs text-muted-foreground">
                  Mín. {MIN_RENDIMIENTO_KM_L} km/l (evita división por cero).
                </p>
              </div>
              <div className="space-y-2">
                <Label htmlFor="resumen-precio-comb">Precio combustible (CLP/l)</Label>
                <Input
                  id="resumen-precio-comb"
                  type="number"
                  inputMode="numeric"
                  min={MIN_PRECIO_COMBUSTIBLE_CLP}
                  step={1}
                  placeholder="Ej. 1200"
                  value={precioCombustible}
                  onChange={(e) => setPrecioCombustible(e.target.value)}
                />
                <p className="text-xs text-muted-foreground">
                  Mín. {MIN_PRECIO_COMBUSTIBLE_CLP} CLP/l.
                </p>
              </div>
            </div>
            <Card className="border-primary/20 bg-primary/5">
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">
                  Viático estimado semanal
                </CardTitle>
              </CardHeader>
              <CardContent>
                {viaticoEstimado != null ? (
                  <p className="text-2xl font-semibold tabular-nums tracking-tight">
                    {formatViaticoCLP(viaticoEstimado)}
                  </p>
                ) : (
                  <p className="text-sm text-muted-foreground">
                    Ingrese rendimiento (≥ {MIN_RENDIMIENTO_KM_L} km/l) y precio (≥{" "}
                    {MIN_PRECIO_COMBUSTIBLE_CLP} CLP/l) para estimar
                    {Number.isFinite(Number(resumen.km_total_semana)) ? (
                      <>
                        {" "}
                        con los <strong className="text-foreground">{fmtMetric(resumen.km_total_semana)}</strong>{" "}
                        km de la semana.
                      </>
                    ) : (
                      <> según el kilometraje de la semana (dato no disponible en este resumen).</>
                    )}
                  </p>
                )}
              </CardContent>
            </Card>
          </div>

          <div className="flex flex-wrap items-center gap-3">
            <Button
              type="button"
              variant="default"
              disabled={pdfGenerando}
              onClick={() => void descargarAnalisisPdf()}
            >
              {pdfGenerando ? <Loader2 className="mr-2 h-4 w-4 animate-spin" aria-hidden /> : null}
              {pdfGenerando ? "Generando PDF…" : "Descargar análisis PDF"}
            </Button>
            <Button
              type="button"
              variant="outline"
              onClick={() => {
                if (!resumen) return
                setVistaImpresionError(null)
                setPdfError(null)
                const w = window.open("", "_blank")
                if (!w) {
                  setVistaImpresionError(RESUMEN_VENDEDOR_PRINT_POPUP_BLOCKED)
                  return
                }
                try {
                  writeResumenVendedorPrintToWindow(w, resumen, viaticoEstimado)
                } catch (e) {
                  w.close()
                  setVistaImpresionError(
                    e instanceof Error ? e.message : "No se pudo cargar la vista para imprimir.",
                  )
                }
              }}
            >
              Vista previa para imprimir
            </Button>
            {pdfError ? <p className="w-full text-sm text-destructive">{pdfError}</p> : null}
            {vistaImpresionError ? (
              <p className="max-w-xl text-sm text-destructive">{vistaImpresionError}</p>
            ) : !pdfError ? (
              <p className="max-w-xl text-xs text-muted-foreground">
                <strong className="text-foreground">PDF:</strong> mapas por bloque, clientes en una tabla y
                análisis al final.
                <span className="mx-1">·</span>
                <strong className="text-foreground">Vista previa:</strong> ventana emergente → Imprimir /
                Guardar como PDF.
              </p>
            ) : null}
          </div>

          {diasLista.length === 0 ? (
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Rutas de la semana</CardTitle>
              </CardHeader>
              <CardContent className="space-y-2 text-sm text-muted-foreground">
                <p>
                  No hay jornadas con datos de ruta para este vendedor en el período consultado. Puede
                  deberse a que aún no se planificaron visitas o a un problema al obtener los datos.
                </p>
                <p className="text-xs">
                  El resto del resumen (métricas y simulación de viático) sigue disponible si el servidor envió
                  totales.
                </p>
              </CardContent>
            </Card>
          ) : (
            <div className="grid gap-4 xl:grid-cols-[1fr_min(280px,32%)]">
              <div className="flex w-full min-w-0 flex-col gap-4">
                {diasChunks.map((chunk, blockIndex) => (
                  <MapaBloqueResumen
                    key={chunk.map((d) => String(d.dia ?? "")).join("|") || `bloque-${blockIndex}`}
                    resumen={resumen}
                    chunk={chunk}
                    blockIndex={blockIndex}
                    mapsRef={blockMapsRef}
                  />
                ))}
              </div>
              <Card className="h-fit xl:sticky xl:top-4">
                <CardHeader className="pb-2">
                  <CardTitle className="text-base">Leyenda (colores por día)</CardTitle>
                  <CardDescription className="text-xs">
                    Misma paleta en todos los mapas. Los números en el mapa son el orden de visita del día.
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-3 text-sm">
                  <div className="rounded-md border bg-muted/30 p-2 text-xs text-muted-foreground">
                    Km día más largo:{" "}
                    <strong className="text-foreground">{fmtMetric(resumen.km_dia_mas_largo)}</strong>
                    <br />
                    Km día más corto:{" "}
                    <strong className="text-foreground">{fmtMetric(resumen.km_dia_mas_corto)}</strong>
                  </div>
                  <ul className="space-y-2">
                    {diasLista.map((d, idx) => (
                      <li key={String(d.dia ?? `dia-${idx}`)} className="flex items-start gap-2">
                        <span
                          className="mt-1 h-3 w-3 shrink-0 rounded-full ring-1 ring-black/10"
                          style={{ backgroundColor: d.color }}
                        />
                        <div className="min-w-0 flex-1">
                          <div className="font-medium">{d.dia ?? "—"}</div>
                          <div className="text-muted-foreground">
                            {fmtMetric(d.km_totales)} km · {fmtMetric(d.clientes_count)} clientes
                            {!diaTieneGeometriaRuta(d) ? (
                              <span className="ml-1 text-xs">· sin trazado en mapa</span>
                            ) : null}
                          </div>
                          {d.alerta_calidad ? (
                            <p className="mt-0.5 text-xs text-amber-800 dark:text-amber-300">
                              Atención: ruta larga por cliente (~{fmtMetric(d.km_por_cliente)} km por cliente).
                            </p>
                          ) : null}
                        </div>
                      </li>
                    ))}
                  </ul>
                </CardContent>
              </Card>
            </div>
          )}

          <Card>
            <CardHeader>
              <CardTitle className="text-base">Clientes de la semana</CardTitle>
              <CardDescription className="text-sm">
                Orden de la semana (lunes a viernes) y orden de visita dentro de cada día. Una sola tabla para
                revisar destinos sin saltar entre listas.
              </CardDescription>
            </CardHeader>
            <CardContent>
              {filasClientesSemana.length === 0 ? (
                <p className="text-sm text-muted-foreground">No hay clientes con coordenadas o datos de visita.</p>
              ) : (
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead className="w-14">Orden</TableHead>
                        <TableHead>Cliente</TableHead>
                        <TableHead className="w-36">Día</TableHead>
                        <TableHead>Comuna</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {filasClientesSemana.map((r) => (
                        <TableRow key={`${r.dia}-${r.ordenGlobal}-${r.nombre}`}>
                          <TableCell className="tabular-nums text-muted-foreground">{r.ordenGlobal}</TableCell>
                          <TableCell className="font-medium">{r.nombre}</TableCell>
                          <TableCell>{r.dia}</TableCell>
                          <TableCell className="text-muted-foreground">{r.comuna}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-base">Resumen por día</CardTitle>
              <CardDescription className="text-sm">
                Carga por jornada; las marcas indican el día con más kilómetros y el de menor recorrido (si
                aplica).
              </CardDescription>
            </CardHeader>
            <CardContent>
              {lineasResumenDia.length === 0 ? (
                <p className="text-sm text-muted-foreground">Sin datos por día.</p>
              ) : (
                <ul className="space-y-1.5 text-sm leading-relaxed">
                  {lineasResumenDia.map((line) => (
                    <li key={line} className="border-b border-border/60 pb-1.5 last:border-0 last:pb-0">
                      {line}
                    </li>
                  ))}
                </ul>
              )}
            </CardContent>
          </Card>
        </>
      )}

      {resumen ? (
        <section
          aria-labelledby="analisis-operativo-heading"
          className="mt-8 scroll-mt-6 border-t border-border pt-6"
        >
          <Card className="border-muted/80 bg-muted/20">
            <CardHeader className="pb-2">
              <CardTitle id="analisis-operativo-heading" className="text-base">
                Análisis operativo
              </CardTitle>
              <CardDescription className="text-sm text-muted-foreground">
                Conclusión del comportamiento de la semana: eficiencia aparente{" "}
                <strong className="font-medium text-foreground">{eficienciaUi.etiqueta}</strong>
                {" — "}
                {eficienciaUi.texto}
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-2 text-sm leading-relaxed">
              {!analisisUi.ok && analisisUi.message ? (
                <p className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-amber-950 dark:border-amber-900/60 dark:bg-amber-950/35 dark:text-amber-100">
                  {analisisUi.message}
                </p>
              ) : null}
              {analisisUi.ok && analisisUi.paragraphs.length === 0 ? (
                <p className="text-muted-foreground">Sin texto de análisis para mostrar.</p>
              ) : null}
              {analisisUi.paragraphs.map((p, i) => (
                <p key={i}>{p}</p>
              ))}
            </CardContent>
          </Card>
        </section>
      ) : null}
    </div>
  )
}
