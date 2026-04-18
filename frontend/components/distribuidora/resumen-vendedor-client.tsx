"use client"

import Link from "next/link"
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
import { AlertTriangle, Loader2, MapPin } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
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
  getDistribuidoraMapa,
  getDistribuidoraResumenVendedor,
  type DistribuidoraMapaCliente,
  type DistribuidoraResumenDiaJson,
  type DistribuidoraResumenVendedorJson,
} from "@/lib/api"
import { geometryToLatLngs } from "@/lib/distribuidora-resumen-geometry"
import {
  safeBuildOperationalInsights,
  safeClasificarEficiencia,
} from "@/lib/resumen-vendedor-analisis"
import { fitMapToResumenBounds } from "@/lib/resumen-vendedor-map-fit"
import { captureMapElementJpeg, exportResumenVendedorPdf } from "@/lib/resumen-vendedor-pdf"
import {
  RESUMEN_VENDEDOR_PRINT_POPUP_BLOCKED,
  writeResumenVendedorPrintToWindow,
} from "@/lib/resumen-vendedor-print-html"

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

function CaptureMapRef({ mapRef }: { mapRef: MutableRefObject<L.Map | null> }) {
  const map = useMap()
  useEffect(() => {
    mapRef.current = map
    return () => {
      mapRef.current = null
    }
  }, [map, mapRef])
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

export default function ResumenVendedorClient() {
  const [mapaClientes, setMapaClientes] = useState<DistribuidoraMapaCliente[]>([])
  const [vendedoresMapa, setVendedoresMapa] = useState<string[]>([])
  const [cargandoMapa, setCargandoMapa] = useState(true)
  const [vendedor, setVendedor] = useState<string>("")
  const [resumen, setResumen] = useState<DistribuidoraResumenVendedorJson | null>(null)
  const [cargandoResumen, setCargandoResumen] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [visibleDias, setVisibleDias] = useState<Set<string>>(new Set())
  const [focoDia, setFocoDia] = useState<string | null>(null)
  const [viewNonce, setViewNonce] = useState(0)
  const [rendimientoKmL, setRendimientoKmL] = useState("")
  const [precioCombustible, setPrecioCombustible] = useState("")
  const [vistaImpresionError, setVistaImpresionError] = useState<string | null>(null)
  const [pdfGenerando, setPdfGenerando] = useState(false)
  const [pdfError, setPdfError] = useState<string | null>(null)
  const mapRef = useRef<L.Map | null>(null)

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
      setVisibleDias(
        new Set((data.dias ?? []).map((d) => String(d.dia ?? "")).filter(Boolean)),
      )
      setFocoDia(null)
      setViewNonce((n) => n + 1)
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
  const analisisUi = useMemo(() => safeBuildOperationalInsights(resumen), [resumen])
  const eficienciaUi = useMemo(() => safeClasificarEficiencia(resumen), [resumen])

  const toggleDia = (dia: string) => {
    setVisibleDias((prev) => {
      const n = new Set(prev)
      if (n.has(dia)) n.delete(dia)
      else n.add(dia)
      return n
    })
  }

  const centrarEnDia = useCallback(
    (dia: string) => {
      if (!resumen) return
      const d = (resumen.dias ?? []).find((x) => String(x.dia) === dia)
      if (!d) return
      let latlngs: L.LatLngTuple[] = []
      try {
        latlngs = geometryToLatLngs(d.geometry)
      } catch {
        return
      }
      if (latlngs.length < 2 || !mapRef.current) return
      setFocoDia(dia)
      setViewNonce((n) => n + 1)
      const b = L.latLngBounds(latlngs)
      mapRef.current.fitBounds(b, { padding: [36, 36], maxZoom: 14 })
    },
    [resumen],
  )

  const ajustarMapaAResumen = useCallback(() => {
    const map = mapRef.current
    if (!map || !resumen?.dias?.length) return
    try {
      fitMapToResumenBounds(map, resumen, { visibleDias, padding: [50, 50] })
      map.invalidateSize({ animate: false })
    } catch {
      /* vista actual se mantiene */
    }
  }, [resumen, visibleDias])

  useEffect(() => {
    if (!resumen?.dias?.length) return
    if (focoDia) return
    const t = window.setTimeout(() => {
      ajustarMapaAResumen()
    }, 700)
    return () => window.clearTimeout(t)
  }, [resumen, visibleDias, focoDia, ajustarMapaAResumen])

  const limpiarFoco = useCallback(() => {
    setFocoDia(null)
    setViewNonce((n) => n + 1)
  }, [])

  const descargarAnalisisPdf = useCallback(async () => {
    if (!resumen) return
    const el = mapRef.current?.getContainer()
    if (!el) {
      setPdfError("El mapa no está listo. Espere un momento e intente de nuevo.")
      return
    }
    setPdfError(null)
    setPdfGenerando(true)
    const prevVisible = new Set(visibleDias)
    const prevFoco = focoDia
    const diasSafe = diasLista
    const todos = new Set(diasSafe.map((d) => String(d.dia ?? "")).filter(Boolean))
    try {
      setFocoDia(null)
      setVisibleDias(todos)
      setViewNonce((n) => n + 1)
      await new Promise((r) => setTimeout(r, 500))

      const map = mapRef.current
      if (map) {
        try {
          fitMapToResumenBounds(map, resumen, { visibleDias: todos, padding: [48, 48] })
          map.invalidateSize({ animate: false })
        } catch {
          /* */
        }
      }
      await new Promise((r) => setTimeout(r, 700))
      const dataUrl = await captureMapElementJpeg(el)

      const rend = parseNumInputLoose(rendimientoKmL)
      const precio = parseNumInputLoose(precioCombustible)
      await exportResumenVendedorPdf({
        resumen,
        mapBlocks: [{ title: "Semana completa", dataUrl }],
        viaticoClp: viaticoEstimado,
        rendimientoKmL: rend,
        precioCombustibleClp: precio,
      })
    } catch (e) {
      setPdfError(e instanceof Error ? e.message : "No se pudo generar el PDF.")
    } finally {
      setVisibleDias(prevVisible)
      setFocoDia(prevFoco)
      setViewNonce((n) => n + 1)
      setPdfGenerando(false)
    }
  }, [resumen, diasLista, visibleDias, focoDia, viaticoEstimado, rendimientoKmL, precioCombustible])

  return (
    <div className="space-y-4 p-4">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Resumen semanal por vendedor</h1>
          <p className="text-sm text-muted-foreground">
            Rutas por día en un solo mapa (colores por día). Reutiliza la misma lógica que ruta detalle.
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
                <strong className="text-foreground">PDF:</strong> descarga directa con mapa y análisis.
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
            <div className="grid gap-3 lg:grid-cols-[1fr_280px]">
              <Card className="overflow-hidden">
                <CardHeader className="flex flex-row items-center justify-between py-3">
                  <CardTitle className="text-base">Mapa</CardTitle>
                  {focoDia ? (
                    <Button variant="outline" size="sm" onClick={limpiarFoco}>
                      Quitar foco
                    </Button>
                  ) : null}
                </CardHeader>
                <CardContent className="p-0">
                  <div className="relative h-[min(72vh,560px)] w-full">
                    <MapContainer center={MAP_CENTER} zoom={MAP_ZOOM} className="h-full w-full z-0">
                      <CaptureMapRef mapRef={mapRef} />
                      <TileLayer attribution="&copy; CARTO" url={CARTO_LIGHT} />
                      <ResumenMapInvalidate />
                      <RutasSemanaCapas
                        resumen={resumen}
                        visibleDias={visibleDias}
                        focoDia={focoDia}
                        viewNonce={viewNonce}
                      />
                      <BaseMarkerResumen resumen={resumen} />
                      <ResumenClienteMarkersVisita
                        dias={diasLista.filter((d) => visibleDias.has(String(d.dia)))}
                      />
                    </MapContainer>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Leyenda</CardTitle>
                  {diasLista.some((d) => /sabado/i.test(String(d.dia ?? ""))) ? (
                    <CardDescription className="text-xs text-muted-foreground">
                      <strong className="font-medium text-foreground">Sabado</strong> (atención extra en rutero) usa
                      color morado fijo en polilíneas y leyenda.
                    </CardDescription>
                  ) : null}
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
                            <div className="mt-0.5 flex items-center gap-1 text-amber-700 dark:text-amber-400">
                              <AlertTriangle className="h-3.5 w-3.5" />
                              <span className="text-xs">
                                Ruta larga por cliente (~{fmtMetric(d.km_por_cliente)} km/cli)
                              </span>
                            </div>
                          ) : null}
                          <div className="mt-1 flex flex-wrap gap-1">
                            <Button
                              variant="secondary"
                              size="sm"
                              className="h-7 text-xs"
                              disabled={!diaTieneGeometriaRuta(d)}
                              title={
                                diaTieneGeometriaRuta(d)
                                  ? undefined
                                  : "Este día no tiene geometría válida para centrar el mapa."
                              }
                              onClick={() => centrarEnDia(String(d.dia))}
                            >
                              <MapPin className="mr-1 h-3 w-3" />
                              Centrar
                            </Button>
                          </div>
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
              <CardTitle className="text-base">Detalle por día</CardTitle>
            </CardHeader>
            <CardContent>
              {diasLista.length === 0 ? (
                <p className="text-sm text-muted-foreground">No hay filas para mostrar.</p>
              ) : (
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-10">Ver</TableHead>
                      <TableHead>Día</TableHead>
                      <TableHead>Clientes</TableHead>
                      <TableHead>Km</TableHead>
                      <TableHead>Min</TableHead>
                      <TableHead>Capa</TableHead>
                      <TableHead className="text-right">Acción</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {diasLista.map((d: DistribuidoraResumenDiaJson, idx) => (
                      <TableRow key={String(d.dia ?? `fila-${idx}`)}>
                        <TableCell>
                          <Checkbox
                            checked={visibleDias.has(String(d.dia))}
                            onCheckedChange={() => toggleDia(String(d.dia))}
                            aria-label={`Mostrar ${String(d.dia)}`}
                          />
                        </TableCell>
                        <TableCell>
                          <span
                            className="mr-2 inline-block h-2 w-2 rounded-full align-middle"
                            style={{ backgroundColor: d.color }}
                          />
                          {d.dia ?? "—"}
                        </TableCell>
                        <TableCell>{fmtMetric(d.clientes_count)}</TableCell>
                        <TableCell>{fmtMetric(d.km_totales)}</TableCell>
                        <TableCell>{fmtMetric(d.minutos_totales)}</TableCell>
                        <TableCell>
                          <Button
                            type="button"
                            variant="ghost"
                            size="sm"
                            className="h-8"
                            disabled={!diaTieneGeometriaRuta(d)}
                            title={
                              diaTieneGeometriaRuta(d)
                                ? undefined
                                : "Sin geometría válida para este día."
                            }
                            onClick={() => centrarEnDia(String(d.dia))}
                          >
                            Centrar mapa
                          </Button>
                        </TableCell>
                        <TableCell className="text-right">
                          <Button variant="outline" size="sm" asChild>
                            <Link href="/distribuidora/rutero">Ver rutero</Link>
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
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
