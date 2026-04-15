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
  getDistribuidoraMapa,
  getDistribuidoraResumenVendedor,
  type DistribuidoraMapaCliente,
  type DistribuidoraResumenDiaJson,
  type DistribuidoraResumenVendedorJson,
} from "@/lib/api"
import { geometryToLatLngs } from "@/lib/distribuidora-resumen-geometry"
import { openResumenVendedorPrintPreview } from "@/lib/resumen-vendedor-print-html"
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

/** Marcador base (sin iconUrl de Leaflet / bundler → evita “Mark” e imagen rota). */
let resumenBaseIconSingleton: L.DivIcon | null = null
function getResumenVendedorBaseIcon(): L.DivIcon {
  if (!resumenBaseIconSingleton) {
    resumenBaseIconSingleton = L.divIcon({
      className: "base-icon",
      html: "🏁",
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
      if (!visibleDias.has(d.dia)) continue
      const latlngs = geometryToLatLngs(d.geometry)
      if (latlngs.length < 2) continue
      const esFoco = focoDia === d.dia
      const atenuar = Boolean(focoDia && focoDia !== d.dia)
      const targetOpacity = atenuar ? RUTA_OPACITY_ATENUADA : RUTA_OPACITY_NORMAL
      const weight = esFoco ? RUTA_WEIGHT_FOCO : RUTA_WEIGHT_BASE

      const pl = L.polyline(latlngs, {
        color: d.color,
        weight,
        opacity: 0,
        lineJoin: "round",
        lineCap: "round",
      })
      pl.addTo(map)
      pl.bindPopup(`<strong>${d.dia}</strong><br/>${d.km_totales} km · ${d.clientes_count} clientes`)
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
  /** Incrementar solo al centrar en un día (fitBounds); toggles de capa no resetean cámara. */
  const [viewNonce, setViewNonce] = useState(0)
  const [rendimientoKmL, setRendimientoKmL] = useState("")
  const [precioCombustible, setPrecioCombustible] = useState("")
  const [vistaImpresionError, setVistaImpresionError] = useState<string | null>(null)
  const mapRef = useRef<L.Map | null>(null)
  const autoFitKey = useRef<string | null>(null)

  useEffect(() => {
    if (!resumen?.dias?.length) return
    const key = `${resumen.vendedor}:${resumen.dias.map((x) => x.dia).join("|")}`
    if (autoFitKey.current === key) return
    autoFitKey.current = key
    const all: L.LatLngTuple[] = []
    for (const d of resumen.dias) {
      all.push(...geometryToLatLngs(d.geometry))
    }
    if (all.length < 2) return
    window.setTimeout(() => {
      mapRef.current?.fitBounds(L.latLngBounds(all), { padding: [40, 40], maxZoom: 12 })
    }, 500)
  }, [resumen])

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
    autoFitKey.current = null
    setCargandoResumen(true)
    setError(null)
    setVistaImpresionError(null)
    try {
      const data = await getDistribuidoraResumenVendedor(v)
      setResumen(data)
      setVisibleDias(new Set(data.dias.map((d) => d.dia)))
      setFocoDia(null)
      setViewNonce((n) => n + 1)
    } catch (e) {
      setResumen(null)
      setError(e instanceof Error ? e.message : "Error al cargar resumen")
    } finally {
      setCargandoResumen(false)
    }
  }, [])

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
      const d = resumen.dias.find((x) => x.dia === dia)
      if (!d) return
      const latlngs = geometryToLatLngs(d.geometry)
      if (latlngs.length < 2 || !mapRef.current) return
      setFocoDia(dia)
      setViewNonce((n) => n + 1)
      const b = L.latLngBounds(latlngs)
      mapRef.current.fitBounds(b, { padding: [36, 36], maxZoom: 14 })
    },
    [resumen],
  )

  const limpiarFoco = () => {
    setFocoDia(null)
    setViewNonce((n) => n + 1)
  }

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

  const abrirVistaPreviaImprimir = useCallback(() => {
    if (!resumen) return
    setVistaImpresionError(null)
    try {
      openResumenVendedorPrintPreview(resumen, viaticoEstimado)
    } catch (e) {
      setVistaImpresionError(e instanceof Error ? e.message : "No se pudo abrir la vista para imprimir.")
    }
  }, [resumen, viaticoEstimado])

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
          <div className="flex flex-wrap items-center gap-3">
            <Button type="button" variant="default" onClick={abrirVistaPreviaImprimir}>
              Vista previa para imprimir
            </Button>
            {vistaImpresionError ? (
              <p className="max-w-xl text-sm text-destructive">{vistaImpresionError}</p>
            ) : (
              <p className="max-w-xl text-xs text-muted-foreground">
                Se abre una ventana solo con estilos clásicos (sin oklch). Use{" "}
                <strong className="text-foreground">Imprimir</strong> y elija{" "}
                <strong className="text-foreground">Guardar como PDF</strong> en el navegador.
              </p>
            )}
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
                    {MIN_PRECIO_COMBUSTIBLE_CLP} CLP/l) para estimar con los{" "}
                    <strong className="text-foreground">{resumen.km_total_semana}</strong> km de
                    la semana.
                  </p>
                )}
              </CardContent>
            </Card>
          </div>

          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">Km semana</CardTitle>
              </CardHeader>
              <CardContent className="text-2xl font-semibold">{resumen.km_total_semana}</CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">Tiempo (min)</CardTitle>
              </CardHeader>
              <CardContent className="text-2xl font-semibold">{resumen.min_total_semana}</CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">Clientes</CardTitle>
              </CardHeader>
              <CardContent className="text-2xl font-semibold">{resumen.clientes_total_semana}</CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">Prom. km / día</CardTitle>
              </CardHeader>
              <CardContent className="text-2xl font-semibold">{resumen.promedio_km_por_dia}</CardContent>
            </Card>
          </div>

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
                  </MapContainer>
                </div>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="text-base">Leyenda</CardTitle>
              </CardHeader>
              <CardContent className="space-y-3 text-sm">
                <div className="rounded-md border bg-muted/30 p-2 text-xs text-muted-foreground">
                  Km día más largo: <strong className="text-foreground">{resumen.km_dia_mas_largo}</strong>
                  <br />
                  Km día más corto: <strong className="text-foreground">{resumen.km_dia_mas_corto}</strong>
                </div>
                <ul className="space-y-2">
                  {resumen.dias.map((d) => (
                    <li key={d.dia} className="flex items-start gap-2">
                      <span
                        className="mt-1 h-3 w-3 shrink-0 rounded-full ring-1 ring-black/10"
                        style={{ backgroundColor: d.color }}
                      />
                      <div className="min-w-0 flex-1">
                        <div className="font-medium">{d.dia}</div>
                        <div className="text-muted-foreground">
                          {d.km_totales} km · {d.clientes_count} clientes
                        </div>
                        {d.alerta_calidad ? (
                          <div className="mt-0.5 flex items-center gap-1 text-amber-700 dark:text-amber-400">
                            <AlertTriangle className="h-3.5 w-3.5" />
                            <span className="text-xs">Ruta larga por cliente (~{d.km_por_cliente} km/cli)</span>
                          </div>
                        ) : null}
                        <div className="mt-1 flex flex-wrap gap-1">
                          <Button variant="secondary" size="sm" className="h-7 text-xs" onClick={() => centrarEnDia(d.dia)}>
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

          <Card>
            <CardHeader>
              <CardTitle className="text-base">Detalle por día</CardTitle>
            </CardHeader>
            <CardContent>
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
                  {resumen.dias.map((d: DistribuidoraResumenDiaJson) => (
                    <TableRow key={d.dia}>
                      <TableCell>
                        <Checkbox
                          checked={visibleDias.has(d.dia)}
                          onCheckedChange={() => toggleDia(d.dia)}
                          aria-label={`Mostrar ${d.dia}`}
                        />
                      </TableCell>
                      <TableCell>
                        <span className="mr-2 inline-block h-2 w-2 rounded-full align-middle" style={{ backgroundColor: d.color }} />
                        {d.dia}
                      </TableCell>
                      <TableCell>{d.clientes_count}</TableCell>
                      <TableCell>{d.km_totales}</TableCell>
                      <TableCell>{d.minutos_totales}</TableCell>
                      <TableCell>
                        <Button type="button" variant="ghost" size="sm" className="h-8" onClick={() => centrarEnDia(d.dia)}>
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
            </CardContent>
          </Card>
        </>
      )}
    </div>
  )
}
