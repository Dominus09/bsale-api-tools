"use client"

import { useCallback, useEffect, useState } from "react"
import { ExternalLink, Loader2, RefreshCw } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { getDistribuidoraViaticos, type DistribuidoraViaticosJson } from "@/lib/api"

function fmtMoney(n: number): string {
  return new Intl.NumberFormat("es-CL", { maximumFractionDigits: 0 }).format(n)
}

function fmtKm(n: number): string {
  return new Intl.NumberFormat("es-CL", { maximumFractionDigits: 1 }).format(n)
}

function fmtLitros(n: number): string {
  return new Intl.NumberFormat("es-CL", { maximumFractionDigits: 2 }).format(n)
}

function osmLink(lat: unknown, lon: unknown): string | null {
  const la = Number(lat)
  const lo = Number(lon)
  if (!Number.isFinite(la) || !Number.isFinite(lo)) return null
  return `https://www.openstreetmap.org/?mlat=${la}&mlon=${lo}#map=14/${la}/${lo}`
}

export default function ViaticosVistaClient() {
  const [data, setData] = useState<DistribuidoraViaticosJson | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState("")

  const cargar = useCallback(async () => {
    setLoading(true)
    setError("")
    try {
      const json = await getDistribuidoraViaticos({ max_rutas: 120 })
      setData(json)
    } catch (e: unknown) {
      setData(null)
      setError(e instanceof Error ? e.message : "Error al cargar viáticos")
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void cargar()
  }, [cargar])

  const t = data?.totales
  const cfg = data?.config

  return (
    <div className="space-y-6 p-4 md:p-6">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <h1 className="text-xl font-semibold tracking-tight text-foreground">Viáticos</h1>
          <p className="mt-1 max-w-3xl text-sm text-muted-foreground">
            Costo combustible estimado por ruta: km desde la misma lógica que <strong>ruta-detalle</strong>,
            rendimiento por vendedor (v1–v4) y precio en <span className="font-mono text-xs">config_viaticos</span>.
            Totales sobre rutas Lunes–Domingo detectadas en rutero (hasta 120 combinaciones por carga).
          </p>
        </div>
        <Button type="button" variant="outline" size="sm" className="shrink-0 gap-2" disabled={loading} onClick={() => void cargar()}>
          {loading ? <Loader2 className="h-4 w-4 animate-spin" aria-hidden /> : <RefreshCw className="h-4 w-4" aria-hidden />}
          Actualizar
        </Button>
      </div>

      {cfg ? (
        <p className="text-xs text-muted-foreground">
          Config: combustible{" "}
          <span className="font-medium tabular-nums text-foreground">{fmtMoney(Number(cfg.valor_combustible) || 0)}</span>{" "}
          · rendimientos km/L v1–v4:{" "}
          <span className="font-mono tabular-nums text-foreground">
            {[1, 2, 3, 4].map((i) => String(cfg[`rendimiento_v${i}`] ?? "—")).join(" / ")}
          </span>
        </p>
      ) : null}

      {error ? (
        <p className="text-sm text-destructive" role="alert">
          {error}
        </p>
      ) : null}

      {loading && !data ? (
        <div className="flex items-center gap-2 text-muted-foreground">
          <Loader2 className="h-5 w-5 animate-spin" aria-hidden />
          Calculando rutas y costos… (puede tardar)
        </div>
      ) : null}

      {t && data ? (
        <>
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <Card>
              <CardHeader className="pb-2">
                <CardDescription>Total semanal (costo)</CardDescription>
                <CardTitle className="text-2xl tabular-nums">{fmtMoney(t.costo_semanal)}</CardTitle>
              </CardHeader>
            </Card>
            <Card>
              <CardHeader className="pb-2">
                <CardDescription>KM total rutas</CardDescription>
                <CardTitle className="text-2xl tabular-nums">{fmtKm(t.km_total)}</CardTitle>
              </CardHeader>
            </Card>
            <Card>
              <CardHeader className="pb-2">
                <CardDescription>Promedio diario general</CardDescription>
                <CardTitle className="text-2xl tabular-nums">{fmtMoney(t.promedio_diario_general)}</CardTitle>
              </CardHeader>
              <CardContent className="pt-0 text-xs text-muted-foreground">Total ÷ 7 días</CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2">
                <CardDescription>Rutas evaluadas</CardDescription>
                <CardTitle className="text-2xl tabular-nums">{t.rutas_evaluadas}</CardTitle>
              </CardHeader>
            </Card>
          </div>

          <div>
            <h2 className="mb-3 text-sm font-semibold tracking-tight text-foreground">Por vendedor</h2>
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
              {data.por_vendedor.map((pv) => (
                <Card key={pv.vendedor}>
                  <CardHeader className="pb-2">
                    <CardTitle className="text-base">{pv.vendedor}</CardTitle>
                    <CardDescription>
                      {pv.dias_con_ruta} día(s) · {fmtKm(pv.km_total)} km
                    </CardDescription>
                  </CardHeader>
                  <CardContent className="space-y-1 text-sm">
                    <div className="flex justify-between gap-2">
                      <span className="text-muted-foreground">Costo total</span>
                      <span className="font-semibold tabular-nums">{fmtMoney(pv.costo_total)}</span>
                    </div>
                    <div className="flex justify-between gap-2 text-xs text-muted-foreground">
                      <span>Prom. / ruta-día</span>
                      <span className="tabular-nums">{fmtMoney(pv.promedio_diario_rutas)}</span>
                    </div>
                    <div className="flex justify-between gap-2 text-xs text-muted-foreground">
                      <span>Prom. / 7 días</span>
                      <span className="tabular-nums">{fmtMoney(pv.promedio_diario_calendario)}</span>
                    </div>
                  </CardContent>
                </Card>
              ))}
            </div>
          </div>

          <div>
            <h2 className="mb-3 text-sm font-semibold tracking-tight text-foreground">Detalle por ruta</h2>
            <div className="max-h-[min(60vh,560px)] overflow-auto rounded-md border border-border">
              <table className="w-full min-w-[720px] border-collapse text-left text-sm">
                <thead className="sticky top-0 z-10 border-b border-border bg-muted/90 backdrop-blur">
                  <tr>
                    <th className="px-3 py-2 font-medium">Vendedor</th>
                    <th className="px-3 py-2 font-medium">Día</th>
                    <th className="px-3 py-2 font-medium">KM</th>
                    <th className="px-3 py-2 font-medium">Litros</th>
                    <th className="px-3 py-2 font-medium">Costo</th>
                    <th className="px-3 py-2 font-medium">Mapa</th>
                  </tr>
                </thead>
                <tbody>
                  {data.filas.map((f, i) => {
                    const href = f.centro_mapa ? osmLink(f.centro_mapa.lat, f.centro_mapa.lon) : null
                    return (
                      <tr key={`${f.vendedor}-${f.dia}-${i}`} className="border-b border-border/70 last:border-0">
                        <td className="whitespace-nowrap px-3 py-2">{f.vendedor}</td>
                        <td className="px-3 py-2">{f.dia}</td>
                        <td className="px-3 py-2 tabular-nums">{fmtKm(f.km)}</td>
                        <td className="px-3 py-2 tabular-nums">{fmtLitros(f.litros)}</td>
                        <td className="px-3 py-2 tabular-nums font-medium">{fmtMoney(f.costo)}</td>
                        <td className="px-3 py-2">
                          {href ? (
                            <a
                              href={href}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="inline-flex items-center gap-1 text-primary underline-offset-4 hover:underline"
                            >
                              <ExternalLink className="size-3.5 shrink-0" aria-hidden />
                              Base
                            </a>
                          ) : (
                            <span className="text-muted-foreground">—</span>
                          )}
                          {f.error_ruta ? (
                            <span className="mt-0.5 block text-xs text-destructive" title={f.error_ruta}>
                              Error ruta
                            </span>
                          ) : null}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </>
      ) : null}
    </div>
  )
}
