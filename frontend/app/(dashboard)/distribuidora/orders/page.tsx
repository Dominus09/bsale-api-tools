"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { Loader2, MapPin, Package, Truck } from "lucide-react"

import {
  getDistribuidoraDispatchPrepByMunicipality,
  getDistribuidoraDispatchPrepObservaciones,
  type DistribuidoraDispatchPrepMunicipalityRow,
} from "@/lib/api"
import { aggregateObservationTags } from "@/lib/dispatch-prep-tags"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Switch } from "@/components/ui/switch"
import { cn } from "@/lib/utils"

function localIsoDate(d = new Date()): string {
  const y = d.getFullYear()
  const m = String(d.getMonth() + 1).padStart(2, "0")
  const day = String(d.getDate()).padStart(2, "0")
  return `${y}-${m}-${day}`
}

const clp = new Intl.NumberFormat("es-CL", {
  style: "currency",
  currency: "CLP",
  maximumFractionDigits: 0,
})

function formatClp(n: number): string {
  return clp.format(Number.isFinite(n) ? n : 0)
}

export default function DistribuidoraOrdersPage() {
  const [dateFrom, setDateFrom] = useState(() => localIsoDate())
  const [dateTo, setDateTo] = useState(() => localIsoDate())
  const [onlyNotInvoiced, setOnlyNotInvoiced] = useState(true)

  const [rows, setRows] = useState<DistribuidoraDispatchPrepMunicipalityRow[]>([])
  const [observationTexts, setObservationTexts] = useState<string[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [detailOpen, setDetailOpen] = useState(false)
  const [detailRow, setDetailRow] =
    useState<DistribuidoraDispatchPrepMunicipalityRow | null>(null)

  useEffect(() => {
    const ac = new AbortController()
    let cancelled = false
    ;(async () => {
      setLoading(true)
      setError(null)
      try {
        const [byMuni, obs] = await Promise.all([
          getDistribuidoraDispatchPrepByMunicipality({
            emission_date_from: dateFrom,
            emission_date_to: dateTo,
            only_not_invoiced: onlyNotInvoiced,
            signal: ac.signal,
          }),
          getDistribuidoraDispatchPrepObservaciones({
            emission_date_from: dateFrom,
            emission_date_to: dateTo,
            only_not_invoiced: onlyNotInvoiced,
            signal: ac.signal,
          }),
        ])
        if (cancelled) return
        setRows(byMuni.items)
        setObservationTexts(obs.items)
      } catch (e: unknown) {
        if (cancelled || (e instanceof Error && e.name === "AbortError")) return
        setError(e instanceof Error ? e.message : "Error al cargar datos")
        setRows([])
        setObservationTexts([])
      } finally {
        if (!cancelled) setLoading(false)
      }
    })()
    return () => {
      cancelled = true
      ac.abort()
    }
  }, [dateFrom, dateTo, onlyNotInvoiced])

  const tagStats = useMemo(
    () => aggregateObservationTags(observationTexts),
    [observationTexts],
  )

  const kpis = useMemo(() => {
    let pedidos = 0
    let ventas = 0
    for (const r of rows) {
      pedidos += Number(r.pedidos) || 0
      ventas += Number(r.total_ventas) || 0
    }
    return {
      comunas: rows.length,
      pedidos,
      ventas,
    }
  }, [rows])

  const openDetail = useCallback((r: DistribuidoraDispatchPrepMunicipalityRow) => {
    setDetailRow(r)
    setDetailOpen(true)
  }, [])

  return (
    <div className="mx-auto flex max-w-6xl flex-col gap-10 pb-16">
      <header className="space-y-2">
        <h1 className="text-3xl font-semibold tracking-tight">
          Pre‑planificación de despacho
        </h1>
        <p className="max-w-2xl text-sm leading-relaxed text-muted-foreground">
          Vista de análisis por comuna y señales de día en observaciones de órdenes de
          compra (sin listado operativo de documentos).
        </p>
      </header>

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      <section className="rounded-2xl border border-border/60 bg-card/40 p-6 shadow-sm backdrop-blur-sm">
        <div className="flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
          <div className="grid gap-5 sm:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="prep-from">Fecha desde</Label>
              <Input
                id="prep-from"
                type="date"
                value={dateFrom}
                onChange={(e) => setDateFrom(e.target.value)}
                disabled={loading}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="prep-to">Fecha hasta</Label>
              <Input
                id="prep-to"
                type="date"
                value={dateTo}
                onChange={(e) => setDateTo(e.target.value)}
                disabled={loading}
              />
            </div>
          </div>
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:gap-6">
            <div className="flex items-center gap-3">
              <Switch
                id="prep-only-open"
                checked={onlyNotInvoiced}
                onCheckedChange={(v) => setOnlyNotInvoiced(v === true)}
                disabled={loading}
              />
              <Label htmlFor="prep-only-open" className="text-sm font-medium">
                Solo no facturadas{" "}
                <span className="block text-xs font-normal text-muted-foreground">
                  Equivale a <code className="text-xs">state = 0</code> en documentos
                  OC
                </span>
              </Label>
            </div>
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="shrink-0 self-start"
              disabled={loading || !onlyNotInvoiced}
              onClick={() => setOnlyNotInvoiced(false)}
            >
              Mostrar todo
            </Button>
          </div>
        </div>
      </section>

      <section className="grid gap-4 sm:grid-cols-3">
        <Card className="border-0 bg-muted/30 py-5 shadow-sm">
          <CardHeader className="pb-2">
            <CardDescription>Comunas con movimiento</CardDescription>
            <CardTitle className="flex items-center gap-2 text-2xl tabular-nums">
              <MapPin className="size-5 text-muted-foreground" />
              {loading ? "—" : kpis.comunas}
            </CardTitle>
          </CardHeader>
        </Card>
        <Card className="border-0 bg-muted/30 py-5 shadow-sm">
          <CardHeader className="pb-2">
            <CardDescription>Pedidos (OC)</CardDescription>
            <CardTitle className="flex items-center gap-2 text-2xl tabular-nums">
              <Package className="size-5 text-muted-foreground" />
              {loading ? "—" : kpis.pedidos.toLocaleString("es-CL")}
            </CardTitle>
          </CardHeader>
        </Card>
        <Card className="border-0 bg-muted/30 py-5 shadow-sm">
          <CardHeader className="pb-2">
            <CardDescription>Monto total</CardDescription>
            <CardTitle className="flex items-center gap-2 text-xl tabular-nums sm:text-2xl">
              <Truck className="size-5 shrink-0 text-muted-foreground" />
              {loading ? "—" : formatClp(kpis.ventas)}
            </CardTitle>
          </CardHeader>
        </Card>
      </section>

      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Loader2 className="size-4 animate-spin" />
          Cargando resumen…
        </div>
      ) : null}

      <div className="grid gap-10 lg:grid-cols-[1fr_min(22rem,100%)] lg:items-start">
        <section className="min-w-0 space-y-3">
          <h2 className="text-sm font-semibold uppercase tracking-wide text-muted-foreground">
            Por comuna
          </h2>
          <div className="overflow-x-auto rounded-xl border border-border/50 bg-background/80">
            <table className="w-full min-w-[28rem] border-collapse text-sm">
              <thead>
                <tr className="text-left text-xs font-medium uppercase tracking-wide text-muted-foreground">
                  <th className="px-4 py-3">Comuna</th>
                  <th className="px-4 py-3 text-right">Clientes únicos</th>
                  <th className="px-4 py-3 text-right">Pedidos</th>
                  <th className="px-4 py-3 text-right">Venta total</th>
                </tr>
              </thead>
              <tbody>
                {rows.length === 0 && !loading ? (
                  <tr>
                    <td
                      colSpan={4}
                      className="px-4 py-10 text-center text-muted-foreground"
                    >
                      Sin datos en el rango seleccionado.
                    </td>
                  </tr>
                ) : (
                  rows.map((r) => (
                    <tr
                      key={r.municipality}
                      className={cn(
                        "border-t border-border/40 transition-colors",
                        "hover:bg-muted/50",
                      )}
                    >
                      <td className="px-4 py-2.5 font-medium">
                        <button
                          type="button"
                          className="rounded text-left underline-offset-2 hover:underline"
                          onClick={() => openDetail(r)}
                        >
                          {r.municipality}
                        </button>
                      </td>
                      <td className="px-4 py-2.5 text-right tabular-nums text-muted-foreground">
                        {Number(r.clientes_unicos).toLocaleString("es-CL")}
                      </td>
                      <td className="px-4 py-2.5 text-right tabular-nums text-muted-foreground">
                        {Number(r.pedidos).toLocaleString("es-CL")}
                      </td>
                      <td className="px-4 py-2.5 text-right tabular-nums">
                        {formatClp(Number(r.total_ventas))}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </section>

        <aside className="space-y-3 rounded-xl border border-border/50 bg-muted/20 p-5">
          <h2 className="text-sm font-semibold uppercase tracking-wide text-muted-foreground">
            Observaciones
          </h2>
          <p className="text-xs leading-relaxed text-muted-foreground">
            Hasta 2000 textos de observaciones en el rango. Se detectan días de la semana
            y palabras como entrega, reparto o retiro para agrupar frecuencias.
          </p>
          <div className="flex flex-wrap gap-2">
            {tagStats.length === 0 && !loading ? (
              <span className="text-sm text-muted-foreground">
                Sin menciones de días en observaciones.
              </span>
            ) : (
              tagStats.map(({ tag, count }) => (
                <span
                  key={tag}
                  className="inline-flex items-center rounded-full border border-border/60 bg-background px-3 py-1 text-xs font-medium shadow-sm"
                >
                  {tag}{" "}
                  <span className="ml-1 tabular-nums text-muted-foreground">
                    ({count})
                  </span>
                </span>
              ))
            )}
          </div>
        </aside>
      </div>

      <section className="space-y-3">
        <h2 className="text-sm font-semibold uppercase tracking-wide text-muted-foreground">
          Resumen compacto
        </h2>
        <div className="max-w-xl overflow-x-auto rounded-lg border border-border/40 bg-background/90 text-xs">
          <table className="w-full border-collapse">
            <thead>
              <tr className="border-b border-border/50 bg-muted/40 text-left text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
                <th className="px-2 py-1.5">Comuna</th>
                <th className="px-2 py-1.5 text-right">Clientes únicos</th>
                <th className="px-2 py-1.5 text-right">Venta total</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={`sum-${r.municipality}`} className="border-t border-border/30">
                  <td className="px-2 py-1 font-medium">{r.municipality}</td>
                  <td className="px-2 py-1 text-right tabular-nums">
                    {Number(r.clientes_unicos).toLocaleString("es-CL")}
                  </td>
                  <td className="px-2 py-1 text-right tabular-nums">
                    {formatClp(Number(r.total_ventas))}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <Dialog open={detailOpen} onOpenChange={setDetailOpen}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>{detailRow?.municipality ?? "Comuna"}</DialogTitle>
            <DialogDescription>
              Resumen agregado en el rango de fechas y filtros actuales.
            </DialogDescription>
          </DialogHeader>
          {detailRow ? (
            <dl className="grid gap-2 text-sm">
              <div className="flex justify-between gap-4">
                <dt className="text-muted-foreground">Clientes únicos</dt>
                <dd className="font-medium tabular-nums">
                  {Number(detailRow.clientes_unicos).toLocaleString("es-CL")}
                </dd>
              </div>
              <div className="flex justify-between gap-4">
                <dt className="text-muted-foreground">Pedidos</dt>
                <dd className="font-medium tabular-nums">
                  {Number(detailRow.pedidos).toLocaleString("es-CL")}
                </dd>
              </div>
              <div className="flex justify-between gap-4">
                <dt className="text-muted-foreground">Venta total</dt>
                <dd className="font-medium tabular-nums">
                  {formatClp(Number(detailRow.total_ventas))}
                </dd>
              </div>
            </dl>
          ) : null}
        </DialogContent>
      </Dialog>
    </div>
  )
}
