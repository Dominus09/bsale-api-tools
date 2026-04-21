"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { useRouter } from "next/navigation"
import { Loader2, MapPin, Package, RefreshCw, Truck } from "lucide-react"

import {
  distribuidoraTruckCapacityLabel,
  getDistribuidoraDispatchPrepByMunicipality,
  getDistribuidoraDispatchPrepObservaciones,
  getDistribuidoraDispatchPrepPlanningRows,
  getDistribuidoraTrucks,
  postDistribuidoraResyncOc,
  type DistribuidoraDispatchPrepMunicipalityRow,
  type DistribuidoraDispatchPrepPlanningRow,
  type DistribuidoraTruck,
} from "@/lib/api"
import {
  aggregateObservationTags,
  weekdayTokenFromTagLabel,
} from "@/lib/dispatch-prep-tags"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Checkbox } from "@/components/ui/checkbox"
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
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { cn } from "@/lib/utils"
import {
  writePlanificacionPayload,
  type PlanificacionStoredOrder,
} from "@/lib/planificacion-despacho-storage"

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

function documentsProcessedFromResyncResult(result: unknown): number | null {
  if (!result || typeof result !== "object") return null
  const v = (result as { documents_processed?: unknown }).documents_processed
  if (typeof v === "number" && Number.isFinite(v)) return v
  if (typeof v === "string" && v.trim() !== "") {
    const n = Number(v)
    return Number.isFinite(n) ? n : null
  }
  return null
}

/** Valor sentinela en `<select>` nativo para “sin camión”. */
const TRUCK_UNSET = "__unset__"

export default function DistribuidoraOrdersPage() {
  const router = useRouter()
  const [dateFrom, setDateFrom] = useState(() => localIsoDate())
  const [dateTo, setDateTo] = useState(() => localIsoDate())
  const [onlyNotInvoiced, setOnlyNotInvoiced] = useState(true)
  const [activeDayFilter, setActiveDayFilter] = useState<string | null>(null)

  const [rows, setRows] = useState<DistribuidoraDispatchPrepMunicipalityRow[]>([])
  const [observationTexts, setObservationTexts] = useState<string[]>([])
  const [planningRows, setPlanningRows] = useState<DistribuidoraDispatchPrepPlanningRow[]>([])
  const [loading, setLoading] = useState(true)
  const [loadingSync, setLoadingSync] = useState(false)
  const [lastOrdersLoadAt, setLastOrdersLoadAt] = useState<string | null>(null)
  const [lastResyncDocumentsProcessed, setLastResyncDocumentsProcessed] = useState<
    number | null
  >(null)
  const [error, setError] = useState<string | null>(null)
  const [trucks, setTrucks] = useState<DistribuidoraTruck[]>([])
  const [trucksError, setTrucksError] = useState<string | null>(null)
  const [planificacionFeedback, setPlanificacionFeedback] = useState<string | null>(null)

  const [detailOpen, setDetailOpen] = useState(false)
  const [detailRow, setDetailRow] =
    useState<DistribuidoraDispatchPrepMunicipalityRow | null>(null)

  const [selectedPlanning, setSelectedPlanning] = useState<Set<number>>(() => new Set())
  const [truckIdByDoc, setTruckIdByDoc] = useState<Record<number, number | null>>({})

  useEffect(() => {
    const ac = new AbortController()
    ;(async () => {
      setTrucksError(null)
      try {
        const res = await getDistribuidoraTrucks({ signal: ac.signal })
        setTrucks(res.items)
      } catch (e: unknown) {
        if (e instanceof Error && e.name === "AbortError") return
        setTrucks([])
        setTrucksError(e instanceof Error ? e.message : "No se pudieron cargar los camiones")
      }
    })()
    return () => ac.abort()
  }, [])

  const loadDispatchPrep = useCallback(
    async (signal?: AbortSignal) => {
      setLoading(true)
      setError(null)
      try {
        const dayParam = activeDayFilter ?? undefined
        const [byMuni, obs, plan] = await Promise.all([
          getDistribuidoraDispatchPrepByMunicipality({
            emission_date_from: dateFrom,
            emission_date_to: dateTo,
            only_not_invoiced: onlyNotInvoiced,
            day_filter: dayParam,
            signal,
          }),
          getDistribuidoraDispatchPrepObservaciones({
            emission_date_from: dateFrom,
            emission_date_to: dateTo,
            only_not_invoiced: onlyNotInvoiced,
            day_filter: dayParam,
            signal,
          }),
          getDistribuidoraDispatchPrepPlanningRows({
            emission_date_from: dateFrom,
            emission_date_to: dateTo,
            only_not_invoiced: onlyNotInvoiced,
            day_filter: dayParam,
            signal,
          }),
        ])
        setRows(byMuni.items)
        setObservationTexts(obs.items)
        setPlanningRows(plan.items)
        console.log("📋 Órdenes cargadas:", plan.items.length)
        setLastOrdersLoadAt(
          new Date().toLocaleString("es-CL", {
            dateStyle: "short",
            timeStyle: "medium",
          }),
        )
        setSelectedPlanning(new Set())
        setTruckIdByDoc({})
      } catch (e: unknown) {
        if (e instanceof Error && e.name === "AbortError") return
        setError(e instanceof Error ? e.message : "Error al cargar datos")
        setRows([])
        setObservationTexts([])
        setPlanningRows([])
      } finally {
        setLoading(false)
      }
    },
    [dateFrom, dateTo, onlyNotInvoiced, activeDayFilter],
  )

  useEffect(() => {
    const ac = new AbortController()
    void loadDispatchPrep(ac.signal)
    return () => ac.abort()
  }, [loadDispatchPrep])

  const onResyncOrders = useCallback(async () => {
    const emissionDateFrom = dateFrom
    const emissionDateTo = dateTo
    console.log("🔄 Iniciando actualización órdenes")
    console.log("📅 Rango fechas:", emissionDateFrom, "→", emissionDateTo)
    setLoadingSync(true)
    try {
      const syncRes = await postDistribuidoraResyncOc()
      console.log("✅ Resync respuesta:", syncRes)
      const proc = documentsProcessedFromResyncResult(syncRes.result)
      if (syncRes.ok) {
        setLastResyncDocumentsProcessed(proc)
      } else {
        setLastResyncDocumentsProcessed(null)
      }
      await loadDispatchPrep()
      console.log("📦 Órdenes recargadas correctamente")
    } catch (e) {
      console.error("❌ Error en resync:", e)
      setLastResyncDocumentsProcessed(null)
    } finally {
      setLoadingSync(false)
    }
  }, [loadDispatchPrep, dateFrom, dateTo])

  const tagStats = useMemo(
    () => aggregateObservationTags(observationTexts),
    [observationTexts],
  )

  const onChipClick = useCallback((tag: string) => {
    const token = weekdayTokenFromTagLabel(tag)
    if (!token) return
    setActiveDayFilter((prev) => (prev === token ? null : token))
  }, [])

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

  const planningSelectionSummary = useMemo(() => {
    let count = 0
    let amount = 0
    for (const r of planningRows) {
      if (!selectedPlanning.has(r.document_id)) continue
      count += 1
      amount += Number(r.total_amount ?? 0)
    }
    return { count, amount }
  }, [planningRows, selectedPlanning])

  const canPassToPlanificacion = useMemo(() => {
    if (trucks.length === 0 || selectedPlanning.size === 0) return false
    for (const docId of selectedPlanning) {
      const row = planningRows.find((r) => r.document_id === docId)
      const geo = Boolean(
        row?.has_georef && row.lat != null && row.lng != null,
      )
      const tid = truckIdByDoc[docId]
      const truckOk =
        tid != null &&
        Number.isFinite(tid) &&
        tid > 0 &&
        trucks.some((t) => t.id === tid)
      if (!geo || !truckOk) return false
    }
    return true
  }, [trucks, selectedPlanning, planningRows, truckIdByDoc])

  const onPassToPlanificacion = useCallback(() => {
    setPlanificacionFeedback(null)
    if (trucks.length === 0) {
      setPlanificacionFeedback("⚠️ No hay camiones disponibles.")
      return
    }
    if (selectedPlanning.size === 0) {
      setPlanificacionFeedback("Seleccione al menos un pedido con georreferencia y camión.")
      return
    }
    const byTruck: Record<number, number> = {}
    const stored: PlanificacionStoredOrder[] = []

    for (const r of planningRows) {
      if (!selectedPlanning.has(r.document_id)) continue
      const geo = Boolean(r.has_georef && r.lat != null && r.lng != null)
      const tid = truckIdByDoc[r.document_id]
      const truck =
        tid != null ? trucks.find((t) => t.id === tid) : undefined
      if (!geo || !truck) {
        setPlanificacionFeedback(
          "Cada pedido seleccionado debe tener georreferencia y un camión asignado.",
        )
        return
      }
      const idx = (byTruck[tid] = (byTruck[tid] ?? 0) + 1)
      const camion = distribuidoraTruckCapacityLabel(truck)
      stored.push({
        document_id: r.document_id,
        client_id: r.client_id ?? null,
        lat: Number(r.lat),
        lng: Number(r.lng),
        truck_id: tid,
        camion,
        oc: r.oc ?? null,
        nombre_fantasia: r.nombre_fantasia ?? null,
        total_amount: r.total_amount != null ? Number(r.total_amount) : null,
        stop_index: idx,
      })
    }

    writePlanificacionPayload({
      submittedAt: new Date().toISOString(),
      orders: stored,
    })
    router.push("/distribuidora/planificacion")
  }, [trucks, planningRows, selectedPlanning, truckIdByDoc, router])

  const togglePlanning = useCallback(
    (id: number, checked: boolean, canSelect: boolean) => {
      if (!canSelect) return
      setSelectedPlanning((prev) => {
        const n = new Set(prev)
        if (checked) n.add(id)
        else n.delete(id)
        return n
      })
    },
    [],
  )

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
          Análisis por comuna, observaciones con filtro por día y tabla de pre‑planificación
          (órdenes de compra).
        </p>
      </header>

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      <section className="rounded-2xl border border-border/60 bg-card/40 p-6 shadow-sm backdrop-blur-sm">
        <div className="mb-4 flex flex-col gap-3 sm:flex-row sm:flex-wrap sm:items-center sm:justify-between">
          <Button
            type="button"
            variant="outline"
            size="sm"
            disabled={loading || loadingSync}
            onClick={() => void onResyncOrders()}
            className="shrink-0 gap-2"
          >
            {loadingSync ? (
              <Loader2 className="size-4 animate-spin" aria-hidden />
            ) : (
              <RefreshCw className="size-4" aria-hidden />
            )}
            Actualizar órdenes
          </Button>
          {loadingSync ? (
            <span className="text-xs text-muted-foreground">
              Actualizando desde Bsale…
            </span>
          ) : null}
        </div>
        <div className="mb-4 space-y-1 text-xs text-muted-foreground">
          {lastOrdersLoadAt ? (
            <p>Última actualización: {lastOrdersLoadAt}</p>
          ) : null}
          {lastResyncDocumentsProcessed != null ? (
            <p className="text-foreground/90">
              Órdenes actualizadas:{" "}
              <span className="font-mono tabular-nums">
                {lastResyncDocumentsProcessed}
              </span>
            </p>
          ) : null}
        </div>
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
            Pulse un chip para filtrar por ese día en observaciones (otro clic limpia el filtro).
            {activeDayFilter ? (
              <span className="mt-1 flex flex-wrap items-center gap-2 font-medium text-foreground">
                Filtro: <code className="text-xs">{activeDayFilter}</code>
                <button
                  type="button"
                  className="text-xs font-normal text-primary underline-offset-2 hover:underline"
                  onClick={() => setActiveDayFilter(null)}
                >
                  Quitar filtro
                </button>
              </span>
            ) : null}
          </p>
          <div className="flex flex-wrap gap-2">
            {tagStats.length === 0 && !loading ? (
              <span className="text-sm text-muted-foreground">
                Sin menciones de días en observaciones.
              </span>
            ) : (
              tagStats.map(({ tag, count }) => {
                const token = weekdayTokenFromTagLabel(tag)
                const active = token != null && activeDayFilter === token
                const clickable = token != null
                return (
                  <button
                    key={tag}
                    type="button"
                    disabled={!clickable || loading}
                    onClick={() => onChipClick(tag)}
                    className={cn(
                      "inline-flex items-center rounded-full border px-3 py-1 text-xs font-medium shadow-sm transition-colors",
                      "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
                      clickable && "cursor-pointer",
                      !clickable && "cursor-default opacity-80",
                      active
                        ? "border-primary bg-primary text-primary-foreground"
                        : "border-border/60 bg-muted/50 text-muted-foreground hover:bg-muted",
                    )}
                  >
                    {tag}{" "}
                    <span
                      className={cn(
                        "ml-1 tabular-nums",
                        active ? "text-primary-foreground/90" : "text-muted-foreground",
                      )}
                    >
                      ({count})
                    </span>
                  </button>
                )
              })
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

      <section className="space-y-4">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h2 className="text-sm font-semibold uppercase tracking-wide text-muted-foreground">
              Pre‑planificación
            </h2>
            <p className="text-xs text-muted-foreground">
              Órdenes OC en el rango (y con filtro de día si aplica), ordenadas por monto.
            </p>
          </div>
          <Button
            type="button"
            variant="secondary"
            disabled={!canPassToPlanificacion}
            onClick={onPassToPlanificacion}
          >
            Pasar a planificación
          </Button>
        </div>
        {planificacionFeedback ? (
          <Alert>
            <AlertTitle>No se puede continuar</AlertTitle>
            <AlertDescription>{planificacionFeedback}</AlertDescription>
          </Alert>
        ) : null}
        {trucksError ? (
          <Alert variant="destructive">
            <AlertTitle>Camiones</AlertTitle>
            <AlertDescription>{trucksError}</AlertDescription>
          </Alert>
        ) : trucks.length === 0 ? (
          <Alert>
            <AlertTitle>Camiones</AlertTitle>
            <AlertDescription>
              ⚠️ No hay camiones disponibles
            </AlertDescription>
          </Alert>
        ) : null}
        <div className="flex flex-wrap items-center gap-4 rounded-lg border border-border/50 bg-muted/20 px-4 py-3 text-sm">
          <span>
            Pedidos seleccionados:{" "}
            <strong className="tabular-nums text-foreground">
              {planningSelectionSummary.count}
            </strong>
          </span>
          <span>
            Monto seleccionado:{" "}
            <strong className="tabular-nums text-foreground">
              {formatClp(planningSelectionSummary.amount)}
            </strong>
          </span>
        </div>
        <div className="overflow-x-auto rounded-xl border border-border/50">
          <Table>
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <TableHead className="w-10" />
                <TableHead>OC</TableHead>
                <TableHead>Nombre fantasía</TableHead>
                <TableHead>Comuna</TableHead>
                <TableHead>Dirección</TableHead>
                <TableHead>Vendedor</TableHead>
                <TableHead className="text-right">Monto</TableHead>
                <TableHead>Georef</TableHead>
                <TableHead className="min-w-[12rem]">Camión</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {planningRows.length === 0 && !loading ? (
                <TableRow>
                  <TableCell colSpan={9} className="py-10 text-center text-muted-foreground">
                    Sin filas para mostrar (ajuste fechas o filtro de día).
                  </TableCell>
                </TableRow>
              ) : (
                planningRows.map((r) => {
                  const geo = Boolean(r.has_georef && r.lat != null && r.lng != null)
                  const docId = r.document_id
                  const tid = truckIdByDoc[docId]
                  const truck =
                    tid != null ? trucks.find((t) => t.id === tid) : undefined
                  const capLabel = truck ? distribuidoraTruckCapacityLabel(truck) : null
                  return (
                    <TableRow key={r.document_id} className="text-sm">
                      <TableCell>
                        <Checkbox
                          checked={selectedPlanning.has(r.document_id)}
                          disabled={!geo}
                          onCheckedChange={(c) =>
                            togglePlanning(r.document_id, c === true, geo)
                          }
                          aria-label={`Seleccionar OC ${r.oc ?? r.document_id}`}
                        />
                      </TableCell>
                      <TableCell className="font-mono tabular-nums">{r.oc ?? "—"}</TableCell>
                      <TableCell className="max-w-[10rem] truncate">
                        {r.nombre_fantasia?.trim() || "—"}
                      </TableCell>
                      <TableCell className="max-w-[8rem] truncate">
                        {r.municipality?.trim() || "—"}
                      </TableCell>
                      <TableCell className="max-w-[12rem] truncate">
                        {r.direccion?.trim() || "—"}
                      </TableCell>
                      <TableCell className="max-w-[8rem] truncate">
                        {r.seller_name?.trim() || "—"}
                      </TableCell>
                      <TableCell className="text-right tabular-nums">
                        {formatClp(Number(r.total_amount ?? 0))}
                      </TableCell>
                      <TableCell>
                        {geo ? (
                          <Badge className="bg-emerald-600 hover:bg-emerald-600">OK</Badge>
                        ) : (
                          <Badge variant="destructive">Sin coordenadas</Badge>
                        )}
                      </TableCell>
                      <TableCell>
                        <div className="flex min-w-[11rem] flex-col gap-1.5">
                          {trucks.length === 0 ? (
                            <span className="text-xs text-muted-foreground">
                              ⚠️ No hay camiones disponibles
                            </span>
                          ) : (
                          <select
                            className="h-8 max-w-[16rem] rounded-md border border-input bg-background px-2 text-xs shadow-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50"
                            value={
                              tid != null && trucks.some((x) => x.id === tid)
                                ? String(tid)
                                : TRUCK_UNSET
                            }
                            onChange={(e) => {
                              const val =
                                e.target.value === TRUCK_UNSET
                                  ? null
                                  : Number(e.target.value)
                              setTruckIdByDoc((prev) => ({
                                ...prev,
                                [docId]: val,
                              }))
                            }}
                            disabled={!geo}
                            aria-label={`Camión OC ${r.oc ?? docId}`}
                          >
                            <option value={TRUCK_UNSET}>Asignar</option>
                            {trucks.map((t) => (
                              <option key={t.id} value={t.id}>
                                {t.name} ({t.plate})
                              </option>
                            ))}
                          </select>
                          )}
                          {capLabel && geo ? (
                            <Badge
                              variant="secondary"
                              className="w-fit max-w-[16rem] truncate text-[10px] font-normal"
                              title={capLabel}
                            >
                              {capLabel}
                            </Badge>
                          ) : null}
                        </div>
                      </TableCell>
                    </TableRow>
                  )
                })
              )}
            </TableBody>
          </Table>
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
