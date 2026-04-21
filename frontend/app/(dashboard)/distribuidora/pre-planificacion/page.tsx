"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import Link from "next/link"
import { useRouter } from "next/navigation"
import { Loader2, RefreshCw } from "lucide-react"

import {
  distribuidoraTruckCapacityLabel,
  getDistribuidoraPlanificacionOrders,
  getDistribuidoraTrucks,
  postDistribuidoraResyncOc,
  type DistribuidoraPlanificacionOrderRow,
  type DistribuidoraTruck,
} from "@/lib/api"
import {
  writePlanificacionPayload,
  type PlanificacionStoredOrder,
} from "@/lib/planificacion-despacho-storage"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
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

const DELIVERY_OPTIONS: { value: string; label: string }[] = [
  { value: "all", label: "Todos" },
  { value: "lunes", label: "Lunes" },
  { value: "martes", label: "Martes" },
  { value: "miercoles", label: "Miércoles" },
  { value: "jueves", label: "Jueves" },
  { value: "viernes", label: "Viernes" },
  { value: "sabado", label: "Sábado" },
]

function localIsoDate(d = new Date()): string {
  const y = d.getFullYear()
  const m = String(d.getMonth() + 1).padStart(2, "0")
  const day = String(d.getDate()).padStart(2, "0")
  return `${y}-${m}-${day}`
}

function formatCLP(n: number): string {
  return n.toLocaleString("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  })
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

type DistribuidoraResyncSummary = { total: number; errores: number }

function resyncSummaryFromApiResponse(data: {
  ok: boolean
  total?: unknown
  errores?: unknown
  result?: unknown
}): DistribuidoraResyncSummary | null {
  if (typeof data.total === "number" && Number.isFinite(data.total)) {
    const errores =
      typeof data.errores === "number" && Number.isFinite(data.errores)
        ? data.errores
        : 0
    return { total: data.total, errores }
  }
  const total = documentsProcessedFromResyncResult(data.result)
  if (total == null) return null
  const r = data.result
  let errores = 0
  if (r && typeof r === "object" && "document_errors" in r) {
    const e = (r as { document_errors?: unknown }).document_errors
    if (typeof e === "number" && Number.isFinite(e)) errores = e
  }
  return { total, errores }
}

function formatResyncCompletedMessage(s: DistribuidoraResyncSummary): string {
  if (s.errores > 0) {
    return `Sync completado: ${s.total} documentos (${s.errores} errores)`
  }
  return `Sync completado: ${s.total} documentos`
}

const TRUCK_UNSET = "__unset__"

export default function PrePlanificacionDespachoPage() {
  const router = useRouter()
  const [dateFrom, setDateFrom] = useState(() => localIsoDate())
  const [dateTo, setDateTo] = useState(() => localIsoDate())
  const [deliveryDay, setDeliveryDay] = useState("all")

  const [rows, setRows] = useState<DistribuidoraPlanificacionOrderRow[]>([])
  const [loading, setLoading] = useState(true)
  const [loadingSync, setLoadingSync] = useState(false)
  const [lastOrdersLoadAt, setLastOrdersLoadAt] = useState<string | null>(null)
  const [lastResyncSummary, setLastResyncSummary] =
    useState<DistribuidoraResyncSummary | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [feedback, setFeedback] = useState<string | null>(null)

  const [selected, setSelected] = useState<Set<number>>(() => new Set())
  const [truckIdByDoc, setTruckIdByDoc] = useState<Record<number, number | null>>({})
  const [trucks, setTrucks] = useState<DistribuidoraTruck[]>([])
  const [trucksError, setTrucksError] = useState<string | null>(null)

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
        setTrucksError(
          e instanceof Error ? e.message : "No se pudieron cargar los camiones",
        )
      }
    })()
    return () => ac.abort()
  }, [])

  const loadPlanificacionRows = useCallback(
    async (signal?: AbortSignal) => {
      setLoading(true)
      setError(null)
      try {
        const res = await getDistribuidoraPlanificacionOrders({
          emission_date_from: dateFrom,
          emission_date_to: dateTo,
          delivery_day: deliveryDay === "all" ? undefined : deliveryDay,
          signal,
        })
        setRows(res.items)
        console.log("📋 Órdenes cargadas:", res.items.length)
        setLastOrdersLoadAt(
          new Date().toLocaleString("es-CL", {
            dateStyle: "short",
            timeStyle: "medium",
          }),
        )
        setSelected(new Set())
        setTruckIdByDoc({})
      } catch (e: unknown) {
        if (e instanceof Error && e.name === "AbortError") return
        setError(e instanceof Error ? e.message : "Error al cargar")
        setRows([])
      } finally {
        setLoading(false)
      }
    },
    [dateFrom, dateTo, deliveryDay],
  )

  useEffect(() => {
    const ac = new AbortController()
    void loadPlanificacionRows(ac.signal)
    return () => ac.abort()
  }, [loadPlanificacionRows])

  const onResyncOrders = useCallback(async () => {
    const emissionDateFrom = dateFrom
    const emissionDateTo = dateTo
    console.log("🔄 Iniciando actualización órdenes")
    console.log("📅 Rango fechas:", emissionDateFrom, "→", emissionDateTo)
    setLoadingSync(true)
    try {
      const syncRes = await postDistribuidoraResyncOc()
      console.log("✅ Resync respuesta:", syncRes)
      if (syncRes.ok) {
        setLastResyncSummary(resyncSummaryFromApiResponse(syncRes))
      } else {
        setLastResyncSummary(null)
      }
      await loadPlanificacionRows()
      console.log("📦 Órdenes recargadas correctamente")
    } catch (e) {
      console.error("❌ Error en resync:", e)
      setLastResyncSummary(null)
    } finally {
      setLoadingSync(false)
    }
  }, [loadPlanificacionRows, dateFrom, dateTo])

  const toggle = useCallback((id: number, checked: boolean, canSelect: boolean) => {
    if (!canSelect) return
    setSelected((prev) => {
      const n = new Set(prev)
      if (checked) n.add(id)
      else n.delete(id)
      return n
    })
  }, [])

  const selectedWithGeo = useMemo(() => {
    const list: DistribuidoraPlanificacionOrderRow[] = []
    for (const r of rows) {
      if (!selected.has(r.document_id)) continue
      if (!r.has_georef || r.lat == null || r.lng == null) continue
      list.push(r)
    }
    return list
  }, [rows, selected])

  const canEnviarPlanificacion = useMemo(() => {
    if (trucks.length === 0 || selected.size === 0) return false
    for (const docId of selected) {
      const row = rows.find((r) => r.document_id === docId)
      const geo = Boolean(row?.has_georef && row.lat != null && row.lng != null)
      const tid = truckIdByDoc[docId]
      const truckOk =
        tid != null &&
        Number.isFinite(tid) &&
        tid > 0 &&
        trucks.some((t) => t.id === tid)
      if (!geo || !truckOk) return false
    }
    return true
  }, [trucks, selected, rows, truckIdByDoc])

  const onSubmit = useCallback(() => {
    setFeedback(null)
    if (selected.size === 0) {
      setFeedback("Seleccione al menos una orden.")
      return
    }
    const missingGeo: number[] = []
    const ordersOut: PlanificacionStoredOrder[] = []
    const byTruckOrder: Record<number, number> = {}

    if (trucks.length === 0) {
      setFeedback("⚠️ No hay camiones disponibles.")
      return
    }

    for (const r of rows) {
      if (!selected.has(r.document_id)) continue
      if (!r.has_georef || r.lat == null || r.lng == null) {
        missingGeo.push(r.document_id)
        continue
      }
      const tid = truckIdByDoc[r.document_id]
      const truck =
        tid != null ? trucks.find((t) => t.id === tid) : undefined
      if (!truck || tid == null) {
        setFeedback("Asigne un camión válido a cada fila seleccionada.")
        return
      }
      const idx = (byTruckOrder[tid] = (byTruckOrder[tid] ?? 0) + 1)
      ordersOut.push({
        document_id: r.document_id,
        client_id: r.client_id ?? null,
        lat: Number(r.lat),
        lng: Number(r.lng),
        truck_id: tid,
        camion: distribuidoraTruckCapacityLabel(truck),
        oc: r.oc ?? null,
        nombre_fantasia: r.nombre_fantasia ?? null,
        total_amount: r.total_amount != null ? Number(r.total_amount) : null,
        stop_index: idx,
      })
    }

    if (missingGeo.length) {
      setFeedback("Hay filas seleccionadas sin georreferencia; desmárquelas o corrija datos de cliente.")
      return
    }

    writePlanificacionPayload({
      submittedAt: new Date().toISOString(),
      orders: ordersOut,
    })

    router.push("/distribuidora/planificacion")
  }, [rows, selected, truckIdByDoc, trucks, router])

  return (
    <div className="mx-auto flex max-w-[1400px] flex-col gap-8 pb-16">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight">Pre‑planificación de despacho</h1>
          <p className="text-sm text-muted-foreground">
            Selección manual de órdenes de compra (tipo 33) con filtro por día en observaciones.
          </p>
        </div>
        <Button asChild variant="outline" size="sm">
          <Link href="/distribuidora/planificacion">Ir a planificación (mapa)</Link>
        </Button>
      </div>

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {feedback ? (
        <Alert>
          <AlertTitle>No se puede continuar</AlertTitle>
          <AlertDescription>{feedback}</AlertDescription>
        </Alert>
      ) : null}

      <div className="flex flex-col gap-2 sm:flex-row sm:flex-wrap sm:items-center sm:gap-4">
        <Button
          type="button"
          variant="outline"
          size="sm"
          disabled={loading || loadingSync}
          onClick={() => void onResyncOrders()}
          className="w-fit gap-2"
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
      <div className="mb-2 space-y-1 text-xs text-muted-foreground">
        {lastOrdersLoadAt ? (
          <p>Última actualización: {lastOrdersLoadAt}</p>
        ) : null}
        {lastResyncSummary != null ? (
          <p className="text-foreground/90">{formatResyncCompletedMessage(lastResyncSummary)}</p>
        ) : null}
      </div>

      <section className="grid gap-4 rounded-xl border border-border/60 bg-card/50 p-5 shadow-sm sm:grid-cols-2 lg:grid-cols-4">
        <div className="space-y-2">
          <Label>Fecha desde</Label>
          <Input type="date" value={dateFrom} onChange={(e) => setDateFrom(e.target.value)} />
        </div>
        <div className="space-y-2">
          <Label>Fecha hasta</Label>
          <Input type="date" value={dateTo} onChange={(e) => setDateTo(e.target.value)} />
        </div>
        <div className="space-y-2 sm:col-span-2 lg:col-span-2">
          <Label>Día de entrega (observaciones)</Label>
          <Select value={deliveryDay} onValueChange={setDeliveryDay}>
            <SelectTrigger className="w-full max-w-md">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {DELIVERY_OPTIONS.map((o) => (
                <SelectItem key={o.value} value={o.value}>
                  {o.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <p className="text-xs text-muted-foreground">
            Coincidencia insensible a tildes sobre texto de observaciones y comentarios del documento.
          </p>
        </div>
      </section>

      {loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Loader2 className="size-4 animate-spin" />
          Cargando órdenes…
        </div>
      ) : null}

      {trucksError ? (
        <Alert variant="destructive">
          <AlertTitle>Camiones</AlertTitle>
          <AlertDescription>{trucksError}</AlertDescription>
        </Alert>
      ) : trucks.length === 0 ? (
        <Alert>
          <AlertTitle>Camiones</AlertTitle>
          <AlertDescription>⚠️ No hay camiones disponibles</AlertDescription>
        </Alert>
      ) : null}

      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="text-sm text-muted-foreground">
          Seleccionadas con georef:{" "}
          <strong className="text-foreground">{selectedWithGeo.length}</strong> / {selected.size}{" "}
          marcadas
        </p>
        <Button
          type="button"
          onClick={onSubmit}
          disabled={loading || !canEnviarPlanificacion}
        >
          Enviar a planificación
        </Button>
      </div>

      <div className="overflow-x-auto rounded-xl border border-border/50">
        <Table>
          <TableHeader>
            <TableRow className="hover:bg-transparent">
              <TableHead className="w-10" />
              <TableHead>OC</TableHead>
              <TableHead>Nombre fantasía</TableHead>
              <TableHead>Municipality</TableHead>
              <TableHead>Dirección</TableHead>
              <TableHead>Comuna</TableHead>
              <TableHead>Vendedor</TableHead>
              <TableHead className="text-right">Monto</TableHead>
              <TableHead>Georef</TableHead>
              <TableHead className="min-w-[9rem]">Camión</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.length === 0 && !loading ? (
              <TableRow>
                <TableCell colSpan={10} className="py-10 text-center text-muted-foreground">
                  Sin resultados en el rango y filtro de día.
                </TableCell>
              </TableRow>
            ) : (
              rows.map((r) => {
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
                        checked={selected.has(r.document_id)}
                        disabled={!geo}
                        onCheckedChange={(c) =>
                          toggle(r.document_id, c === true, geo)
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
                    <TableCell className="max-w-[8rem] truncate">{r.comuna?.trim() || "—"}</TableCell>
                    <TableCell className="max-w-[8rem] truncate">
                      {r.seller_name?.trim() || "—"}
                    </TableCell>
                    <TableCell className="text-right tabular-nums">
                      {formatCLP(Number(r.total_amount ?? 0))}
                    </TableCell>
                    <TableCell>
                      {geo ? (
                        <Badge className="bg-emerald-600 hover:bg-emerald-600">Sí</Badge>
                      ) : (
                        <Badge variant="destructive">No</Badge>
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
    </div>
  )
}
