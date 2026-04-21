"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import Link from "next/link"
import { useRouter } from "next/navigation"
import { Loader2, RefreshCw } from "lucide-react"

import {
  distribuidoraTruckCapacityLabel,
  getDistribuidoraPlanificacionOrders,
  getDistribuidoraSyncStatus,
  getDistribuidoraTrucks,
  postDistribuidoraSyncOrders,
  waitDistribuidoraTypedSyncComplete,
  type DistribuidoraPlanificacionOrderRow,
  type DistribuidoraSyncStatusResponse,
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

function syncStatusEmoji(st: string | undefined): string {
  if (st === "running") return "🟡"
  if (st === "error") return "🔴"
  return "🟢"
}

function formatSyncLastRun(iso: string | null | undefined): string {
  if (!iso) return "—"
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  return d.toLocaleString("es-CL", { dateStyle: "short", timeStyle: "short" })
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
  const [error, setError] = useState<string | null>(null)
  const [feedback, setFeedback] = useState<string | null>(null)
  const [syncStatus, setSyncStatus] = useState<DistribuidoraSyncStatusResponse | null>(null)
  const [syncStatusError, setSyncStatusError] = useState<string | null>(null)

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

  const loadSyncStatus = useCallback(async (signal?: AbortSignal) => {
    try {
      const s = await getDistribuidoraSyncStatus({ signal })
      setSyncStatus(s)
      setSyncStatusError(null)
    } catch (e: unknown) {
      if (e instanceof Error && e.name === "AbortError") return
      setSyncStatus(null)
      setSyncStatusError(e instanceof Error ? e.message : "No se pudo leer el estado de sync.")
    }
  }, [])

  useEffect(() => {
    const ac = new AbortController()
    void loadSyncStatus(ac.signal)
    const id = window.setInterval(() => {
      void loadSyncStatus()
    }, 30_000)
    return () => {
      ac.abort()
      window.clearInterval(id)
    }
  }, [loadSyncStatus])

  const onSyncOrdersFromBsale = useCallback(async () => {
    const ac = new AbortController()
    setLoadingSync(true)
    setFeedback(null)
    let baseline: string | null = null
    try {
      try {
        const st0 = await getDistribuidoraSyncStatus({ signal: ac.signal })
        baseline = st0.orders.last_run ?? null
      } catch {
        baseline = null
      }
      const r = await postDistribuidoraSyncOrders({ signal: ac.signal })
      if (!r.ok) {
        setFeedback(r.error ?? "No se pudo encolar sync de órdenes.")
        return
      }
      await waitDistribuidoraTypedSyncComplete({
        branch: "orders",
        baselineLastRun: baseline,
        signal: ac.signal,
      })
      await loadPlanificacionRows(ac.signal)
      await loadSyncStatus(ac.signal)
      setFeedback("Órdenes sincronizadas. Tabla actualizada.")
    } catch (e: unknown) {
      if (e instanceof Error && e.name === "AbortError") return
      setFeedback(e instanceof Error ? e.message : "Error al sincronizar.")
    } finally {
      setLoadingSync(false)
    }
  }, [loadPlanificacionRows, loadSyncStatus])

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
        <Alert variant={feedback.startsWith("Órdenes") ? "default" : "destructive"}>
          <AlertTitle>{feedback.startsWith("Órdenes") ? "Listo" : "No se puede continuar"}</AlertTitle>
          <AlertDescription>{feedback}</AlertDescription>
        </Alert>
      ) : null}

      {syncStatusError ? (
        <p className="text-xs text-amber-700 dark:text-amber-400">{syncStatusError}</p>
      ) : null}
      {syncStatus ? (
        <div className="rounded-lg border border-border/70 bg-muted/30 px-4 py-3 text-sm">
          <p className="font-medium text-foreground">
            {syncStatusEmoji(syncStatus.orders.status)} Sync órdenes (Bsale tipo 33)
            {syncStatus.orders.status === "running"
              ? " — ejecutando"
              : syncStatus.orders.status === "error"
                ? " — error"
                : " — OK"}
            {syncStatus.sync_lock_active ? " · lock activo" : ""}
          </p>
          <ul className="mt-2 grid gap-1 text-xs text-muted-foreground sm:grid-cols-2">
            <li>
              Última actualización:{" "}
              <span className="text-foreground">{formatSyncLastRun(syncStatus.orders.last_run)}</span>
            </li>
            <li>
              Procesados (último ciclo):{" "}
              <span className="tabular-nums text-foreground">{syncStatus.orders.processed}</span>
            </li>
            <li>
              OC visibles (ventana último sync):{" "}
              <span className="tabular-nums text-foreground">{syncStatus.orders.visibles ?? 0}</span>
            </li>
            <li>
              OC ocultas (con boleta/factura):{" "}
              <span className="tabular-nums text-foreground">{syncStatus.orders.ocultas ?? 0}</span>
            </li>
          </ul>
          <p className="mt-2 text-[11px] text-muted-foreground">
            Estado vía servidor cada 30 s. Las cantidades reflejan la ventana de emisión del último sync
            incremental.
          </p>
        </div>
      ) : null}

      <div className="flex flex-col gap-2 sm:flex-row sm:flex-wrap sm:items-center sm:gap-4">
        <Button
          type="button"
          variant="outline"
          size="sm"
          disabled={loading || loadingSync}
          onClick={() => void onSyncOrdersFromBsale()}
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
            Sincronizando órdenes de compra desde Bsale (tipo 33)…
          </span>
        ) : null}
      </div>
      <div className="mb-2 space-y-1 text-xs text-muted-foreground">
        {lastOrdersLoadAt ? (
          <p>Última actualización: {lastOrdersLoadAt}</p>
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
              <TableHead>Estado real</TableHead>
              <TableHead className="text-right">Monto</TableHead>
              <TableHead>Georef</TableHead>
              <TableHead className="min-w-[9rem]">Camión</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.length === 0 && !loading ? (
              <TableRow>
                <TableCell colSpan={11} className="py-10 text-center text-muted-foreground">
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
                    <TableCell className="whitespace-nowrap text-xs">
                      {r.estado_real === "Facturada" ? (
                        <Badge className="bg-emerald-600 hover:bg-emerald-600">Facturada</Badge>
                      ) : (
                        <Badge variant="secondary">Pendiente</Badge>
                      )}
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
