"use client"

import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { useRouter } from "next/navigation"
import { Loader2, MapPin, Package, RefreshCw, Truck } from "lucide-react"

import {
  distribuidoraTruckCapacityLabel,
  getDistribuidoraDispatchPrepObservaciones,
  getDistribuidoraDispatchPrepPlanningRows,
  getDistribuidoraTrucks,
  postDistribuidoraSyncOrders,
  waitDistribuidoraTypedSyncComplete,
  type DistribuidoraDispatchPrepMunicipalityRow,
  type DistribuidoraDispatchPrepPlanningRow,
  type DistribuidoraTruck,
} from "@/lib/api"
import {
  aggregateObservationTags,
  weekdayTokenFromTagLabel,
} from "@/lib/dispatch-prep-tags"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { PlanningRowsLoadingOverlay } from "@/components/distribuidora/orders/PlanningRowsLoadingOverlay"
import { PreDespachoKpiStrip } from "@/components/distribuidora/orders/PreDespachoKpiStrip"
import { PreDespachoPlanningTable } from "@/components/distribuidora/orders/PreDespachoPlanningTable"
import { PreDespachoStatusChips } from "@/components/distribuidora/orders/PreDespachoStatusChips"
import { Button } from "@/components/ui/button"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import { TooltipProvider } from "@/components/ui/tooltip"
import { cn } from "@/lib/utils"
import {
  aggregateDispatchPrepByMunicipality,
  computePreDespachoStats,
  computeResumenKpis,
  filterPlanningRowsByStatus,
  type PreDespachoOperationalStats,
} from "@/lib/pre-despacho-stats"
import {
  matchesPurchaseStatusFilter,
  type PurchaseInvoiceStatusFilter,
  type PurchaseInvoiceStatusFields,
} from "@/lib/purchase-invoice-status"
import {
  buildClusterLabelByDocumentId,
  buildRouteStubsFromAssignments,
  normMunicipality,
} from "@/lib/distribuidora-logistics"
import {
  writePlanificacionPayload,
  type PlanificacionStoredOrder,
} from "@/lib/planificacion-despacho-storage"
import { toast } from "@/hooks/use-toast"
import {
  isWidePreDespachoRange,
  PRE_DESPACHO_PAGE_LIMIT,
  PRE_DESPACHO_TIMEOUT_MESSAGE,
  PRE_DESPACHO_WIDE_RANGE_HINT,
  rangeSpanDays,
} from "@/lib/pre-despacho-range"

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

const RESUMEN_ESTADO_OPTIONS: {
  value: PurchaseInvoiceStatusFilter
  label: string
}[] = [
  { value: "pending", label: "Pendientes" },
  { value: "probable", label: "Probables" },
  { value: "confirmed", label: "Facturadas" },
  { value: "all", label: "Todos" },
]

const EMPTY_STATUS_COUNTS: Record<PurchaseInvoiceStatusFilter, number> = {
  all: 0,
  pending: 0,
  probable: 0,
  confirmed: 0,
}

const EMPTY_OPERATIONAL_STATS: PreDespachoOperationalStats = {
  totalOrders: 0,
  totalAmount: 0,
  pending: 0,
  invoiced: 0,
  probable: 0,
}

const EMPTY_RESUMEN_KPIS = { comunas: 0, pedidos: 0, ventas: 0 }

/** Valor sentinela en `<select>` nativo para “sin camión”. */
const TRUCK_UNSET = "__unset__"

const LAST_GROUP_TRUCK_STORAGE_KEY = "distribuidora_last_group_truck_id"

type PlanningSortKey = "oc" | "municipality" | "amount"

function rowHasGeo(r: DistribuidoraDispatchPrepPlanningRow): boolean {
  return Boolean(r.has_georef && r.lat != null && r.lng != null)
}

function isValidTruckId(
  tid: number | null | undefined,
  trucks: DistribuidoraTruck[],
): tid is number {
  return (
    tid != null &&
    Number.isFinite(tid) &&
    tid > 0 &&
    trucks.some((t) => t.id === tid)
  )
}

function dispatchPrepLoadErrorMessage(e: unknown): string {
  if (!(e instanceof Error)) return PRE_DESPACHO_TIMEOUT_MESSAGE
  const m = e.message.toLowerCase()
  if (
    m.includes("tardó demasiado") ||
    m.includes("cancelada") ||
    m.includes("timeout") ||
    m.includes("socket hang up") ||
    m.includes("econnreset") ||
    m.includes("failed to fetch") ||
    m.includes("network")
  ) {
    return PRE_DESPACHO_TIMEOUT_MESSAGE
  }
  return e.message || PRE_DESPACHO_TIMEOUT_MESSAGE
}

export default function DistribuidoraOrdersPage() {
  const router = useRouter()
  const todayIso = localIsoDate()
  const [draftDateFrom, setDraftDateFrom] = useState(todayIso)
  const [draftDateTo, setDraftDateTo] = useState(todayIso)
  const [appliedDateFrom, setAppliedDateFrom] = useState(todayIso)
  const [appliedDateTo, setAppliedDateTo] = useState(todayIso)
  const [onlyNotInvoiced, setOnlyNotInvoiced] = useState(true)
  const [estadoResumen, setEstadoResumen] =
    useState<PurchaseInvoiceStatusFilter>("pending")
  const [activeDayFilter, setActiveDayFilter] = useState<string | null>(null)

  const [observationTexts, setObservationTexts] = useState<string[]>([])
  const [planningRows, setPlanningRows] = useState<DistribuidoraDispatchPrepPlanningRow[]>([])
  const [planningHasMore, setPlanningHasMore] = useState(false)
  const [rangeWarning, setRangeWarning] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [loadingMore, setLoadingMore] = useState(false)
  const [loadingSync, setLoadingSync] = useState(false)
  const [wideRangeConfirmOpen, setWideRangeConfirmOpen] = useState(false)
  const initialLoadDone = useRef(false)
  const skipFilterReload = useRef(true)
  const [lastOrdersLoadAt, setLastOrdersLoadAt] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [trucks, setTrucks] = useState<DistribuidoraTruck[]>([])
  const [trucksError, setTrucksError] = useState<string | null>(null)
  const [planificacionFeedback, setPlanificacionFeedback] = useState<string | null>(null)

  const [detailOpen, setDetailOpen] = useState(false)
  const [detailRow, setDetailRow] =
    useState<DistribuidoraDispatchPrepMunicipalityRow | null>(null)

  const [planningSortBy, setPlanningSortBy] = useState<PlanningSortKey>("oc")
  const [groupByMunicipality, setGroupByMunicipality] = useState(false)
  const [truckIdByDoc, setTruckIdByDoc] = useState<Record<number, number | null>>({})
  const [bulkTruckSuggest, setBulkTruckSuggest] = useState<{
    municipality: string
    truckId: number
    count: number
  } | null>(null)
  const [lastSuggestedGroupTruckId, setLastSuggestedGroupTruckId] = useState<
    number | null
  >(null)

  useEffect(() => {
    if (typeof window === "undefined") return
    try {
      const raw = sessionStorage.getItem(LAST_GROUP_TRUCK_STORAGE_KEY)
      const n = raw != null ? Number.parseInt(raw, 10) : NaN
      if (Number.isFinite(n) && n > 0) setLastSuggestedGroupTruckId(n)
    } catch {
      /* ignore */
    }
  }, [])

  useEffect(() => {
    if (
      lastSuggestedGroupTruckId != null &&
      !trucks.some((t) => t.id === lastSuggestedGroupTruckId)
    ) {
      setLastSuggestedGroupTruckId(null)
    }
  }, [trucks, lastSuggestedGroupTruckId])

  useEffect(() => {
    const ac = new AbortController()
    ;(async () => {
      setTrucksError(null)
      try {
        const res = await getDistribuidoraTrucks({ signal: ac.signal })
        setTrucks(Array.isArray(res.items) ? res.items : [])
      } catch (e: unknown) {
        if (e instanceof Error && e.name === "AbortError") return
        setTrucks([])
        setTrucksError(e instanceof Error ? e.message : "No se pudieron cargar los camiones")
      }
    })()
    return () => ac.abort()
  }, [])

  const draftRangeDays = useMemo(
    () => rangeSpanDays(draftDateFrom, draftDateTo),
    [draftDateFrom, draftDateTo],
  )
  const draftIsWide = draftRangeDays > 7
  const draftDatesDirty =
    draftDateFrom !== appliedDateFrom || draftDateTo !== appliedDateTo

  /** Fechas aplicadas + solo-no-facturadas + chip día → API. Estado KPI/tabla es filtro local. */
  const loadDispatchPrep = useCallback(
    async (opts?: {
      signal?: AbortSignal
      append?: boolean
      offset?: number
      dateFrom?: string
      dateTo?: string
    }) => {
      const from = opts?.dateFrom ?? appliedDateFrom
      const to = opts?.dateTo ?? appliedDateTo
      const append = opts?.append === true
      const offset = opts?.offset ?? 0
      if (append) setLoadingMore(true)
      else {
        setLoading(true)
        setError(null)
      }
      try {
        const dayParam = activeDayFilter ?? undefined
        const plan = await getDistribuidoraDispatchPrepPlanningRows({
          emission_date_from: from,
          emission_date_to: to,
          only_not_invoiced: onlyNotInvoiced,
          day_filter: dayParam,
          limit: PRE_DESPACHO_PAGE_LIMIT,
          offset,
          signal: opts?.signal,
        })
        if (!append) {
          const obs = await getDistribuidoraDispatchPrepObservaciones({
            emission_date_from: from,
            emission_date_to: to,
            only_not_invoiced: onlyNotInvoiced,
            day_filter: dayParam,
            limit: PRE_DESPACHO_PAGE_LIMIT,
            offset: 0,
            signal: opts?.signal,
          })
          setObservationTexts(Array.isArray(obs.items) ? obs.items : [])
          setRangeWarning(plan.warning ?? obs.warning ?? null)
        } else {
          setRangeWarning(plan.warning ?? null)
        }
        const newItems = Array.isArray(plan.items) ? plan.items : []
        setPlanningRows((prev) => (append ? [...prev, ...newItems] : newItems))
        setPlanningHasMore(Boolean(plan.has_more))
        if (!append) {
          setLastOrdersLoadAt(
            new Date().toLocaleString("es-CL", {
              dateStyle: "short",
              timeStyle: "medium",
            }),
          )
          setTruckIdByDoc({})
        }
      } catch (e: unknown) {
        if (e instanceof Error && e.name === "AbortError") return
        setError(dispatchPrepLoadErrorMessage(e))
      } finally {
        if (append) setLoadingMore(false)
        else setLoading(false)
      }
    },
    [appliedDateFrom, appliedDateTo, onlyNotInvoiced, activeDayFilter],
  )

  const applyDraftRange = useCallback(async () => {
    setAppliedDateFrom(draftDateFrom)
    setAppliedDateTo(draftDateTo)
    setPlanningHasMore(false)
    await loadDispatchPrep({
      dateFrom: draftDateFrom,
      dateTo: draftDateTo,
      offset: 0,
    })
  }, [draftDateFrom, draftDateTo, loadDispatchPrep])

  const onRequestApplyRange = useCallback(() => {
    if (isWidePreDespachoRange(draftDateFrom, draftDateTo)) {
      setWideRangeConfirmOpen(true)
      return
    }
    void applyDraftRange()
  }, [draftDateFrom, draftDateTo, applyDraftRange])

  useEffect(() => {
    if (initialLoadDone.current) return
    initialLoadDone.current = true
    const ac = new AbortController()
    void loadDispatchPrep({ signal: ac.signal })
    return () => ac.abort()
  }, [loadDispatchPrep])

  useEffect(() => {
    if (skipFilterReload.current) {
      skipFilterReload.current = false
      return
    }
    const ac = new AbortController()
    void loadDispatchPrep({ signal: ac.signal, offset: 0 })
    return () => ac.abort()
  }, [activeDayFilter, onlyNotInvoiced, loadDispatchPrep])

  const loadMorePlanning = useCallback(() => {
    void loadDispatchPrep({
      append: true,
      offset: planningRows.length,
    })
  }, [loadDispatchPrep, planningRows.length])

  const onSyncOrdersFromBsale = useCallback(async () => {
    const ac = new AbortController()
    setLoadingSync(true)
    try {
      const r = await postDistribuidoraSyncOrders({ signal: ac.signal })
      if (!r.ok) {
        toast({
          title: "No se pudo sincronizar",
          description: r.error ?? "Error desconocido",
          variant: "destructive",
        })
        return
      }
      await waitDistribuidoraTypedSyncComplete({
        branch: "orders",
        baselineLastRun: null,
        signal: ac.signal,
      })
      await loadDispatchPrep(ac.signal)
      toast({
        title: "Órdenes actualizadas",
        description:
          r.message ??
          "Sync en servidor completado. Resumen, observaciones y tabla recargados.",
      })
    } catch (e: unknown) {
      if (e instanceof Error && e.name === "AbortError") return
      toast({
        title: "Error al sincronizar",
        description: e instanceof Error ? e.message : "Error desconocido",
        variant: "destructive",
      })
    } finally {
      setLoadingSync(false)
    }
  }, [loadDispatchPrep])

  const safePlanningRows = useMemo(
    () => (Array.isArray(planningRows) ? planningRows : []),
    [planningRows],
  )

  const tagStats = useMemo(
    () => aggregateObservationTags(observationTexts ?? []),
    [observationTexts],
  )

  const onChipClick = useCallback((tag: string) => {
    const token = weekdayTokenFromTagLabel(tag)
    if (!token) return
    setActiveDayFilter((prev) => (prev === token ? null : token))
  }, [])

  const resumenRows = useMemo(
    () => aggregateDispatchPrepByMunicipality(safePlanningRows, estadoResumen),
    [safePlanningRows, estadoResumen],
  )

  const kpis = useMemo(
    () => computeResumenKpis(resumenRows ?? []),
    [resumenRows],
  )

  const operationalStats = useMemo(
    () => computePreDespachoStats(safePlanningRows),
    [safePlanningRows],
  )

  const statusFilterCounts = useMemo((): Record<
    PurchaseInvoiceStatusFilter,
    number
  > => {
    const counts: Record<PurchaseInvoiceStatusFilter, number> = {
      all: safePlanningRows.length,
      pending: 0,
      probable: 0,
      confirmed: 0,
    }
    for (const r of safePlanningRows) {
      if (
        matchesPurchaseStatusFilter(r as PurchaseInvoiceStatusFields, "pending")
      ) {
        counts.pending += 1
      } else if (
        matchesPurchaseStatusFilter(r as PurchaseInvoiceStatusFields, "probable")
      ) {
        counts.probable += 1
      } else if (
        matchesPurchaseStatusFilter(
          r as PurchaseInvoiceStatusFields,
          "confirmed",
        )
      ) {
        counts.confirmed += 1
      }
    }
    return counts
  }, [safePlanningRows])

  const filteredPlanningRows = useMemo(
    () => filterPlanningRowsByStatus(safePlanningRows, estadoResumen),
    [safePlanningRows, estadoResumen],
  )

  const validTruckIdSet = useMemo(
    () => new Set(trucks.map((t) => t.id)),
    [trucks],
  )

  const sortedPlanningRows = useMemo(() => {
    const list = [...filteredPlanningRows]
    if (planningSortBy === "oc") {
      list.sort((a, b) => {
        const na = Number(a.oc)
        const nb = Number(b.oc)
        const fa = Number.isFinite(na) ? na : -Infinity
        const fb = Number.isFinite(nb) ? nb : -Infinity
        if (fb !== fa) return fb - fa
        return b.document_id - a.document_id
      })
    } else if (planningSortBy === "municipality") {
      list.sort((a, b) =>
        normMunicipality(a.municipality).localeCompare(
          normMunicipality(b.municipality),
          "es",
        ),
      )
    } else {
      list.sort((a, b) => {
        const ma = Number(a.total_amount)
        const mb = Number(b.total_amount)
        const fa = Number.isFinite(ma) ? ma : -Infinity
        const fb = Number.isFinite(mb) ? mb : -Infinity
        return fb - fa
      })
    }
    return list
  }, [filteredPlanningRows, planningSortBy])

  const clusterByDoc = useMemo(
    () => buildClusterLabelByDocumentId(sortedPlanningRows),
    [sortedPlanningRows],
  )

  const routeStubsPreview = useMemo(
    () =>
      buildRouteStubsFromAssignments({
        truckIdByDoc,
        rows: sortedPlanningRows,
        validTruckIds: validTruckIdSet,
      }),
    [truckIdByDoc, sortedPlanningRows, validTruckIdSet],
  )

  const logisticsKpis = useMemo(() => {
    const truckIds = new Set<number>()
    let count = 0
    let amount = 0
    const comunas = new Set<string>()
    for (const r of sortedPlanningRows) {
      if (!rowHasGeo(r)) continue
      const tid = truckIdByDoc[r.document_id]
      if (!isValidTruckId(tid, trucks)) continue
      count += 1
      amount += Number(r.total_amount ?? 0)
      comunas.add(normMunicipality(r.municipality))
      truckIds.add(tid)
    }
    return {
      trucksUsed: truckIds.size,
      orders: count,
      amount,
      comunas: comunas.size,
    }
  }, [sortedPlanningRows, truckIdByDoc, trucks])

  const groupedBlocks = useMemo(() => {
    type Block = {
      key: string
      rows: DistribuidoraDispatchPrepPlanningRow[]
      total: number
    }
    if (!groupByMunicipality) {
      const total = sortedPlanningRows.reduce(
        (s, r) => s + Number(r.total_amount ?? 0),
        0,
      )
      const single: Block = { key: "_all", rows: sortedPlanningRows, total }
      return [single]
    }
    const map = new Map<string, DistribuidoraDispatchPrepPlanningRow[]>()
    for (const r of sortedPlanningRows) {
      const k = normMunicipality(r.municipality)
      if (!map.has(k)) map.set(k, [])
      map.get(k)!.push(r)
    }
    const keys = [...map.keys()].sort((a, b) => a.localeCompare(b, "es"))
    return keys.map((key) => {
      const gr = map.get(key)!
      const total = gr.reduce((s, r) => s + Number(r.total_amount ?? 0), 0)
      return { key, rows: gr, total }
    })
  }, [sortedPlanningRows, groupByMunicipality])

  const canPassToPlanificacion = useMemo(() => {
    if (trucks.length === 0) return false
    let anyInPlan = false
    for (const r of safePlanningRows) {
      const tid = truckIdByDoc[r.document_id]
      const assigned = tid != null
      const valid = isValidTruckId(tid, trucks)
      if (assigned && (!valid || !rowHasGeo(r))) return false
      if (valid && rowHasGeo(r)) anyInPlan = true
    }
    return anyInPlan
  }, [trucks, safePlanningRows, truckIdByDoc])

  const onPassToPlanificacion = useCallback(() => {
    setPlanificacionFeedback(null)
    if (trucks.length === 0) {
      setPlanificacionFeedback("⚠️ No hay camiones disponibles.")
      return
    }
    const byTruck: Record<number, number> = {}
    const stored: PlanificacionStoredOrder[] = []

    for (const r of safePlanningRows) {
      if (!rowHasGeo(r)) continue
      const tid = truckIdByDoc[r.document_id]
      const truck =
        tid != null ? trucks.find((t) => t.id === tid) : undefined
      if (!isValidTruckId(tid, trucks) || !truck) continue
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

    if (stored.length === 0) {
      setPlanificacionFeedback(
        "Faltan camiones o georreferencias para continuar",
      )
      return
    }

    writePlanificacionPayload({
      submittedAt: new Date().toISOString(),
      orders: stored,
    })
    router.push("/distribuidora/planificacion")
  }, [trucks, safePlanningRows, truckIdByDoc, router])

  const trucksOrderedForGroupMenu = useMemo(() => {
    const list = [...trucks]
    const pref = lastSuggestedGroupTruckId
    if (pref != null) {
      const ix = list.findIndex((t) => t.id === pref)
      if (ix > 0) {
        const [sel] = list.splice(ix, 1)
        list.unshift(sel)
      }
    }
    return list
  }, [trucks, lastSuggestedGroupTruckId])

  const assignTruckToGroupWithChoice = useCallback(
    (
      municipalityLabel: string,
      truckId: number,
      groupRows: DistribuidoraDispatchPrepPlanningRow[],
    ) => {
      const geoRows = groupRows.filter(rowHasGeo)
      const noGeoCount = groupRows.length - geoRows.length
      if (geoRows.length === 0) {
        toast({
          variant: "destructive",
          title: "Sin pedidos asignables",
          description:
            "Ningún pedido del grupo tiene coordenadas; no se asignó camión.",
        })
        return
      }
      setTruckIdByDoc((prev) => {
        const next = { ...prev }
        for (const r of geoRows) {
          next[r.document_id] = truckId
        }
        return next
      })
      try {
        sessionStorage.setItem(LAST_GROUP_TRUCK_STORAGE_KEY, String(truckId))
      } catch {
        /* ignore */
      }
      setLastSuggestedGroupTruckId(truckId)
      const truck = trucks.find((t) => t.id === truckId)
      const truckName = truck?.name ?? "Camión"
      toast({
        title: "Camión asignado al grupo",
        description: `Camión ${truckName} asignado a ${geoRows.length} pedido(s) en ${municipalityLabel}${
          noGeoCount > 0
            ? `. ${noGeoCount} sin coordenadas omitidos.`
            : ""
        }`,
      })
    },
    [trucks],
  )

  const confirmBulkTruckSuggest = useCallback(() => {
    const sug = bulkTruckSuggest
    if (!sug) return
    const { municipality, truckId } = sug
    setTruckIdByDoc((prev) => {
      const next = { ...prev }
      for (const x of safePlanningRows) {
        if (normMunicipality(x.municipality) !== municipality) continue
        if (!rowHasGeo(x)) continue
        const t = prev[x.document_id]
        if (t == null || !isValidTruckId(t, trucks)) next[x.document_id] = truckId
      }
      return next
    })
    setBulkTruckSuggest(null)
  }, [bulkTruckSuggest, safePlanningRows, trucks])

  const onPlanningTruckChange = useCallback(
    (row: DistribuidoraDispatchPrepPlanningRow, raw: string) => {
      const nextVal = raw === TRUCK_UNSET ? null : Number(raw)
      const coerced =
        nextVal != null && Number.isFinite(nextVal) ? nextVal : null
      setTruckIdByDoc((prev) => {
        const merged: Record<number, number | null> = {
          ...prev,
          [row.document_id]: coerced,
        }
        if (coerced != null && rowHasGeo(row)) {
          const muni = normMunicipality(row.municipality)
          let c = 0
          for (const x of safePlanningRows) {
            if (x.document_id === row.document_id) continue
            if (normMunicipality(x.municipality) !== muni) continue
            if (!rowHasGeo(x)) continue
            const t = merged[x.document_id]
            if (t == null || !isValidTruckId(t, trucks)) c += 1
          }
          if (c > 0) {
            queueMicrotask(() =>
              setBulkTruckSuggest({
                municipality: muni,
                truckId: coerced,
                count: c,
              }),
            )
          }
        }
        return merged
      })
    },
    [safePlanningRows, trucks],
  )

  const openDetail = useCallback((r: DistribuidoraDispatchPrepMunicipalityRow) => {
    setDetailRow(r)
    setDetailOpen(true)
  }, [])

  const onRefreshData = useCallback(() => {
    void loadDispatchPrep()
  }, [loadDispatchPrep])

  return (
    <div className="-m-6 relative flex w-full max-w-none flex-col gap-4 px-3 pb-12 md:px-4">
      {loading ? <PlanningRowsLoadingOverlay /> : null}
      <header className="flex flex-wrap items-start justify-between gap-4 border-b border-border/60 pb-4">
        <div className="space-y-1">
          <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
            Distribuidora · Operaciones
          </p>
          <h1 className="text-2xl font-semibold tracking-tight md:text-3xl">
            Pre‑despacho OC
          </h1>
          <p className="max-w-2xl text-sm text-muted-foreground">
            Revisión de órdenes, asignación de camiones y validación antes del despacho.
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="gap-2"
            disabled={loading}
            onClick={onRefreshData}
          >
            {loading ? (
              <Loader2 className="size-4 animate-spin" aria-hidden />
            ) : (
              <RefreshCw className="size-4" aria-hidden />
            )}
            Recargar vista
          </Button>
          <Button
            type="button"
            size="sm"
            className="gap-2"
            disabled={loading || loadingSync}
            onClick={() => void onSyncOrdersFromBsale()}
          >
            {loadingSync ? (
              <Loader2 className="size-4 animate-spin" aria-hidden />
            ) : (
              <RefreshCw className="size-4" aria-hidden />
            )}
            Sync Bsale
          </Button>
        </div>
      </header>

      <PreDespachoKpiStrip
        stats={operationalStats ?? EMPTY_OPERATIONAL_STATS}
        loading={loading}
        estadoResumen={estadoResumen}
        onEstadoResumenChange={setEstadoResumen}
      />

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>No se pudieron actualizar los datos</AlertTitle>
          <AlertDescription>
            {error}
            {safePlanningRows.length > 0 ? (
              <span className="mt-2 block text-sm">
                Se mantienen las últimas {safePlanningRows.length} órdenes cargadas.
              </span>
            ) : null}
          </AlertDescription>
        </Alert>
      ) : null}

      {rangeWarning ? (
        <Alert>
          <AlertTitle>Rango amplio</AlertTitle>
          <AlertDescription>{rangeWarning}</AlertDescription>
        </Alert>
      ) : null}

      <section className="rounded-lg border border-border/70 bg-card p-4 shadow-sm md:p-5">
        {lastOrdersLoadAt ? (
          <p className="mb-3 text-xs text-muted-foreground">
            Datos cargados: {lastOrdersLoadAt}
            {" · "}
            Rango aplicado: {appliedDateFrom} — {appliedDateTo}
          </p>
        ) : null}
        {draftIsWide ? (
          <Alert className="mb-4 border-amber-500/40 bg-amber-500/5">
            <AlertTitle>Rango amplio</AlertTitle>
            <AlertDescription>{PRE_DESPACHO_WIDE_RANGE_HINT}</AlertDescription>
          </Alert>
        ) : null}
        <div className="flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
          <div className="flex flex-col gap-3 sm:flex-row sm:flex-wrap sm:items-end">
            <div className="grid gap-5 sm:grid-cols-2">
              <div className="space-y-2">
                <Label htmlFor="prep-from">Fecha desde</Label>
                <Input
                  id="prep-from"
                  type="date"
                  value={draftDateFrom}
                  onChange={(e) => setDraftDateFrom(e.target.value)}
                  disabled={loading && !loadingMore}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="prep-to">Fecha hasta</Label>
                <Input
                  id="prep-to"
                  type="date"
                  value={draftDateTo}
                  onChange={(e) => setDraftDateTo(e.target.value)}
                  disabled={loading && !loadingMore}
                />
              </div>
            </div>
            <Button
              type="button"
              size="sm"
              className="shrink-0"
              disabled={(loading && !loadingMore) || (!draftDatesDirty && !draftIsWide)}
              onClick={() => onRequestApplyRange()}
            >
              {draftIsWide ? "Cargar rango completo" : "Aplicar rango"}
            </Button>
            {draftDatesDirty ? (
              <p className="text-xs text-muted-foreground sm:basis-full">
                Cambió el rango en los campos; pulse el botón para cargar desde el servidor.
              </p>
            ) : null}
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
                  Sin enlace en <code className="text-xs">document_related</code> hacia
                  boleta o factura (tipos 1/6)
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

      <div className="flex flex-wrap items-end gap-4">
        <div className="space-y-2">
          <Label htmlFor="resumen-estado">Estado resumen</Label>
          <Select
            value={estadoResumen}
            onValueChange={(v) =>
              setEstadoResumen(v as PurchaseInvoiceStatusFilter)
            }
            disabled={loading}
          >
            <SelectTrigger id="resumen-estado" className="h-9 w-[180px]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {RESUMEN_ESTADO_OPTIONS.map((o) => (
                <SelectItem key={o.value} value={o.value}>
                  {o.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
      </div>

      <div className="grid gap-3 sm:grid-cols-3">
        <div className="rounded-lg border border-border/60 bg-muted/20 px-3 py-2.5 text-sm">
          <span className="text-xs text-muted-foreground">Comunas</span>
          <p className="flex items-center gap-2 font-semibold tabular-nums">
            <MapPin className="size-4 text-muted-foreground" aria-hidden />
            {loading ? "—" : (kpis?.comunas ?? 0)}
          </p>
        </div>
        <div className="rounded-lg border border-border/60 bg-muted/20 px-3 py-2.5 text-sm">
          <span className="text-xs text-muted-foreground">Pedidos agregados</span>
          <p className="flex items-center gap-2 font-semibold tabular-nums">
            <Package className="size-4 text-muted-foreground" aria-hidden />
            {loading ? "—" : (kpis?.pedidos ?? 0).toLocaleString("es-CL")}
          </p>
        </div>
        <div className="rounded-lg border border-border/60 bg-muted/20 px-3 py-2.5 text-sm">
          <span className="text-xs text-muted-foreground">Venta por comuna</span>
          <p className="flex items-center gap-2 text-sm font-semibold tabular-nums">
            <Truck className="size-4 shrink-0 text-muted-foreground" aria-hidden />
            {loading ? "—" : formatClp(kpis?.ventas ?? 0)}
          </p>
        </div>
      </div>

      <div className="grid w-full gap-3 xl:grid-cols-2">
        <section className="min-w-0 space-y-2">
          <h2 className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
            Por comuna
          </h2>
          <div className="overflow-x-auto rounded-md border border-border/60 bg-background/80">
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
                {resumenRows.length === 0 && !loading ? (
                  <tr>
                    <td
                      colSpan={4}
                      className="px-4 py-10 text-center text-muted-foreground"
                    >
                      Sin datos en el rango seleccionado.
                    </td>
                  </tr>
                ) : (
                  resumenRows.map((r) => (
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

        <aside className="space-y-2 rounded-md border border-border/60 bg-muted/15 p-3">
          <h2 className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
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

      <TooltipProvider delayDuration={200}>
        <section
          className="w-full min-w-0 space-y-3 rounded-md border border-border/70 bg-card p-3 shadow-sm"
          data-route-stubs={routeStubsPreview.length}
        >
          <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
            <div>
              <h2 className="text-sm font-semibold tracking-tight text-foreground">
                Órdenes · pre‑despacho
              </h2>
              <p className="text-xs text-muted-foreground">
                Filtre por estado, asigne camión y pase a planificación ORS.
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
          {!canPassToPlanificacion && safePlanningRows.length > 0 ? (
            <p className="text-xs text-amber-700 dark:text-amber-500">
              Faltan camiones o georreferencias para continuar
            </p>
          ) : null}
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
          <PreDespachoStatusChips
            value={estadoResumen}
            onChange={setEstadoResumen}
            counts={statusFilterCounts ?? EMPTY_STATUS_COUNTS}
            disabled={loading}
          />

          <div className="flex flex-col gap-3 rounded-md border border-border/60 bg-muted/20 p-3 sm:flex-row sm:flex-wrap sm:items-end">
            <div className="space-y-2">
              <Label className="text-xs">Ordenar por</Label>
              <Select
                value={planningSortBy}
                onValueChange={(v) => setPlanningSortBy(v as PlanningSortKey)}
                disabled={loading}
              >
                <SelectTrigger className="h-9 w-[200px] text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="oc">Número OC (mayor primero)</SelectItem>
                  <SelectItem value="municipality">Municipality (A–Z)</SelectItem>
                  <SelectItem value="amount">Monto (mayor primero)</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="flex items-center gap-3">
              <Switch
                id="prep-group-muni"
                checked={groupByMunicipality}
                onCheckedChange={(v) => setGroupByMunicipality(v === true)}
                disabled={loading}
              />
              <Label htmlFor="prep-group-muni" className="text-sm font-medium">
                Agrupar por comuna
              </Label>
            </div>
          </div>
          <div className="grid gap-1.5 text-xs sm:grid-cols-2 lg:grid-cols-4">
            <div className="flex justify-between rounded-md border border-border/50 bg-background/80 px-3 py-2">
              <span className="text-muted-foreground">Camiones usados</span>
              <strong className="tabular-nums">{logisticsKpis.trucksUsed}</strong>
            </div>
            <div className="flex justify-between rounded-md border border-border/50 bg-background/80 px-3 py-2">
              <span className="text-muted-foreground">En plan</span>
              <strong className="tabular-nums">{logisticsKpis.orders}</strong>
            </div>
            <div className="flex justify-between rounded-md border border-border/50 bg-background/80 px-3 py-2">
              <span className="text-muted-foreground">Monto en plan</span>
              <strong className="tabular-nums">{formatClp(logisticsKpis.amount)}</strong>
            </div>
            <div className="flex justify-between rounded-md border border-border/50 bg-background/80 px-3 py-2">
              <span className="text-muted-foreground">Comunas activas</span>
              <strong className="tabular-nums">{logisticsKpis.comunas}</strong>
            </div>
          </div>

          <PreDespachoPlanningTable
            blocks={groupedBlocks}
            groupByMunicipality={groupByMunicipality}
            trucks={trucks}
            trucksOrderedForGroupMenu={trucksOrderedForGroupMenu}
            lastSuggestedGroupTruckId={lastSuggestedGroupTruckId}
            truckIdByDoc={truckIdByDoc}
            clusterByDoc={clusterByDoc}
            allRowsForThresholds={safePlanningRows}
            loading={loading}
            statusFilterActive={estadoResumen !== "all"}
            onGroupTruckPick={assignTruckToGroupWithChoice}
            onTruckChange={onPlanningTruckChange}
          />
          {planningHasMore ? (
            <div className="flex flex-col items-center gap-2 border-t border-border/60 pt-4">
              <p className="text-center text-xs text-muted-foreground">
                Hay más órdenes en este rango ({safePlanningRows.length} cargadas, máx.{" "}
                {PRE_DESPACHO_PAGE_LIMIT} por página).
              </p>
              <Button
                type="button"
                variant="outline"
                size="sm"
                disabled={loading || loadingMore}
                onClick={() => loadMorePlanning()}
              >
                {loadingMore ? (
                  <Loader2 className="mr-2 size-4 animate-spin" aria-hidden />
                ) : null}
                Cargar más órdenes
              </Button>
            </div>
          ) : null}
        </section>
      </TooltipProvider>

      <AlertDialog open={wideRangeConfirmOpen} onOpenChange={setWideRangeConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>¿Cargar rango amplio?</AlertDialogTitle>
            <AlertDialogDescription>
              {PRE_DESPACHO_WIDE_RANGE_HINT} El servidor devolverá hasta{" "}
              {PRE_DESPACHO_PAGE_LIMIT} órdenes por página; puede usar &quot;Cargar más&quot;
              para el resto.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancelar</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => {
                setWideRangeConfirmOpen(false)
                void applyDraftRange()
              }}
            >
              Sí, cargar rango
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog
        open={bulkTruckSuggest != null}
        onOpenChange={(o) => {
          if (!o) setBulkTruckSuggest(null)
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Asignar el mismo camión</AlertDialogTitle>
            <AlertDialogDescription>
              {bulkTruckSuggest
                ? `Hay ${bulkTruckSuggest.count} pedido(s) en ${bulkTruckSuggest.municipality} sin camión → ¿asignar este camión?`
                : null}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel onClick={() => setBulkTruckSuggest(null)}>
              No
            </AlertDialogCancel>
            <AlertDialogAction onClick={() => confirmBulkTruckSuggest()}>
              Sí
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

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
