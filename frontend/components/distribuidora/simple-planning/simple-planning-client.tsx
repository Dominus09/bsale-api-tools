"use client"

import Link from "next/link"
import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from "react"
import {
  AlertTriangle,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  FileSpreadsheet,
  Loader2,
  PanelLeftClose,
  PanelLeftOpen,
  Truck,
  Upload,
} from "lucide-react"

import {
  getDistribuidoraTrucks,
  listAvailableDispatchDrivers,
  postSimplePlanningAssignPreview,
  postSimplePlanningConfirm,
  postSimplePlanningPrepareConfirm,
  postSimplePlanningPreview,
  SimplePlanningApiError,
  type DispatchPlanDriverOption,
  type DistribuidoraTruck,
  type SimplePlanningAssignPreview,
  type SimplePlanningOrsSnapshot,
  type SimplePlanningPreviewItem,
} from "@/lib/api"
import { SimplePlanningTruckMap } from "@/components/distribuidora/simple-planning/SimplePlanningTruckMap"
import {
  hasManagementAccess,
  hasSimplePlanningAccess,
} from "@/lib/permissions"
import {
  applyBulkTruckAssignment,
  clearFolioSelection,
  filterByMunicipality,
  formatTruckSelectLabel,
  toggleFolioInSelection,
  truckIdsFromAssignments,
  uniqueMunicipalities,
  visibleAssignableFolios,
} from "@/lib/simple-planning-bulk-assign"
import {
  draftTruckStatsFromAssignments,
  parseFoliosFromText,
  parseFoliosFromXlsxFile,
  sumOcVentaClp,
  uniqueClientCount,
  type FolioParseResult,
} from "@/lib/simple-planning-folios"
import { formatClp } from "@/lib/ors-map-ui"
import { useAuthMe } from "@/lib/use-auth-me"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
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
import { Textarea } from "@/components/ui/textarea"
import { cn } from "@/lib/utils"

/** Alineado a backend ``oc_source_resolver`` (La Quillotana SPA / Bodega Central). */
const DISTRIBUIDORA_COMPANY_ID = 3
const DISTRIBUIDORA_OFFICE_ID = 1

/** Oficinas Bsale de company_id=3 (bsale_id). */
const OFFICE_OPTIONS = [
  { id: 1, label: "Bodega Central" },
  { id: 2, label: "Bodega 1" },
  { id: 3, label: "Supermercado La Quillotana" },
  { id: 4, label: "Quillotana I" },
  { id: 5, label: "Quillotana II" },
]

const STATUS_LABEL: Record<string, string> = {
  found_available: "Disponible",
  not_found: "No encontrada",
  cancelled_or_replaced: "Anulada/reemplazada",
  already_planned: "Ya planificada",
  pending_invoice: "Facturación pendiente",
  partial_invoice: "Facturación parcial",
  incomplete_weight_coverage: "Cobertura de peso incompleta",
  weight_unevaluable: "Capacidad total aún no verificable",
  insufficient_weight: "Cobertura de peso incompleta",
  duplicate_in_list: "Duplicada",
  invalid_folio: "Folio inválido",
}

type Step = "input" | "preview" | "assign" | "confirm" | "done"

type TruckConfirmState = {
  status: "idle" | "preparing" | "confirming" | "ok" | "error"
  message?: string
  planId?: number
  planningCode?: string
  sessionId?: string
  /** Nombre del chofer elegido en prepare-confirm (solo presentación). */
  driverName?: string | null
  /** Cant. peonetas informada al confirmar (solo presentación). */
  peonetasCount?: number | null
}

function tomorrowISO(): string {
  const d = new Date()
  d.setDate(d.getDate() + 1)
  const y = d.getFullYear()
  const m = String(d.getMonth() + 1).padStart(2, "0")
  const day = String(d.getDate()).padStart(2, "0")
  return `${y}-${m}-${day}`
}

function fmtKg(n: number | null | undefined): string {
  if (n == null || Number.isNaN(n)) return "—"
  return `${Number(n).toLocaleString("es-CL", { maximumFractionDigits: 1 })} kg`
}

function fmtPct(n: number | null | undefined): string {
  if (n == null || Number.isNaN(n)) return "—"
  return `${Number(n).toLocaleString("es-CL", { maximumFractionDigits: 1 })}%`
}

export function SimplePlanningClient() {
  const { meState, staff } = useAuthMe()
  const allowed = hasSimplePlanningAccess(staff)
  const canOpenOcAudit = hasManagementAccess(staff)

  const [step, setStep] = useState<Step>("input")
  const [officeId, setOfficeId] = useState(DISTRIBUIDORA_OFFICE_ID)
  const [pasteText, setPasteText] = useState("")
  const [parseInfo, setParseInfo] = useState<FolioParseResult | null>(null)
  const [fileName, setFileName] = useState<string | null>(null)
  const [parseBusy, setParseBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const [previewItems, setPreviewItems] = useState<SimplePlanningPreviewItem[]>([])
  const [previewBusy, setPreviewBusy] = useState(false)

  const [trucks, setTrucks] = useState<DistribuidoraTruck[]>([])
  const [assignmentByFolio, setAssignmentByFolio] = useState<Record<string, number>>({})
  const [selectedFolios, setSelectedFolios] = useState<Set<string>>(() => clearFolioSelection())
  const [municipalityFilter, setMunicipalityFilter] = useState<string>("__all__")
  const [bulkTruckId, setBulkTruckId] = useState<string>("")
  const [planningDate, setPlanningDate] = useState(tomorrowISO)
  const [dispatchName, setDispatchName] = useState("")
  const [assignPreview, setAssignPreview] = useState<SimplePlanningAssignPreview | null>(null)
  const [assignBusy, setAssignBusy] = useState(false)

  const [confirmOpen, setConfirmOpen] = useState(false)
  const [confirmTruckId, setConfirmTruckId] = useState<number | null>(null)
  const [confirmAck, setConfirmAck] = useState(false)
  const [ackIncompleteWeight, setAckIncompleteWeight] = useState(false)
  const [ackUnevaluableWeight, setAckUnevaluableWeight] = useState(false)
  const [confirmDispatchName, setConfirmDispatchName] = useState("")
  const [confirmDriverUserId, setConfirmDriverUserId] = useState("")
  const [confirmPeonetas, setConfirmPeonetas] = useState("0")
  const [availableDrivers, setAvailableDrivers] = useState<DispatchPlanDriverOption[]>([])
  const [driversNote, setDriversNote] = useState<string | null>(null)
  const [driverNearMisses, setDriverNearMisses] = useState<
    { user_id?: number; display_name?: string; missing_labels?: string[] }[]
  >([])
  const [truckConfirm, setTruckConfirm] = useState<Record<number, TruckConfirmState>>({})
  const [orsByTruck, setOrsByTruck] = useState<Record<number, SimplePlanningOrsSnapshot | null>>(
    {},
  )
  /** Solo presentación: contrae el panel de ingreso sin borrar OC. */
  const [inputPanelCollapsed, setInputPanelCollapsed] = useState(false)
  /** Filas con detalle de productos expandido (clave estable folio/input+idx). */
  const [expandedProductRows, setExpandedProductRows] = useState<Set<string>>(
    () => new Set(),
  )
  const confirmingRef = useRef(false)

  const invalidateAssignAndConfirm = useCallback(() => {
    setAssignPreview(null)
    setTruckConfirm({})
    setOrsByTruck({})
    setConfirmOpen(false)
    setConfirmAck(false)
    setAckIncompleteWeight(false)
    setAckUnevaluableWeight(false)
  }, [])

  // Planificación simplificada opera sobre OCs de La Quillotana SPA (company_id=3).
  // No usar getStoredCompanyId() aquí: el selector global suele quedar en Minimarkets (1)
  // y el filtro de choferes exige (company_id, office_id) exactos en app.user_offices.
  const companyId = DISTRIBUIDORA_COMPANY_ID

  useEffect(() => {
    let cancelled = false
    getDistribuidoraTrucks()
      .then((r) => {
        if (!cancelled) setTrucks(r.items || [])
      })
      .catch(() => {
        if (!cancelled) setTrucks([])
      })
    return () => {
      cancelled = true
    }
  }, [])

  const assignableItems = useMemo(
    () =>
      previewItems.filter(
        (i) =>
          i.folio != null &&
          !i.blocking &&
          i.status !== "not_found" &&
          i.status !== "invalid_folio" &&
          i.status !== "duplicate_in_list" &&
          i.status !== "cancelled_or_replaced",
      ),
    [previewItems],
  )

  const municipalities = useMemo(
    () => uniqueMunicipalities(previewItems),
    [previewItems],
  )

  const filteredPreviewItems = useMemo(
    () => filterByMunicipality(previewItems, municipalityFilter),
    [previewItems, municipalityFilter],
  )

  const visibleAssignable = useMemo(
    () => visibleAssignableFolios(filteredPreviewItems),
    [filteredPreviewItems],
  )

  const allVisibleSelected =
    visibleAssignable.length > 0 &&
    visibleAssignable.every((f) => selectedFolios.has(String(f)))

  const applyParse = useCallback((result: FolioParseResult, sourceLabel?: string) => {
    setParseInfo(result)
    setFileName(sourceLabel ?? null)
    setError(null)
    setPreviewItems([])
    setAssignPreview(null)
    setAssignmentByFolio({})
    setStep("input")
  }, [])

  const onPasteChange = (text: string) => {
    setPasteText(text)
    applyParse(parseFoliosFromText(text))
  }

  const onFile = async (file: File | null) => {
    if (!file) return
    setParseBusy(true)
    setError(null)
    try {
      const result = await parseFoliosFromXlsxFile(file)
      applyParse(result, file.name)
      if (result.folios.length) {
        setPasteText(result.folios.join("\n"))
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "No se pudo leer el Excel")
    } finally {
      setParseBusy(false)
    }
  }

  const runPreview = async () => {
    if (!parseInfo?.folios.length) {
      setError("Ingresa al menos un número de OC válido")
      return
    }
    setPreviewBusy(true)
    setError(null)
    try {
      const report = await postSimplePlanningPreview({
        company_id: companyId,
        office_id: officeId,
        folios: parseInfo.folios,
      })
      setPreviewItems(report.items || [])
      setStep("preview")
      setAssignPreview(null)
      setAssignmentByFolio({})
      setSelectedFolios(clearFolioSelection())
      setMunicipalityFilter("__all__")
      setTruckConfirm({})
      setExpandedProductRows(new Set())
      setInputPanelCollapsed(true)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al consultar OC")
    } finally {
      setPreviewBusy(false)
    }
  }

  const truckLabel = useCallback(
    (truckId: number, truck?: DistribuidoraTruck | null) => {
      const t = truck || trucks.find((x) => x.id === truckId)
      if (!t) return `Camión #${truckId}`
      return formatTruckSelectLabel(t, trucks)
    },
    [trucks],
  )

  const assignedTruckIds = useMemo(
    () => truckIdsFromAssignments(assignmentByFolio),
    [assignmentByFolio],
  )

  const previewStats = useMemo(() => {
    const recovered = previewItems.filter(
      (i) =>
        i.status !== "not_found" &&
        i.status !== "invalid_folio" &&
        i.status !== "duplicate_in_list",
    )
    let knownKg = 0
    let withWeight = 0
    let coverageSum = 0
    let coverageN = 0
    for (const item of recovered) {
      const kw = item.weight?.known_weight_kg ?? item.weight?.peso_total_kg
      if (kw != null && Number.isFinite(Number(kw))) {
        knownKg += Number(kw)
        withWeight += 1
      }
      const cov = item.weight?.coverage_percent
      if (cov != null && Number.isFinite(Number(cov))) {
        coverageSum += Number(cov)
        coverageN += 1
      }
    }
    const avgCoverage = coverageN > 0 ? coverageSum / coverageN : null
    const incomplete =
      recovered.length === 0
        ? "sin datos"
        : withWeight < recovered.length || (avgCoverage != null && avgCoverage < 99.5)
          ? "cobertura incompleta"
          : "cobertura completa"
    return {
      recovered: recovered.length,
      clients: uniqueClientCount(recovered),
      trucksAssigned: assignedTruckIds.length,
      ventaOcClp: sumOcVentaClp(recovered),
      knownKg,
      withWeight,
      totalForWeight: recovered.length,
      avgCoverage,
      coverageLabel: incomplete,
    }
  }, [previewItems, assignedTruckIds])

  const draftTruckStats = useMemo(
    () =>
      draftTruckStatsFromAssignments(
        previewItems,
        assignmentByFolio,
        uniqueClientCount,
      ),
    [previewItems, assignmentByFolio],
  )

  const setOcTruck = (folio: number, truckId: number | null) => {
    setAssignmentByFolio((prev) => {
      const next = { ...prev }
      const key = String(folio)
      if (truckId == null) delete next[key]
      else next[key] = truckId
      return next
    })
    // Invalida assign-preview, ORS y sesiones prepare de todos los camiones afectados.
    invalidateAssignAndConfirm()
  }

  const applyBulkAssign = () => {
    if (!selectedFolios.size) {
      setError("Selecciona al menos una OC")
      return
    }
    if (!bulkTruckId) {
      setError("Elige un camión para la asignación masiva")
      return
    }
    const truckId = Number(bulkTruckId)
    if (!Number.isFinite(truckId) || !trucks.some((t) => t.id === truckId)) {
      setError("Camión no válido del catálogo")
      return
    }
    const assignableSet = new Set(visibleAssignable.map(String))
    const blockedSelected = [...selectedFolios].filter((f) => !assignableSet.has(f))
    const validSelected = [...selectedFolios]
      .map(Number)
      .filter((f) => Number.isFinite(f) && assignableSet.has(String(f)))
    if (!validSelected.length) {
      setError("Las OC seleccionadas no son asignables (bloqueadas o filtradas)")
      return
    }
    setAssignmentByFolio((prev) =>
      applyBulkTruckAssignment(prev, validSelected, truckId),
    )
    invalidateAssignAndConfirm()
    setError(
      blockedSelected.length
        ? `Asignadas ${validSelected.length} OC. ${blockedSelected.length} omitidas (no asignables).`
        : null,
    )
  }

  /** Prefiere huellas del assign-preview (más frescas) sobre el preview inicial. */
  const fingerprintByFolio = useMemo(() => {
    const map = new Map<number, string>()
    for (const item of previewItems) {
      const folio = Number(item.folio)
      if (Number.isFinite(folio) && item.identity_fingerprint) {
        map.set(folio, item.identity_fingerprint)
      }
    }
    for (const a of assignPreview?.assignments || []) {
      const folio = Number(a.folio)
      if (Number.isFinite(folio) && a.identity_fingerprint) {
        map.set(folio, a.identity_fingerprint)
      }
    }
    for (const tr of assignPreview?.trucks || []) {
      for (const o of tr.orders || []) {
        const folio = Number(o.folio)
        if (Number.isFinite(folio) && o.identity_fingerprint) {
          map.set(folio, o.identity_fingerprint)
        }
      }
    }
    return map
  }, [previewItems, assignPreview])

  const buildAssignments = () => {
    const rows: {
      folio: number
      truck_id: number
      expected_fingerprint?: string | null
    }[] = []
    const seen = new Set<number>()
    for (const item of assignableItems) {
      const folio = Number(item.folio)
      if (!Number.isFinite(folio) || seen.has(folio)) continue
      const truckId = assignmentByFolio[String(folio)]
      if (!truckId) continue
      seen.add(folio)
      rows.push({
        folio,
        truck_id: truckId,
        expected_fingerprint:
          fingerprintByFolio.get(folio) ?? item.identity_fingerprint ?? null,
      })
    }
    return rows
  }

  const invalidateConfirmSession = (message: string) => {
    setAssignPreview(null)
    setConfirmOpen(false)
    setConfirmAck(false)
    setConfirmTruckId(null)
    setTruckConfirm({})
    setError(message)
    setStep("preview")
  }

  const runAssignPreview = async () => {
    const assignments = buildAssignments()
    if (!assignments.length) {
      setError("Asigna al menos una OC a un camión")
      return
    }
    setAssignBusy(true)
    setError(null)
    try {
      const report = await postSimplePlanningAssignPreview({
        company_id: companyId,
        office_id: officeId,
        planning_date: planningDate || null,
        dispatch_name: dispatchName.trim() || null,
        assignments,
      })
      setAssignPreview(report)
      setOrsByTruck({})
      setTruckConfirm({})
      setStep("assign")
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error en assign-preview")
    } finally {
      setAssignBusy(false)
    }
  }

  const openConfirm = (truckId: number) => {
    setConfirmTruckId(truckId)
    setConfirmAck(false)
    setAckIncompleteWeight(false)
    setAckUnevaluableWeight(false)
    setConfirmDispatchName(dispatchName.trim())
    setConfirmDriverUserId("")
    setConfirmPeonetas("0")
    setConfirmOpen(true)
    void listAvailableDispatchDrivers({
      company_id: companyId,
      office_id: officeId,
      diagnostics: true,
    })
      .then((r) => {
        setAvailableDrivers(r.items)
        setDriversNote(
          r.diagnostics?.note_es ||
            r.diagnostics?.requirements_summary_es ||
            null,
        )
        setDriverNearMisses(r.diagnostics?.ineligible_near_misses || [])
      })
      .catch(() => {
        setAvailableDrivers([])
        setDriverNearMisses([])
        setDriversNote("No se pudo cargar la lista de choferes. Reintentar.")
      })
  }

  const runConfirmTruck = async () => {
    if (!confirmTruckId || !confirmAck || confirmingRef.current) return
    const driverUid = Number(confirmDriverUserId)
    if (!Number.isFinite(driverUid) || driverUid <= 0) {
      setError("Seleccione un chofer responsable antes de confirmar")
      return
    }
    const peonetasRaw = confirmPeonetas.trim()
    if (!/^\d+$/.test(peonetasRaw)) {
      setError("Cant. de peonetas debe ser un entero no negativo (0, 1, 2…)")
      return
    }
    const peonetasCount = Number(peonetasRaw)
    if (!Number.isInteger(peonetasCount) || peonetasCount < 0 || peonetasCount > 10) {
      setError("Cant. de peonetas debe ser un entero entre 0 y 10")
      return
    }
    confirmingRef.current = true
    const truckId = confirmTruckId
    setTruckConfirm((prev) => ({
      ...prev,
      [truckId]: { status: "preparing", message: "Preparando sesión…" },
    }))
    setError(null)
    try {
      const all = buildAssignments().filter((a) => a.truck_id === truckId)
      if (!all.length) throw new Error("Sin OC asignadas a este camión")
      const idempotencyKey =
        typeof crypto !== "undefined" && crypto.randomUUID
          ? crypto.randomUUID()
          : `sp-${truckId}-${Date.now()}`
      const driverOpt = availableDrivers.find((d) => d.user_id === driverUid)
      const prepared = await postSimplePlanningPrepareConfirm({
        company_id: companyId,
        office_id: officeId,
        planning_date: planningDate || null,
        truck_id: truckId,
        dispatch_name: confirmDispatchName.trim() || dispatchName.trim() || null,
        driver_name: driverOpt?.display_name || null,
        driver_user_id: driverUid,
        assistant_count: peonetasCount,
        idempotency_key: idempotencyKey,
        acknowledge_incomplete_weight: ackIncompleteWeight,
        acknowledge_unevaluable_weight: ackUnevaluableWeight,
        ors_snapshot: orsByTruck[truckId] || null,
        assignments: all,
      })
      const driverName = driverOpt?.display_name || null
      if (prepared.status === "already_confirmed" && prepared.confirmed_plan_id) {
        setTruckConfirm((prev) => ({
          ...prev,
          [truckId]: {
            status: "ok",
            planId: prepared.confirmed_plan_id,
            message: "Ya confirmado (idempotente)",
            sessionId: prepared.session?.session_id,
            driverName,
            peonetasCount,
          },
        }))
        setConfirmOpen(false)
        setStep("done")
        return
      }
      const sessionId = prepared.session?.session_id
      if (!sessionId) throw new Error("Sesión de confirmación no recibida")
      setTruckConfirm((prev) => ({
        ...prev,
        [truckId]: {
          status: "confirming",
          message: "Confirmando despacho…",
          sessionId,
          driverName,
          peonetasCount,
        },
      }))
      const confirmed = await postSimplePlanningConfirm({ session_id: sessionId })
      const planId =
        confirmed.plan_id ??
        confirmed.result?.plan?.id ??
        undefined
      const code = confirmed.result?.plan?.planning_code
      setTruckConfirm((prev) => ({
        ...prev,
        [truckId]: {
          status: "ok",
          planId,
          planningCode: code,
          message: confirmed.idempotent
            ? "Confirmado (reintento seguro)"
            : "Despacho confirmado",
          sessionId,
          driverName,
          peonetasCount,
        },
      }))
      setConfirmOpen(false)
      setStep("done")
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Error al confirmar"
      const isDrift =
        e instanceof SimplePlanningApiError && e.code === "fingerprint_drift"
      const needsWeightAck =
        e instanceof SimplePlanningApiError &&
        (e.code === "weight_incomplete_ack_required" ||
          e.code === "weight_unevaluable_ack_required")
      const sessionObsolete =
        e instanceof SimplePlanningApiError && Boolean(e.sessionObsolete)
      setTruckConfirm((prev) => ({
        ...prev,
        [truckId]: { status: "error", message: msg },
      }))
      if (isDrift || sessionObsolete) {
        invalidateConfirmSession(
          msg +
            " La sesión de confirmación quedó obsoleta: vuelve a previsualizar la asignación.",
        )
      } else if (needsWeightAck) {
        setError(msg)
      } else {
        setError(msg)
      }
    } finally {
      confirmingRef.current = false
    }
  }

  if (meState === "loading") {
    return (
      <div className="flex items-center gap-2 p-6 text-sm text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" /> Cargando permisos…
      </div>
    )
  }

  if (!allowed) {
    return (
      <Alert variant="destructive" className="m-6 max-w-xl">
        <AlertTitle>Sin acceso</AlertTitle>
        <AlertDescription>
          Se requiere rol gerencia/operativo/adm o la capability{" "}
          <code>simple_planning</code>. El backend valida cada acción.
        </AlertDescription>
      </Alert>
    )
  }

  const confirmTruckBlock = assignPreview?.trucks?.find((t) => t.truck_id === confirmTruckId)
  const confirmState = confirmTruckId != null ? truckConfirm[confirmTruckId] : undefined
  const confirmBusy =
    confirmState?.status === "preparing" || confirmState?.status === "confirming"
  const hasPreview = previewItems.length > 0

  const toggleProductRow = (rowKey: string) => {
    setExpandedProductRows((prev) => {
      const next = new Set(prev)
      if (next.has(rowKey)) next.delete(rowKey)
      else next.add(rowKey)
      return next
    })
  }

  return (
    <div className="mx-auto flex w-full max-w-none flex-col gap-4 px-2 py-4 sm:px-3 md:gap-5 md:px-4 md:py-5 xl:px-5">
      <header className="space-y-2">
        <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
          <div className="min-w-0 space-y-1">
            <h1 className="text-xl font-semibold tracking-tight md:text-2xl">
              Planificación simplificada
            </h1>
            <p className="max-w-4xl text-sm text-muted-foreground">
              Ingreso de OC, vista previa, asignación de camiones y confirmación.
            </p>
          </div>
          <div className="flex flex-wrap gap-1.5 text-xs text-muted-foreground">
            {(
              [
                ["input", "1. OC"],
                ["preview", "2. Preview"],
                ["assign", "3. Camiones"],
                ["confirm", "4. Confirmar"],
              ] as const
            ).map(([key, label]) => {
              const active =
                key === "confirm"
                  ? step === "done" || step === "confirm"
                  : step === key
              return (
                <span
                  key={key}
                  className={cn(
                    "rounded px-2 py-0.5",
                    active ? "bg-primary/10 text-primary" : "bg-muted",
                  )}
                >
                  {label}
                </span>
              )
            })}
          </div>
        </div>
        <ol className="flex flex-wrap items-center gap-x-1.5 gap-y-1 rounded-lg border bg-muted/20 px-3 py-2 text-[11px] text-muted-foreground md:text-xs">
          {(
            [
              { label: "Planificación", state: "current" as const },
              { label: "Organizar carga", state: "next" as const },
              { label: "Facturación", state: "later" as const },
              { label: "Picking definitivo", state: "later" as const },
            ]
          ).map((stage, i, arr) => (
            <li key={stage.label} className="inline-flex items-center gap-1.5">
              <span
                className={cn(
                  "rounded px-1.5 py-0.5 font-medium",
                  stage.state === "current" && "bg-primary/15 text-primary",
                  stage.state === "next" && "bg-sky-500/10 text-sky-900 dark:text-sky-200",
                  stage.state === "later" && "bg-muted text-muted-foreground",
                )}
              >
                {stage.label}
                {stage.state === "later" ? " (después)" : ""}
              </span>
              {i < arr.length - 1 ? <span aria-hidden>→</span> : null}
            </li>
          ))}
        </ol>
        <p className="text-[11px] text-muted-foreground">
          «Organización completa» en carga no genera picking definitivo facturado.
        </p>
        <section className="grid gap-3 rounded-lg border bg-card p-3 sm:grid-cols-3 md:p-3.5">
          <div className="space-y-1.5">
            <Label className="text-xs">Oficina</Label>
            <Select value={String(officeId)} onValueChange={(v) => setOfficeId(Number(v))}>
              <SelectTrigger className="h-9">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {OFFICE_OPTIONS.map((o) => (
                  <SelectItem key={o.id} value={String(o.id)}>
                    {o.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-1.5">
            <Label className="text-xs">Fecha de planificación</Label>
            <Input
              type="date"
              className="h-9"
              value={planningDate}
              onChange={(e) => setPlanningDate(e.target.value)}
            />
          </div>
          <div className="space-y-1.5">
            <Label className="text-xs">Nombre del despacho</Label>
            <Input
              className="h-9"
              value={dispatchName}
              onChange={(e) => setDispatchName(e.target.value)}
              placeholder="Ej. Ruta Melinka"
              maxLength={128}
            />
          </div>
        </section>
      </header>

      {error ? (
        <Alert variant="destructive">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Error</AlertTitle>
          <AlertDescription className="whitespace-pre-wrap">{error}</AlertDescription>
        </Alert>
      ) : null}

      <div className="grid gap-3 lg:grid-cols-[minmax(220px,260px)_minmax(0,1fr)] xl:gap-4">
        <aside className="min-w-0 rounded-lg border bg-card p-3 md:p-3.5 lg:sticky lg:top-4 lg:self-start">
          <div className="space-y-3">
            {hasPreview ? (
              <div className="flex items-center justify-between gap-2">
                <p className="text-sm font-medium">Ingreso de OC</p>
                <Button
                  type="button"
                  size="sm"
                  variant="outline"
                  className="h-8 gap-1.5"
                  onClick={() => setInputPanelCollapsed((v) => !v)}
                >
                  {inputPanelCollapsed ? (
                    <>
                      <PanelLeftOpen className="h-3.5 w-3.5" /> Editar OC
                    </>
                  ) : (
                    <>
                      <PanelLeftClose className="h-3.5 w-3.5" /> Contraer
                    </>
                  )}
                </Button>
              </div>
            ) : (
              <p className="text-sm font-medium">Ingreso de OC</p>
            )}

            {inputPanelCollapsed && hasPreview ? (
              <div className="rounded-md border border-dashed bg-muted/20 px-3 py-2 text-xs text-muted-foreground">
                <span className="font-medium text-foreground">
                  {parseInfo?.folios.length ?? 0}
                </span>{" "}
                folios únicos cargados. El contenido se conserva al contraer.
              </div>
            ) : (
              <>
                <div className="space-y-2">
                  <div className="flex items-center gap-2 text-sm font-medium">
                    <FileSpreadsheet className="h-4 w-4 shrink-0" /> Pegar números de OC
                  </div>
                  <p className="text-xs text-muted-foreground">Una OC por fila (como en Excel).</p>
                  <Textarea
                    value={pasteText}
                    onChange={(e) => onPasteChange(e.target.value)}
                    placeholder={"70625\n70626\n70630"}
                    className="min-h-[220px] max-h-[360px] resize-y overflow-y-auto font-mono text-sm md:min-h-[240px]"
                  />
                </div>
                <div className="space-y-2">
                  <div className="flex items-center gap-2 text-sm font-medium">
                    <Upload className="h-4 w-4 shrink-0" /> Importar .xlsx
                  </div>
                  <p className="text-xs text-muted-foreground">
                    Solo lectura local: no se sube el archivo. Columna OC/folio o primera
                    columna. Máx. 2 MB / 200 filas útiles.
                  </p>
                  <Input
                    type="file"
                    accept=".xlsx,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    disabled={parseBusy}
                    onChange={(e) => void onFile(e.target.files?.[0] ?? null)}
                  />
                  {fileName ? (
                    <p className="text-xs text-muted-foreground">Archivo: {fileName}</p>
                  ) : null}
                  {parseBusy ? (
                    <p className="flex items-center gap-2 text-xs text-muted-foreground">
                      <Loader2 className="h-3 w-3 animate-spin" /> Leyendo…
                    </p>
                  ) : null}
                </div>
                {parseInfo ? (
                  <div className="rounded-md border bg-muted/30 px-3 py-2 text-sm">
                    <span className="font-medium">{parseInfo.folios.length}</span> folios
                    únicos
                    {parseInfo.skippedEmpty ? (
                      <span className="text-muted-foreground">
                        {" "}
                        · {parseInfo.skippedEmpty} vacías omitidas
                      </span>
                    ) : null}
                    {parseInfo.issues.length ? (
                      <ul className="mt-2 max-h-24 list-disc overflow-auto pl-4 text-xs text-amber-800 dark:text-amber-200">
                        {parseInfo.issues.slice(0, 12).map((iss, i) => (
                          <li key={`${iss.kind}-${i}`}>{iss.message}</li>
                        ))}
                        {parseInfo.issues.length > 12 ? (
                          <li>… y {parseInfo.issues.length - 12} más</li>
                        ) : null}
                      </ul>
                    ) : null}
                    <div className="mt-3">
                      <Button
                        className="w-full"
                        onClick={() => void runPreview()}
                        disabled={previewBusy || !parseInfo.folios.length}
                      >
                        {previewBusy ? (
                          <>
                            <Loader2 className="mr-2 h-4 w-4 animate-spin" /> Consultando…
                          </>
                        ) : (
                          "Consultar OC"
                        )}
                      </Button>
                    </div>
                  </div>
                ) : null}
              </>
            )}
          </div>
        </aside>

        <div className="min-w-0 space-y-4">
          {!hasPreview ? (
            <section className="rounded-lg border border-dashed bg-muted/15 px-4 py-6 text-sm text-muted-foreground">
              <p className="font-medium text-foreground">Vista previa de OC</p>
              <p className="mt-1 max-w-xl text-xs leading-relaxed md:text-sm">
                Pega o importa los números de OC a la izquierda y pulsa Consultar OC. Aquí
                aparecerán indicadores, filtros, la tabla de asignación y el resumen por
                camión. Las comunas distintas (p. ej. Quellón junto a Melinka) se mantienen
                visibles y filtrables; no se excluyen automáticamente.
              </p>
            </section>
          ) : (
            <section className="space-y-3">
              <div className="flex flex-wrap gap-x-4 gap-y-1 rounded-lg border bg-muted/20 px-3 py-2 text-xs md:text-[13px]">
                <span>
                  <span className="text-muted-foreground">OC recuperadas </span>
                  <span className="font-semibold tabular-nums">{previewStats.recovered}</span>
                </span>
                <span>
                  <span className="text-muted-foreground">Clientes </span>
                  <span className="font-semibold tabular-nums">{previewStats.clients}</span>
                </span>
                <span>
                  <span className="text-muted-foreground">Venta OC </span>
                  <span className="font-semibold tabular-nums">
                    {formatClp(previewStats.ventaOcClp)}
                  </span>
                </span>
                <span>
                  <span className="text-muted-foreground">Camiones asignados </span>
                  <span className="font-semibold tabular-nums">
                    {previewStats.trucksAssigned}
                  </span>
                </span>
                <span>
                  <span className="text-muted-foreground">Peso conocido </span>
                  <span className="font-semibold tabular-nums">
                    {fmtKg(previewStats.knownKg)}
                  </span>
                  <span className="text-muted-foreground">
                    {" "}
                    ({previewStats.withWeight}/{previewStats.totalForWeight} OC)
                  </span>
                </span>
                <span>
                  <span className="text-muted-foreground">Cobertura </span>
                  <span className="font-semibold">{previewStats.coverageLabel}</span>
                  {previewStats.avgCoverage != null ? (
                    <span className="text-muted-foreground">
                      {" "}
                      · media {fmtPct(previewStats.avgCoverage)}
                    </span>
                  ) : null}
                </span>
              </div>

              <div className="flex flex-wrap items-end gap-3 rounded-lg border bg-card p-3">
                <div className="space-y-1">
                  <Label className="text-xs">Filtro comuna</Label>
                  <Select
                    value={municipalityFilter}
                    onValueChange={(v) => setMunicipalityFilter(v)}
                  >
                    <SelectTrigger className="h-8 w-[200px]">
                      <SelectValue placeholder="Todas" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="__all__">Todas las comunas</SelectItem>
                      {municipalities.map((m) => (
                        <SelectItem key={m} value={m}>
                          {m}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className="text-sm">
                  <span className="font-medium">{selectedFolios.size}</span>
                  <span className="text-muted-foreground"> OC seleccionadas</span>
                </div>
                <div className="space-y-1">
                  <Label className="text-xs">Camión masivo</Label>
                  <Select value={bulkTruckId || undefined} onValueChange={setBulkTruckId}>
                    <SelectTrigger className="h-8 w-[220px]">
                      <SelectValue placeholder="Elegir camión" />
                    </SelectTrigger>
                    <SelectContent>
                      {trucks.map((t) => (
                        <SelectItem key={t.id} value={String(t.id)}>
                          {formatTruckSelectLabel(t, trucks)}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <Button
                  size="sm"
                  onClick={applyBulkAssign}
                  disabled={!selectedFolios.size || !bulkTruckId || trucks.length === 0}
                >
                  Asignar seleccionadas
                </Button>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => setSelectedFolios(clearFolioSelection())}
                  disabled={!selectedFolios.size}
                >
                  Limpiar selección
                </Button>
              </div>

              <div className="overflow-x-auto rounded-lg border">
                <table className="w-full min-w-[1100px] text-left text-sm 2xl:min-w-0">
                  <thead className="border-b bg-muted/50 text-xs uppercase text-muted-foreground">
                    <tr>
                      <th className="w-10 px-2 py-2">
                        <Checkbox
                          checked={allVisibleSelected}
                          onCheckedChange={(v) => {
                            if (v === true) {
                              setSelectedFolios((prev) => {
                                const next = new Set(prev)
                                for (const f of visibleAssignable) next.add(String(f))
                                return next
                              })
                            } else {
                              setSelectedFolios((prev) => {
                                const next = new Set(prev)
                                for (const f of visibleAssignable) next.delete(String(f))
                                return next
                              })
                            }
                          }}
                          aria-label="Seleccionar todas las visibles"
                        />
                      </th>
                      <th className="w-[88px] px-2 py-2">OC</th>
                      <th className="min-w-[140px] px-2 py-2">Cliente</th>
                      <th className="w-[120px] px-2 py-2">Comuna</th>
                      <th className="w-[110px] px-2 py-2 text-right">Venta OC</th>
                      <th className="w-[120px] px-2 py-2">Productos</th>
                      <th className="w-[130px] px-2 py-2">Peso / cob.</th>
                      <th className="w-[120px] px-2 py-2">Facturación</th>
                      <th className="w-[170px] px-2 py-2">Camión</th>
                      <th className="min-w-[120px] px-2 py-2">Avisos</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredPreviewItems.map((item, idx) => {
                      const folio = item.folio
                      const rowKey = `${item.folio ?? item.input}-${idx}`
                      const canAssign =
                        folio != null &&
                        !item.blocking &&
                        item.status !== "not_found" &&
                        item.status !== "duplicate_in_list" &&
                        item.status !== "cancelled_or_replaced"
                      const productList = item.products || []
                      const productCount = productList.length
                      const expanded = expandedProductRows.has(rowKey)
                      const missing = item.weight?.products_without_weight || []
                      const municipality = item.client?.municipality || "—"
                      return (
                        <Fragment key={rowKey}>
                          <tr
                            className={cn(
                              "border-b align-middle",
                              item.blocking && "bg-destructive/5",
                            )}
                          >
                            <td className="px-2 py-1.5">
                              {canAssign ? (
                                <Checkbox
                                  checked={selectedFolios.has(String(folio))}
                                  onCheckedChange={(v) =>
                                    setSelectedFolios((prev) =>
                                      toggleFolioInSelection(
                                        prev,
                                        Number(folio),
                                        v === true,
                                      ),
                                    )
                                  }
                                  aria-label={`Seleccionar OC ${folio}`}
                                />
                              ) : (
                                <span className="text-xs text-muted-foreground">—</span>
                              )}
                            </td>
                            <td className="px-2 py-1.5 font-mono text-xs md:text-sm">
                              {folio ?? item.input ?? "—"}
                              {item.audit_oc?.suggested ? (
                                <div className="mt-0.5 text-[11px] font-sans normal-case">
                                  {canOpenOcAudit ? (
                                    <Link
                                      className="text-primary underline"
                                      href={`/distribuidora/auditoria-oc?folio=${encodeURIComponent(String(folio ?? item.input ?? ""))}`}
                                    >
                                      Auditoría OC
                                    </Link>
                                  ) : (
                                    <span className="text-muted-foreground">
                                      Pedir revisión en Auditoría OC
                                    </span>
                                  )}
                                </div>
                              ) : null}
                            </td>
                            <td className="px-2 py-1.5">
                              <div
                                className="truncate font-medium"
                                title={item.client?.client_name || undefined}
                              >
                                {item.client?.client_name || "—"}
                              </div>
                              <div className="truncate text-[11px] text-muted-foreground">
                                {STATUS_LABEL[item.status] || item.status}
                              </div>
                            </td>
                            <td className="px-2 py-1.5">
                              <div className="truncate font-medium" title={municipality}>
                                {municipality}
                              </div>
                              {item.client?.address ? (
                                <div
                                  className="truncate text-[11px] text-muted-foreground"
                                  title={item.client.address}
                                >
                                  {item.client.address}
                                </div>
                              ) : null}
                            </td>
                            <td className="px-2 py-1.5 text-right tabular-nums text-xs md:text-sm">
                              {item.total_amount != null &&
                              Number.isFinite(Number(item.total_amount))
                                ? formatClp(Number(item.total_amount))
                                : "—"}
                            </td>
                            <td className="px-2 py-1.5">
                              {productCount > 0 ? (
                                <button
                                  type="button"
                                  className="inline-flex items-center gap-1 rounded border bg-background px-1.5 py-0.5 text-xs hover:bg-muted"
                                  onClick={() => toggleProductRow(rowKey)}
                                  aria-expanded={expanded}
                                >
                                  {expanded ? (
                                    <ChevronDown className="h-3 w-3" />
                                  ) : (
                                    <ChevronRight className="h-3 w-3" />
                                  )}
                                  {productCount} producto{productCount === 1 ? "" : "s"}
                                </button>
                              ) : (
                                <span className="text-xs text-muted-foreground">—</span>
                              )}
                            </td>
                            <td className="px-2 py-1.5 text-xs">
                              <div className="tabular-nums">
                                {fmtKg(
                                  item.weight?.known_weight_kg ??
                                    item.weight?.peso_total_kg,
                                )}
                              </div>
                              <div className="text-muted-foreground">
                                {item.weight?.coverage_percent != null
                                  ? fmtPct(item.weight.coverage_percent)
                                  : item.weight?.label ||
                                    STATUS_LABEL[item.weight?.quality || ""] ||
                                    "—"}
                              </div>
                              {missing.length ? (
                                <div className="mt-0.5 text-[11px] text-amber-800 dark:text-amber-200">
                                  {missing.length} sin peso
                                </div>
                              ) : null}
                            </td>
                            <td className="px-2 py-1.5 text-xs">
                              <div>
                                {item.billing_label || item.billing_status || "—"}
                              </div>
                              <div className="text-muted-foreground">
                                docs{" "}
                                {item.invoice_count != null
                                  ? item.invoice_count
                                  : Array.isArray(item.invoices)
                                    ? item.invoices.length
                                    : "—"}
                              </div>
                            </td>
                            <td className="px-2 py-1.5">
                              {canAssign ? (
                                <Select
                                  value={
                                    assignmentByFolio[String(folio)] != null
                                      ? String(assignmentByFolio[String(folio)])
                                      : "__none__"
                                  }
                                  onValueChange={(v) =>
                                    setOcTruck(
                                      Number(folio),
                                      v === "__none__" ? null : Number(v),
                                    )
                                  }
                                >
                                  <SelectTrigger className="h-8 w-full min-w-[140px]">
                                    <SelectValue placeholder="—" />
                                  </SelectTrigger>
                                  <SelectContent>
                                    <SelectItem value="__none__">Sin asignar</SelectItem>
                                    {trucks.map((t) => (
                                      <SelectItem key={t.id} value={String(t.id)}>
                                        {formatTruckSelectLabel(t, trucks)}
                                      </SelectItem>
                                    ))}
                                  </SelectContent>
                                </Select>
                              ) : (
                                <span className="text-xs text-muted-foreground">—</span>
                              )}
                            </td>
                            <td className="px-2 py-1.5 text-xs text-amber-800 dark:text-amber-200">
                              {(item.warnings || [])
                                .map((w) => STATUS_LABEL[w] || w)
                                .join(", ") || (item.blocking ? "bloqueante" : "")}
                            </td>
                          </tr>
                          {expanded ? (
                            <tr className="border-b bg-muted/25">
                              <td colSpan={10} className="px-3 py-2">
                                <div className="max-h-48 overflow-y-auto rounded border bg-background p-2">
                                  <p className="mb-1 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
                                    Productos OC {folio ?? item.input ?? ""}
                                  </p>
                                  <ul className="columns-1 gap-x-6 text-xs sm:columns-2 xl:columns-3">
                                    {productList.map((p, pi) => (
                                      <li
                                        key={`${p.variant_id ?? p.code ?? pi}-${pi}`}
                                        className="mb-1 break-inside-avoid"
                                      >
                                        <span className="tabular-nums font-medium">
                                          {p.quantity ?? "?"}×
                                        </span>{" "}
                                        {p.description || p.code || "producto"}
                                      </li>
                                    ))}
                                  </ul>
                                  {missing.length ? (
                                    <p className="mt-2 text-[11px] text-amber-800 dark:text-amber-200">
                                      Sin peso:{" "}
                                      {missing
                                        .map(
                                          (p) =>
                                            p.description || p.code || p.variant_id,
                                        )
                                        .join(", ")}
                                    </p>
                                  ) : null}
                                </div>
                              </td>
                            </tr>
                          ) : null}
                        </Fragment>
                      )
                    })}
                  </tbody>
                </table>
              </div>

              <div className="flex flex-wrap items-center gap-3 rounded-lg border p-3">
                <div className="flex items-center gap-2 text-sm font-medium">
                  <Truck className="h-4 w-4" /> Asignación
                </div>
                <p className="text-xs text-muted-foreground">
                  {assignedTruckIds.length
                    ? `Camiones con OC: ${assignedTruckIds.map((tid) => truckLabel(tid)).join(" · ")}`
                    : "Aún no hay OC asignadas a un camión."}
                </p>
                <Button
                  className="sm:ml-auto"
                  onClick={() => void runAssignPreview()}
                  disabled={assignBusy || assignedTruckIds.length === 0}
                >
                  {assignBusy ? (
                    <>
                      <Loader2 className="mr-2 h-4 w-4 animate-spin" /> Calculando…
                    </>
                  ) : (
                    "Previsualizar asignación"
                  )}
                </Button>
              </div>

              {draftTruckStats.length && !assignPreview?.trucks?.length ? (
                <div className="space-y-2">
                  <h2 className="text-base font-medium">Totales por camión (borrador)</h2>
                  <p className="text-xs text-muted-foreground">
                    Se actualizan al cambiar la asignación. Pulse «Previsualizar asignación»
                    para validar capacidad, mapa, picking preliminar y confirmar.
                  </p>
                  <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-3">
                    {draftTruckStats.map((d) => (
                      <div key={d.truckId} className="rounded-lg border p-3 text-xs">
                        <div className="font-medium">{truckLabel(d.truckId)}</div>
                        <dl className="mt-2 grid grid-cols-2 gap-x-3 gap-y-1">
                          <div>
                            <dt className="text-muted-foreground">Venta OC</dt>
                            <dd className="font-medium tabular-nums">
                              {formatClp(d.ventaOcClp)}
                            </dd>
                          </div>
                          <div>
                            <dt className="text-muted-foreground">OC / clientes</dt>
                            <dd className="tabular-nums">
                              {d.ocCount} · {d.clients}
                            </dd>
                          </div>
                          <div>
                            <dt className="text-muted-foreground">Peso conocido</dt>
                            <dd className="tabular-nums">{fmtKg(d.knownKg)}</dd>
                          </div>
                          <div>
                            <dt className="text-muted-foreground">Cobertura media</dt>
                            <dd>{d.avgCoverage != null ? fmtPct(d.avgCoverage) : "—"}</dd>
                          </div>
                        </dl>
                        <p className="mt-2 text-[11px] text-muted-foreground">
                          Chofer: pendiente de seleccionar
                        </p>
                      </div>
                    ))}
                  </div>
                </div>
              ) : null}

              {assignPreview?.trucks?.length ? (
                <div className="space-y-3">
                  <h2 className="text-base font-medium">Resumen por camión</h2>
                  {(assignPreview.request_duplicate_folios || []).length ? (
                    <Alert variant="destructive">
                      <AlertTitle>OC duplicadas en la solicitud</AlertTitle>
                      <AlertDescription>
                        {(assignPreview.request_duplicate_folios || []).join(", ")}
                      </AlertDescription>
                    </Alert>
                  ) : null}
                  {(assignPreview.fingerprint_drifts || []).length ? (
                    <Alert variant="destructive">
                      <AlertTitle>OC modificadas después del preview</AlertTitle>
                      <AlertDescription>
                        {(
                          assignPreview.fingerprint_drifts as {
                            folio?: number
                            detail?: string
                          }[]
                        )
                          .map((d) =>
                            d.detail || (d.folio != null ? `OC ${d.folio}` : null),
                          )
                          .filter(Boolean)
                          .join(" · ")}{" "}
                        Actualiza el listado (preview) y vuelve a previsualizar la
                        asignación.
                      </AlertDescription>
                    </Alert>
                  ) : null}
                  <div className="flex flex-col gap-3">
                    {assignPreview.trucks
                      .filter((tr) => (tr.order_count ?? tr.orders?.length ?? 0) > 0)
                      .map((tr) => {
                        const cap = tr.capacity
                        const clients = uniqueClientCount(tr.orders || [])
                        const ventaOcClp = sumOcVentaClp(tr.orders || [])
                        const st = truckConfirm[tr.truck_id]
                        const driftBlocked =
                          (assignPreview.fingerprint_drifts || []).length > 0
                        const needsIncompleteAck =
                          (cap?.incomplete_order_count ?? 0) > 0
                        const needsUnevaluableAck =
                          (cap?.unevaluable_order_count ?? 0) > 0
                        const blocked =
                          Boolean(tr.blocking) ||
                          Boolean(cap?.blocking) ||
                          Boolean(tr.truck_missing) ||
                          driftBlocked ||
                          !cap?.capacity_known
                        const catalogTruck = trucks.find((t) => t.id === tr.truck_id)
                        const title = truckLabel(
                          tr.truck_id,
                          catalogTruck || (tr.truck as DistribuidoraTruck),
                        )
                        const driverLabel =
                          st?.driverName?.trim() ||
                          (st?.status === "ok"
                            ? "Chofer registrado en el plan"
                            : "pendiente de seleccionar")
                        const plate =
                          (
                            catalogTruck as
                              | (DistribuidoraTruck & { plate?: string })
                              | undefined
                          )?.plate || null
                        const warnText = [
                          ...(tr.warnings || []),
                          ...(cap?.warnings || []),
                        ]
                          .filter(
                            (w, i, arr) =>
                              w !== "orders_with_unavailable_weight" &&
                              arr.indexOf(w) === i,
                          )
                          .map((w) =>
                            w === "incomplete_weight_coverage"
                              ? "Cobertura de peso incompleta"
                              : w === "weight_capacity_unevaluable"
                                ? "Capacidad total aún no verificable"
                                : w === "truck_overloaded"
                                  ? "Camión sobrecargado (peso conocido)"
                                  : w === "truck_capacity_unknown"
                                    ? "Capacidad del camión desconocida"
                                    : w,
                          )
                          .join(" · ")
                        return (
                          <div
                            key={tr.truck_id}
                            className={cn(
                              "rounded-lg border p-3 md:p-4",
                              blocked && "border-destructive/40",
                            )}
                          >
                            <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
                              <div className="min-w-0 flex-1 space-y-2">
                                <div className="flex items-start gap-2">
                                  <div className="min-w-0">
                                    <div className="font-medium">
                                      {title}
                                      {plate ? (
                                        <span className="ml-2 text-xs font-normal text-muted-foreground">
                                          · {plate}
                                        </span>
                                      ) : null}
                                    </div>
                                    <div className="text-xs text-muted-foreground">
                                      {tr.order_count ?? 0} OC · {clients} clientes
                                    </div>
                                    <div className="mt-1 text-xs font-medium">
                                      Chofer:{" "}
                                      <span
                                        className={cn(
                                          !st?.driverName && st?.status !== "ok"
                                            ? "text-amber-800 dark:text-amber-200"
                                            : "text-foreground",
                                        )}
                                      >
                                        {st?.driverName
                                          ? st.driverName
                                          : driverLabel}
                                      </span>
                                    </div>
                                    {st?.status === "ok" && st.peonetasCount != null ? (
                                      <div className="text-xs font-medium">
                                        Peonetas: {st.peonetasCount}
                                      </div>
                                    ) : null}
                                  </div>
                                  {st?.status === "ok" ? (
                                    <CheckCircle2 className="h-5 w-5 shrink-0 text-emerald-600" />
                                  ) : null}
                                </div>
                                <dl className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs sm:grid-cols-3 lg:grid-cols-6">
                                  <div>
                                    <dt className="text-muted-foreground">Venta total OC</dt>
                                    <dd className="font-medium tabular-nums">
                                      {formatClp(ventaOcClp)}
                                    </dd>
                                  </div>
                                  <div>
                                    <dt className="text-muted-foreground">OC / clientes</dt>
                                    <dd className="font-medium tabular-nums">
                                      {tr.order_count ?? 0} · {clients}
                                    </dd>
                                  </div>
                                  <div>
                                    <dt className="text-muted-foreground">Peso conocido</dt>
                                    <dd className="font-medium tabular-nums">
                                      {fmtKg(cap?.known_weight_kg)}
                                    </dd>
                                  </div>
                                  <div>
                                    <dt className="text-muted-foreground">Capacidad máx.</dt>
                                    <dd className="font-medium tabular-nums">
                                      {fmtKg(cap?.truck_max_weight_kg)}
                                    </dd>
                                  </div>
                                  <div>
                                    <dt className="text-muted-foreground">
                                      Utilización (peso conocido)
                                    </dt>
                                    <dd className="font-medium tabular-nums">
                                      {fmtPct(cap?.utilization_pct)}
                                    </dd>
                                  </div>
                                  <div>
                                    <dt className="text-muted-foreground">Cobertura</dt>
                                    <dd className="font-medium">
                                      {cap?.weight_total_determined === false
                                        ? "incompleta / no total"
                                        : needsIncompleteAck
                                          ? "incompleta"
                                          : "completa"}
                                    </dd>
                                  </div>
                                </dl>
                                {needsIncompleteAck || needsUnevaluableAck ? (
                                  <p className="text-xs text-amber-800 dark:text-amber-200">
                                    {needsUnevaluableAck
                                      ? "Capacidad total aún no verificable — revisión manual al confirmar. "
                                      : ""}
                                    {needsIncompleteAck
                                      ? "Cobertura de peso incompleta — aceptación explícita al confirmar. "
                                      : ""}
                                    No autoriza salida a ruta. La utilización no es
                                    validación definitiva si la cobertura es incompleta.
                                  </p>
                                ) : null}
                                {warnText ? (
                                  <p className="text-xs text-amber-800 dark:text-amber-200">
                                    {warnText}
                                  </p>
                                ) : null}
                              </div>
                              <div className="flex w-full shrink-0 flex-col gap-2 xl:w-[280px]">
                                {st?.status === "ok" && st.planId ? (
                                  <div className="space-y-2 rounded-lg border border-emerald-500/40 bg-emerald-500/10 p-3">
                                    <p className="text-sm font-semibold text-emerald-800 dark:text-emerald-200">
                                      ¡Despacho confirmado!
                                    </p>
                                    <p className="text-xs text-muted-foreground">
                                      {st.planningCode || `PLAN-${st.planId}`}
                                      {st.message ? ` · ${st.message}` : ""}
                                      {st.driverName ? ` · Chofer: ${st.driverName}` : ""}
                                      {st.peonetasCount != null
                                        ? ` · Peonetas: ${st.peonetasCount}`
                                        : ""}
                                    </p>
                                    <Button
                                      asChild
                                      className="h-10 w-full bg-[var(--lq-brand)] font-semibold text-white hover:bg-[var(--lq-brand-strong)]"
                                    >
                                      <Link
                                        href={`/distribuidora/planificaciones/${st.planId}?tab=picking&panel=organizar`}
                                      >
                                        Organizar carga
                                      </Link>
                                    </Button>
                                    <Button asChild size="sm" variant="outline" className="w-full">
                                      <Link
                                        href={`/distribuidora/planificaciones/${st.planId}`}
                                      >
                                        Abrir despacho{" "}
                                        {st.planningCode ||
                                          `PLAN-${String(st.planId).padStart(5, "0")}`}
                                      </Link>
                                    </Button>
                                    <p className="text-[11px] text-muted-foreground">
                                      Abre Picking y carga → organización F2. Si 053 no está
                                      aplicada, la función queda deshabilitada con explicación.
                                    </p>
                                  </div>
                                ) : st?.status === "error" ? (
                                  <div className="space-y-2 rounded-lg border border-destructive/40 bg-destructive/5 p-3">
                                    <p className="text-sm font-medium text-destructive">
                                      Confirmación fallida
                                    </p>
                                    <p className="text-xs text-destructive">{st.message}</p>
                                    <Button
                                      size="sm"
                                      variant="outline"
                                      disabled={blocked}
                                      onClick={() => openConfirm(tr.truck_id)}
                                    >
                                      Reintentar confirmación
                                    </Button>
                                  </div>
                                ) : (
                                  <>
                                    {st?.status === "preparing" ||
                                    st?.status === "confirming" ? (
                                      <p className="text-xs text-muted-foreground">
                                        {st.message || "Procesando…"}
                                      </p>
                                    ) : (
                                      <p className="text-xs text-muted-foreground">
                                        Chofer: pendiente de seleccionar (en el siguiente paso)
                                      </p>
                                    )}
                                    <Button
                                      size="sm"
                                      disabled={
                                        blocked ||
                                        st?.status === "preparing" ||
                                        st?.status === "confirming"
                                      }
                                      onClick={() => openConfirm(tr.truck_id)}
                                    >
                                      Preparar confirmación y seleccionar chofer
                                    </Button>
                                  </>
                                )}
                              </div>
                            </div>
                            <div
                              className={cn(
                                "mt-3",
                                confirmOpen &&
                                  "pointer-events-none select-none opacity-60",
                              )}
                              aria-hidden={confirmOpen || undefined}
                            >
                              <SimplePlanningTruckMap
                                companyId={companyId}
                                officeId={officeId}
                                truckId={tr.truck_id}
                                truckName={title}
                                orders={tr.orders || []}
                                orsSnapshot={orsByTruck[tr.truck_id] || null}
                                onOrsSnapshot={(snap) => {
                                  if (confirmOpen) return
                                  setOrsByTruck((prev) => ({
                                    ...prev,
                                    [tr.truck_id]: snap,
                                  }))
                                }}
                              />
                            </div>
                            {tr.picking_preview?.products_consolidated?.length ? (
                              <div className="mt-3 rounded-lg border border-dashed bg-muted/20 p-3 text-xs">
                                <div className="flex flex-wrap items-baseline justify-between gap-2">
                                  <p className="font-semibold">
                                    Picking preliminar (desde OC, no definitivo)
                                  </p>
                                  <span className="text-[11px] text-muted-foreground">
                                    {tr.picking_preview.products_consolidated.length} SKU
                                    consolidados
                                  </span>
                                </div>
                                <p className="mt-1 text-[11px] text-muted-foreground">
                                  Vista de productos según líneas de OC. No es el picking
                                  final facturado; no altera documentos ni cantidades.
                                </p>
                                <details className="mt-2">
                                  <summary className="cursor-pointer font-medium text-foreground">
                                    Ver listado preliminar
                                  </summary>
                                  <ul className="mt-1 max-h-36 list-disc overflow-auto pl-4">
                                    {tr.picking_preview.products_consolidated
                                      .slice(0, 40)
                                      .map((p, i) => (
                                        <li key={i}>
                                          {p.quantity}× {p.description || p.code || "ítem"}
                                        </li>
                                      ))}
                                  </ul>
                                </details>
                              </div>
                            ) : null}
                          </div>
                        )
                      })}
                  </div>
                  <p className="text-xs text-muted-foreground">
                    Tras confirmar: use «Organizar carga» para agrupar OC en el plan. La
                    facturación y el picking definitivo siguen en el detalle del despacho. El
                    chofer se elige solo al preparar la confirmación.
                  </p>
                </div>
              ) : null}
            </section>
          )}
        </div>
      </div>

      <Dialog
        open={confirmOpen}
        onOpenChange={(open) => {
          if (!confirmBusy) setConfirmOpen(open)
        }}
      >
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>Confirmar despacho del camión</DialogTitle>
            <DialogDescription>
              Flujo prepare-confirm → confirm. El backend valida disponibilidad, fingerprints,
              capacidad y permisos. Evita doble clic; la sesión es idempotente.
            </DialogDescription>
          </DialogHeader>
          {confirmTruckBlock ? (
            <div className="space-y-2 text-sm">
              {(() => {
                const cap = confirmTruckBlock.capacity
                const needsIncomplete = (cap?.incomplete_order_count ?? 0) > 0
                const needsUnevaluable = (cap?.unevaluable_order_count ?? 0) > 0
                const dialogBlocked =
                  Boolean(confirmTruckBlock.blocking) ||
                  Boolean(cap?.blocking) ||
                  !cap?.capacity_known ||
                  (assignPreview?.fingerprint_drifts || []).length > 0
                const weightAckOk =
                  (!needsIncomplete || ackIncompleteWeight) &&
                  (!needsUnevaluable || ackUnevaluableWeight)
                const confirmTitle =
                  confirmTruckId != null
                    ? truckLabel(
                        confirmTruckId,
                        trucks.find((t) => t.id === confirmTruckId) ||
                          (confirmTruckBlock.truck as DistribuidoraTruck),
                      )
                    : "Camión"
                return (
                  <>
                    <div className="space-y-1.5">
                      <Label htmlFor="confirm-dispatch-name">Nombre del despacho</Label>
                      <Input
                        id="confirm-dispatch-name"
                        value={confirmDispatchName}
                        onChange={(e) => setConfirmDispatchName(e.target.value)}
                        placeholder="Ej. Ruta sur Hyundai"
                        disabled={confirmBusy}
                      />
                    </div>
                    <p>
                      <span className="font-medium">{confirmTitle}</span>
                      {(
                        trucks.find((t) => t.id === confirmTruckId) as
                          | (DistribuidoraTruck & { plate?: string })
                          | undefined
                      )?.plate
                        ? ` · patente ${(trucks.find((t) => t.id === confirmTruckId) as DistribuidoraTruck & { plate?: string }).plate}`
                        : ""}
                      {" · "}
                      {confirmTruckBlock.order_count ?? 0} OC · fecha {planningDate || "—"}
                    </p>
                    <p className="text-xs text-muted-foreground">
                      Utilización {fmtPct(cap?.utilization_pct)} · peso conocido{" "}
                      {fmtKg(cap?.known_weight_kg)}
                      {cap?.weight_total_determined === false
                        ? " (total real no determinado)"
                        : ""}
                    </p>
                    <div className="space-y-1.5">
                      <Label htmlFor="confirm-driver">Chofer responsable</Label>
                      <select
                        id="confirm-driver"
                        className="driver-chofer-native-select flex h-9 w-full rounded-md border border-input bg-transparent px-3 text-sm"
                        value={confirmDriverUserId}
                        onChange={(e) => setConfirmDriverUserId(e.target.value)}
                        disabled={confirmBusy}
                      >
                        <option value="">Seleccionar chofer…</option>
                        {availableDrivers.map((d) => (
                          <option key={d.user_id} value={d.user_id}>
                            {d.display_name}
                          </option>
                        ))}
                      </select>
                      {driversNote?.includes("Reintentar") ? (
                        <div className="flex flex-wrap items-center gap-2">
                          <p className="text-[11px] text-destructive">{driversNote}</p>
                          <Button
                            type="button"
                            variant="outline"
                            size="sm"
                            className="h-7 text-[11px]"
                            onClick={() => openConfirm(confirmTruckId!)}
                            disabled={confirmBusy || !confirmTruckId}
                          >
                            Reintentar
                          </Button>
                        </div>
                      ) : !availableDrivers.length ? (
                        <p className="text-[11px] text-amber-800 dark:text-amber-200">
                          {driversNote ||
                            "No hay choferes habilitados para esta empresa y oficina."}
                        </p>
                      ) : null}
                      {driverNearMisses.length ? (
                        <ul className="mt-1 list-inside list-disc text-[11px] text-muted-foreground">
                          {driverNearMisses.slice(0, 6).map((n) => (
                            <li key={n.user_id ?? n.display_name}>
                              {n.display_name || "Usuario"}
                              {n.user_id != null ? ` (#${n.user_id})` : ""}:{" "}
                              {(n.missing_labels || []).join("; ") ||
                                "requisito pendiente"}
                            </li>
                          ))}
                        </ul>
                      ) : null}
                      <p className="text-[10px] text-muted-foreground">
                        Filtro despacho: empresa {companyId} · oficina {officeId}{" "}
                        (Bodega Central = 1).
                      </p>
                    </div>
                    <div className="space-y-1.5">
                      <Label htmlFor="confirm-peonetas">Cant. de peonetas</Label>
                      <Input
                        id="confirm-peonetas"
                        type="number"
                        inputMode="numeric"
                        min={0}
                        max={10}
                        step={1}
                        className="h-9"
                        value={confirmPeonetas}
                        onChange={(e) => setConfirmPeonetas(e.target.value)}
                        disabled={confirmBusy}
                      />
                      <p className="text-[10px] text-muted-foreground">
                        Entero ≥ 0 (0 = sin peonetas). Máx. 10 por el esquema actual.
                      </p>
                    </div>
                    {dialogBlocked ? (
                      <Alert variant="destructive">
                        <AlertTriangle className="h-4 w-4" />
                        <AlertTitle>Confirmación bloqueada</AlertTitle>
                        <AlertDescription>
                          Hay bloqueos de capacidad o fingerprint. Cierra este diálogo,
                          corrige el preview y vuelve a previsualizar la asignación.
                        </AlertDescription>
                      </Alert>
                    ) : (
                      <div className="space-y-2">
                        {needsIncomplete ? (
                          <label className="flex items-start gap-2 text-sm">
                            <Checkbox
                              checked={ackIncompleteWeight}
                              onCheckedChange={(v) => setAckIncompleteWeight(v === true)}
                              disabled={confirmBusy}
                            />
                            <span>
                              Acepto planificar con cobertura de peso incompleta (productos
                              sin peso registrado). No autoriza salida a ruta.
                            </span>
                          </label>
                        ) : null}
                        {needsUnevaluable ? (
                          <label className="flex items-start gap-2 text-sm">
                            <Checkbox
                              checked={ackUnevaluableWeight}
                              onCheckedChange={(v) => setAckUnevaluableWeight(v === true)}
                              disabled={confirmBusy}
                            />
                            <span>
                              Revisión manual expresa: hay OC sin peso usable; la capacidad
                              total aún no es verificable. No autoriza salida a ruta.
                            </span>
                          </label>
                        ) : null}
                        <label className="flex items-start gap-2 text-sm">
                          <Checkbox
                            checked={confirmAck}
                            onCheckedChange={(v) => setConfirmAck(v === true)}
                            disabled={confirmBusy || !weightAckOk}
                          />
                          <span>
                            Confirmo expresamente este despacho. Entiendo que no debo
                            reintentar si la página se recarga a mitad de proceso sin revisar
                            el listado de planificaciones.
                          </span>
                        </label>
                      </div>
                    )}
                    {confirmState?.message && confirmState.status !== "ok" ? (
                      <p className="text-xs text-destructive">{confirmState.message}</p>
                    ) : null}
                  </>
                )
              })()}
            </div>
          ) : null}
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirmOpen(false)} disabled={confirmBusy}>
              Cancelar
            </Button>
            <Button
              onClick={() => void runConfirmTruck()}
              disabled={
                !confirmAck ||
                !confirmDriverUserId ||
                !/^\d+$/.test(confirmPeonetas.trim()) ||
                Number(confirmPeonetas) > 10 ||
                confirmBusy ||
                Boolean(confirmTruckBlock?.blocking) ||
                Boolean(confirmTruckBlock?.capacity?.blocking) ||
                !confirmTruckBlock?.capacity?.capacity_known ||
                (assignPreview?.fingerprint_drifts || []).length > 0 ||
                ((confirmTruckBlock?.capacity?.incomplete_order_count ?? 0) > 0 &&
                  !ackIncompleteWeight) ||
                ((confirmTruckBlock?.capacity?.unevaluable_order_count ?? 0) > 0 &&
                  !ackUnevaluableWeight)
              }
            >
              {confirmBusy ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" /> Procesando…
                </>
              ) : (
                "Confirmar despacho"
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
