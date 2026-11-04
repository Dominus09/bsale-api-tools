"use client"

import Link from "next/link"
import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import {
  AlertTriangle,
  CheckCircle2,
  FileSpreadsheet,
  Loader2,
  Truck,
  Upload,
} from "lucide-react"

import {
  getDistribuidoraTrucks,
  getStoredCompanyId,
  postSimplePlanningAssignPreview,
  postSimplePlanningConfirm,
  postSimplePlanningPrepareConfirm,
  postSimplePlanningPreview,
  SimplePlanningApiError,
  type DistribuidoraTruck,
  type SimplePlanningAssignPreview,
  type SimplePlanningPreviewItem,
} from "@/lib/api"
import {
  hasManagementAccess,
  hasSimplePlanningAccess,
} from "@/lib/permissions"
import {
  applyBulkTruckAssignment,
  clearFolioSelection,
  filterByMunicipality,
  toggleFolioInSelection,
  uniqueMunicipalities,
  visibleAssignableFolios,
} from "@/lib/simple-planning-bulk-assign"
import {
  parseFoliosFromText,
  parseFoliosFromXlsxFile,
  uniqueClientCount,
  type FolioParseResult,
} from "@/lib/simple-planning-folios"
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

const OFFICE_OPTIONS = [
  { id: 1, label: "Oficina 1" },
  { id: 2, label: "Oficina 2" },
  { id: 3, label: "Oficina 3" },
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
  const [officeId, setOfficeId] = useState(1)
  const [pasteText, setPasteText] = useState("")
  const [parseInfo, setParseInfo] = useState<FolioParseResult | null>(null)
  const [fileName, setFileName] = useState<string | null>(null)
  const [parseBusy, setParseBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const [previewItems, setPreviewItems] = useState<SimplePlanningPreviewItem[]>([])
  const [previewBusy, setPreviewBusy] = useState(false)

  const [trucks, setTrucks] = useState<DistribuidoraTruck[]>([])
  const [selectedTruckIds, setSelectedTruckIds] = useState<number[]>([])
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
  const [truckConfirm, setTruckConfirm] = useState<Record<number, TruckConfirmState>>({})
  const confirmingRef = useRef(false)

  const invalidateAssignAndConfirm = useCallback(() => {
    setAssignPreview(null)
    setTruckConfirm({})
    setConfirmOpen(false)
    setConfirmAck(false)
    setAckIncompleteWeight(false)
    setAckUnevaluableWeight(false)
  }, [])

  const companyId = getStoredCompanyId() ?? 1

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
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al consultar OC")
    } finally {
      setPreviewBusy(false)
    }
  }

  const toggleTruck = (id: number) => {
    setSelectedTruckIds((prev) =>
      prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id],
    )
  }

  const setOcTruck = (folio: number, truckId: number | null) => {
    setAssignmentByFolio((prev) => {
      const next = { ...prev }
      const key = String(folio)
      if (truckId == null) delete next[key]
      else next[key] = truckId
      return next
    })
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
    if (selectedTruckIds.length < 1) {
      setError("Selecciona camiones del catálogo antes de asignar")
      return
    }
    const truckId = Number(bulkTruckId)
    if (!selectedTruckIds.includes(truckId)) {
      setError("El camión elegido no está en la selección del catálogo")
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
    if (selectedTruckIds.length < 1) {
      setError("Selecciona al menos un camión del catálogo")
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
    setConfirmOpen(true)
  }

  const runConfirmTruck = async () => {
    if (!confirmTruckId || !confirmAck || confirmingRef.current) return
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
      const prepared = await postSimplePlanningPrepareConfirm({
        company_id: companyId,
        office_id: officeId,
        planning_date: planningDate || null,
        truck_id: truckId,
        dispatch_name: dispatchName.trim() || null,
        driver_name: null,
        idempotency_key: idempotencyKey,
        acknowledge_incomplete_weight: ackIncompleteWeight,
        acknowledge_unevaluable_weight: ackUnevaluableWeight,
        assignments: all,
      })
      if (prepared.status === "already_confirmed" && prepared.confirmed_plan_id) {
        setTruckConfirm((prev) => ({
          ...prev,
          [truckId]: {
            status: "ok",
            planId: prepared.confirmed_plan_id,
            message: "Ya confirmado (idempotente)",
            sessionId: prepared.session?.session_id,
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

  return (
    <div className="mx-auto flex w-full max-w-7xl flex-col gap-6 p-4 md:p-6">
      <header className="space-y-1">
        <h1 className="text-2xl font-semibold tracking-tight">Planificación simplificada</h1>
        <p className="text-sm text-muted-foreground">
          Ingreso por lista de OC o Excel → preview → asignación a camiones → confirmación
          por vehículo. Sin optimizador ORS. El chofer se asigna después desde el detalle del
          despacho.
        </p>
        <div className="flex flex-wrap gap-2 pt-2 text-xs text-muted-foreground">
          <span
            className={cn(
              "rounded px-2 py-0.5",
              step === "input" ? "bg-primary/10 text-primary" : "bg-muted",
            )}
          >
            1. OC
          </span>
          <span
            className={cn(
              "rounded px-2 py-0.5",
              step === "preview" ? "bg-primary/10 text-primary" : "bg-muted",
            )}
          >
            2. Preview
          </span>
          <span
            className={cn(
              "rounded px-2 py-0.5",
              step === "assign" ? "bg-primary/10 text-primary" : "bg-muted",
            )}
          >
            3. Camiones
          </span>
          <span
            className={cn(
              "rounded px-2 py-0.5",
              step === "done" || step === "confirm" ? "bg-primary/10 text-primary" : "bg-muted",
            )}
          >
            4. Confirmar
          </span>
        </div>
      </header>

      {error ? (
        <Alert variant="destructive">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Error</AlertTitle>
          <AlertDescription className="whitespace-pre-wrap">{error}</AlertDescription>
        </Alert>
      ) : null}

      <section className="grid gap-4 rounded-lg border bg-card p-4 md:grid-cols-3">
        <div className="space-y-2">
          <Label>Oficina</Label>
          <Select value={String(officeId)} onValueChange={(v) => setOfficeId(Number(v))}>
            <SelectTrigger>
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
        <div className="space-y-2">
          <Label>Fecha de planificación</Label>
          <Input
            type="date"
            value={planningDate}
            onChange={(e) => setPlanningDate(e.target.value)}
          />
        </div>
        <div className="space-y-2">
          <Label>Nombre despacho (opcional)</Label>
          <Input
            value={dispatchName}
            onChange={(e) => setDispatchName(e.target.value)}
            placeholder="Ej. Ruta norte Excel"
            maxLength={128}
          />
        </div>
      </section>

      <section className="grid gap-4 md:grid-cols-2">
        <div className="space-y-2 rounded-lg border p-4">
          <div className="flex items-center gap-2 text-sm font-medium">
            <FileSpreadsheet className="h-4 w-4" /> Pegar números de OC
          </div>
          <p className="text-xs text-muted-foreground">Una OC por fila (como en Excel).</p>
          <Textarea
            value={pasteText}
            onChange={(e) => onPasteChange(e.target.value)}
            rows={10}
            placeholder={"70625\n70626\n70630"}
            className="font-mono text-sm"
          />
        </div>
        <div className="space-y-2 rounded-lg border p-4">
          <div className="flex items-center gap-2 text-sm font-medium">
            <Upload className="h-4 w-4" /> Importar .xlsx
          </div>
          <p className="text-xs text-muted-foreground">
            Solo lectura local: no se sube el archivo. Columna OC/folio o primera columna.
            Máx. 2 MB / 200 filas útiles.
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
      </section>

      {parseInfo ? (
        <div className="rounded-lg border bg-muted/30 px-4 py-3 text-sm">
          <span className="font-medium">{parseInfo.folios.length}</span> OC únicas listas
          {parseInfo.skippedEmpty ? (
            <span className="text-muted-foreground">
              {" "}
              · {parseInfo.skippedEmpty} filas vacías omitidas
            </span>
          ) : null}
          {parseInfo.issues.length ? (
            <ul className="mt-2 max-h-28 list-disc overflow-auto pl-5 text-xs text-amber-800 dark:text-amber-200">
              {parseInfo.issues.slice(0, 12).map((iss, i) => (
                <li key={`${iss.kind}-${i}`}>{iss.message}</li>
              ))}
              {parseInfo.issues.length > 12 ? (
                <li>… y {parseInfo.issues.length - 12} más</li>
              ) : null}
            </ul>
          ) : null}
          <div className="mt-3 flex flex-wrap gap-2">
            <Button onClick={() => void runPreview()} disabled={previewBusy || !parseInfo.folios.length}>
              {previewBusy ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" /> Consultando…
                </>
              ) : (
                "Consultar OC (preview)"
              )}
            </Button>
          </div>
        </div>
      ) : null}

      {previewItems.length > 0 ? (
        <section className="space-y-3">
          <h2 className="text-lg font-medium">Resultado preview</h2>

          <div className="flex flex-wrap items-end gap-3 rounded-lg border bg-muted/20 p-3">
            <div className="space-y-1">
              <Label className="text-xs">Filtro comuna</Label>
              <Select
                value={municipalityFilter}
                onValueChange={(v) => setMunicipalityFilter(v)}
              >
                <SelectTrigger className="h-8 w-[180px]">
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
                <SelectTrigger className="h-8 w-[160px]">
                  <SelectValue placeholder="Elegir camión" />
                </SelectTrigger>
                <SelectContent>
                  {selectedTruckIds.map((tid) => {
                    const t = trucks.find((x) => x.id === tid)
                    return (
                      <SelectItem key={tid} value={String(tid)}>
                        {t?.name || `Camión ${tid}`}
                      </SelectItem>
                    )
                  })}
                </SelectContent>
              </Select>
            </div>
            <Button
              size="sm"
              onClick={applyBulkAssign}
              disabled={!selectedFolios.size || !bulkTruckId || !selectedTruckIds.length}
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
            <table className="w-full min-w-[1100px] text-left text-sm">
              <thead className="border-b bg-muted/50 text-xs uppercase text-muted-foreground">
                <tr>
                  <th className="px-2 py-2">
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
                  <th className="px-2 py-2">OC</th>
                  <th className="px-2 py-2">Cliente</th>
                  <th className="px-2 py-2">Comuna / dirección</th>
                  <th className="px-2 py-2">Productos</th>
                  <th className="px-2 py-2">Peso / cobertura</th>
                  <th className="px-2 py-2">Docs</th>
                  <th className="px-2 py-2">Facturación</th>
                  <th className="px-2 py-2">Planificación</th>
                  <th className="px-2 py-2">Camión</th>
                  <th className="px-2 py-2">Avisos</th>
                </tr>
              </thead>
              <tbody>
                {filteredPreviewItems.map((item, idx) => {
                  const folio = item.folio
                  const canAssign =
                    folio != null &&
                    !item.blocking &&
                    item.status !== "not_found" &&
                    item.status !== "duplicate_in_list" &&
                    item.status !== "cancelled_or_replaced"
                  const products = (item.products || [])
                    .slice(0, 4)
                    .map(
                      (p) =>
                        `${p.quantity ?? "?"}× ${p.description || p.code || "producto"}`,
                    )
                    .join("; ")
                  const more =
                    (item.products || []).length > 4
                      ? ` (+${(item.products || []).length - 4})`
                      : ""
                  const missing = item.weight?.products_without_weight || []
                  return (
                    <tr
                      key={`${item.folio ?? item.input}-${idx}`}
                      className={cn(
                        "border-b align-top",
                        item.blocking && "bg-destructive/5",
                      )}
                    >
                      <td className="px-2 py-2">
                        {canAssign ? (
                          <Checkbox
                            checked={selectedFolios.has(String(folio))}
                            onCheckedChange={(v) =>
                              setSelectedFolios((prev) =>
                                toggleFolioInSelection(prev, Number(folio), v === true),
                              )
                            }
                            aria-label={`Seleccionar OC ${folio}`}
                          />
                        ) : (
                          <span className="text-xs text-muted-foreground">—</span>
                        )}
                      </td>
                      <td className="px-2 py-2 font-mono">
                        {folio ?? item.input ?? "—"}
                        {item.audit_oc?.suggested ? (
                          <div className="text-xs">
                            {canOpenOcAudit ? (
                              <Link
                                className="text-primary underline"
                                href={`/distribuidora/auditoria-oc?folio=${encodeURIComponent(String(folio ?? item.input ?? ""))}`}
                              >
                                Auditoría OC
                              </Link>
                            ) : (
                              <span className="text-muted-foreground">
                                OC no hallada — pedir a gerencia/adm revisar en
                                Auditoría OC
                              </span>
                            )}
                          </div>
                        ) : null}
                      </td>
                      <td className="px-2 py-2">{item.client?.client_name || "—"}</td>
                      <td className="px-2 py-2">
                        <div>{item.client?.municipality || "—"}</div>
                        <div className="text-xs text-muted-foreground">
                          {item.client?.address || ""}
                        </div>
                      </td>
                      <td className="max-w-[220px] px-2 py-2 text-xs">
                        {products || "—"}
                        {more}
                      </td>
                      <td className="px-2 py-2 text-xs">
                        <div>
                          Peso conocido:{" "}
                          {fmtKg(item.weight?.known_weight_kg ?? item.weight?.peso_total_kg)}
                        </div>
                        <div className="text-muted-foreground">
                          {item.weight?.label ||
                            STATUS_LABEL[item.weight?.quality || ""] ||
                            item.weight?.quality ||
                            "—"}
                          {item.weight?.coverage_percent != null
                            ? ` · ${fmtPct(item.weight.coverage_percent)}`
                            : ""}
                        </div>
                        {missing.length ? (
                          <div className="mt-1 text-amber-800 dark:text-amber-200">
                            Productos sin peso registrado:{" "}
                            {missing
                              .slice(0, 3)
                              .map((p) => p.description || p.code || p.variant_id)
                              .join(", ")}
                            {missing.length > 3 ? ` (+${missing.length - 3})` : ""}
                          </div>
                        ) : null}
                      </td>
                      <td className="px-2 py-2 text-xs">
                        {item.invoice_count != null
                          ? item.invoice_count
                          : Array.isArray(item.invoices)
                            ? item.invoices.length
                            : "—"}
                      </td>
                      <td className="px-2 py-2 text-xs">
                        {item.billing_label || item.billing_status || "—"}
                      </td>
                      <td className="px-2 py-2 text-xs">
                        {STATUS_LABEL[item.status] || item.status}
                      </td>
                      <td className="px-2 py-2">
                        {canAssign ? (
                          <Select
                            value={
                              assignmentByFolio[String(folio)] != null
                                ? String(assignmentByFolio[String(folio)])
                                : "__none__"
                            }
                            onValueChange={(v) =>
                              setOcTruck(Number(folio), v === "__none__" ? null : Number(v))
                            }
                          >
                            <SelectTrigger className="h-8 w-[140px]">
                              <SelectValue placeholder="—" />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="__none__">Sin asignar</SelectItem>
                              {selectedTruckIds.map((tid) => {
                                const t = trucks.find((x) => x.id === tid)
                                return (
                                  <SelectItem key={tid} value={String(tid)}>
                                    {t?.name || `Camión ${tid}`}
                                  </SelectItem>
                                )
                              })}
                            </SelectContent>
                          </Select>
                        ) : (
                          <span className="text-xs text-muted-foreground">—</span>
                        )}
                      </td>
                      <td className="px-2 py-2 text-xs text-amber-800 dark:text-amber-200">
                        {(item.warnings || [])
                          .map((w) => STATUS_LABEL[w] || w)
                          .join(", ") || (item.blocking ? "bloqueante" : "")}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>

          <div className="space-y-3 rounded-lg border p-4">
            <div className="flex items-center gap-2 text-sm font-medium">
              <Truck className="h-4 w-4" /> Camiones del catálogo
            </div>
            <p className="text-xs text-muted-foreground">
              Selecciona vehículos reales; luego asigna OC en la tabla. Una OC no puede ir a
              dos camiones en la misma operación.
            </p>
            <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
              {trucks.map((t) => {
                const checked = selectedTruckIds.includes(t.id)
                return (
                  <label
                    key={t.id}
                    className={cn(
                      "flex cursor-pointer items-start gap-2 rounded border px-3 py-2 text-sm",
                      checked && "border-primary bg-primary/5",
                    )}
                  >
                    <Checkbox
                      checked={checked}
                      onCheckedChange={() => toggleTruck(t.id)}
                    />
                    <span>
                      <span className="font-medium">{t.name}</span>
                      <span className="block text-xs text-muted-foreground">
                        Cap. {fmtKg(t.max_weight_kg)}
                        {t.plate ? ` · ${t.plate}` : ""}
                      </span>
                    </span>
                  </label>
                )
              })}
            </div>
            <Button
              onClick={() => void runAssignPreview()}
              disabled={assignBusy || selectedTruckIds.length === 0}
            >
              {assignBusy ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" /> Calculando asignación…
                </>
              ) : (
                "Previsualizar asignación (assign-preview)"
              )}
            </Button>
          </div>
        </section>
      ) : null}

      {assignPreview?.trucks?.length ? (
        <section className="space-y-3">
          <h2 className="text-lg font-medium">Resumen por camión</h2>
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
                {(assignPreview.fingerprint_drifts as { folio?: number; detail?: string }[])
                  .map((d) => d.detail || (d.folio != null ? `OC ${d.folio}` : null))
                  .filter(Boolean)
                  .join(" · ")}
                {" "}
                Actualiza el listado (preview) y vuelve a previsualizar la asignación.
              </AlertDescription>
            </Alert>
          ) : null}
          <div className="grid gap-4 lg:grid-cols-2">
            {assignPreview.trucks.map((tr) => {
              const cap = tr.capacity
              const clients = uniqueClientCount(tr.orders || [])
              const st = truckConfirm[tr.truck_id]
              const driftBlocked = (assignPreview.fingerprint_drifts || []).length > 0
              const needsIncompleteAck = (cap?.incomplete_order_count ?? 0) > 0
              const needsUnevaluableAck = (cap?.unevaluable_order_count ?? 0) > 0
              const blocked =
                Boolean(tr.blocking) ||
                Boolean(cap?.blocking) ||
                Boolean(tr.truck_missing) ||
                driftBlocked ||
                !cap?.capacity_known
              return (
                <div
                  key={tr.truck_id}
                  className={cn(
                    "space-y-2 rounded-lg border p-4",
                    blocked && "border-destructive/40",
                  )}
                >
                  <div className="flex items-start justify-between gap-2">
                    <div>
                      <div className="font-medium">
                        {tr.truck?.name || `Camión ${tr.truck_id}`}
                      </div>
                      <div className="text-xs text-muted-foreground">
                        {tr.order_count ?? 0} OC · {clients} clientes
                      </div>
                    </div>
                    {st?.status === "ok" ? (
                      <CheckCircle2 className="h-5 w-5 text-emerald-600" />
                    ) : null}
                  </div>
                  <dl className="grid grid-cols-2 gap-x-3 gap-y-1 text-xs">
                    <dt className="text-muted-foreground">Peso conocido</dt>
                    <dd>{fmtKg(cap?.known_weight_kg)}</dd>
                    <dt className="text-muted-foreground">Peso estimado</dt>
                    <dd>{fmtKg(cap?.estimated_weight_kg)}</dd>
                    <dt className="text-muted-foreground">Capacidad máx.</dt>
                    <dd>{fmtKg(cap?.truck_max_weight_kg)}</dd>
                    <dt className="text-muted-foreground">Utilización</dt>
                    <dd>{fmtPct(cap?.utilization_pct)}</dd>
                  </dl>
                  <p className="text-xs text-muted-foreground">
                    Peso conocido: {fmtKg(cap?.known_weight_kg)}
                    {cap?.weight_total_determined === false
                      ? " · total real aún no determinado"
                      : ""}
                  </p>
                  {needsIncompleteAck || needsUnevaluableAck ? (
                    <p className="text-xs text-amber-800 dark:text-amber-200">
                      {needsUnevaluableAck
                        ? "Capacidad total aún no verificable — revisión manual al confirmar. "
                        : ""}
                      {needsIncompleteAck
                        ? "Cobertura de peso incompleta — aceptación explícita al confirmar. "
                        : ""}
                      No autoriza salida a ruta.
                    </p>
                  ) : null}
                  {(tr.warnings || []).length || (cap?.warnings || []).length ? (
                    <p className="text-xs text-amber-800 dark:text-amber-200">
                      {[...(tr.warnings || []), ...(cap?.warnings || [])]
                        .filter(
                          (w, i, arr) =>
                            w !== "orders_with_unavailable_weight" && arr.indexOf(w) === i,
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
                        .join(" · ")}
                    </p>
                  ) : null}
                  {tr.picking_preview?.products_consolidated?.length ? (
                    <details className="text-xs">
                      <summary className="cursor-pointer font-medium">
                        Picking preliminar (desde OC, no definitivo)
                      </summary>
                      <ul className="mt-1 max-h-32 list-disc overflow-auto pl-4">
                        {tr.picking_preview.products_consolidated.slice(0, 40).map((p, i) => (
                          <li key={i}>
                            {p.quantity}× {p.description || p.code || "ítem"}
                          </li>
                        ))}
                      </ul>
                      <p className="mt-1 text-muted-foreground">
                        El picking definitivo depende de boletas/facturas tras confirmar.
                      </p>
                    </details>
                  ) : null}
                  {st?.status === "ok" && st.planId ? (
                    <Button asChild variant="secondary" size="sm">
                      <Link href={`/distribuidora/planificaciones/${st.planId}`}>
                        Abrir despacho
                        {st.planningCode ? ` ${st.planningCode}` : ""}
                      </Link>
                    </Button>
                  ) : (
                    <Button
                      size="sm"
                      disabled={blocked || st?.status === "preparing" || st?.status === "confirming"}
                      onClick={() => openConfirm(tr.truck_id)}
                    >
                      Preparar confirmación
                    </Button>
                  )}
                  {st?.status === "error" ? (
                    <p className="text-xs text-destructive">{st.message}</p>
                  ) : null}
                </div>
              )
            })}
          </div>
          <p className="text-xs text-muted-foreground">
            Tras confirmar: revisa facturación y picking en el detalle del plan. Asigna chofer
            ahí (módulo 3B.3.2). No se crean cargas ni se certifica mercadería automáticamente.
          </p>
        </section>
      ) : null}

      <Dialog
        open={confirmOpen}
        onOpenChange={(open) => {
          if (!confirmBusy) setConfirmOpen(open)
        }}
      >
        <DialogContent>
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
                return (
                  <>
                    <p>
                      <span className="font-medium">
                        {confirmTruckBlock.truck?.name || `Camión ${confirmTruckId}`}
                      </span>
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
