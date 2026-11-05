"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import {
  AlertTriangle,
  ChevronDown,
  ChevronRight,
  FileText,
  Loader2,
  Plus,
  RefreshCw,
  Save,
  Trash2,
} from "lucide-react"

import {
  addDispatchPlanOrder,
  createDispatchPlanLoadBatch,
  deleteDispatchPlanLoadBatch,
  getDispatchPlanLoadOrganization,
  getDispatchPlanPickingAssignments,
  getDispatchPlanPickingCliente,
  getDispatchPlanPickingProducto,
  getDispatchPlanPickingRegenerationLog,
  previewAddDispatchPlanOrder,
  saveDispatchPlanPickingAssignments,
  searchDispatchPlanOrders,
  type DispatchPlanDocumentAssignment,
  type DispatchPlanLoadBatch,
  type DispatchPlanOrderEvent,
  type DispatchPlanOrderSearchHit,
  type DispatchPlanStatus,
  type DocumentLoadReconciliationResponse,
  type SupplementalAddPreview,
} from "@/lib/api"
import { Checkbox } from "@/components/ui/checkbox"
import {
  exportDispatchPlanPickingClientePdf,
  exportDispatchPlanPickingProductoPdf,
} from "@/lib/dispatch-plan-picking-pdf"
import { dispatchPlanVersionLabel } from "@/lib/dispatch-plan-operational-status"
import { stripBatchRoleDescription } from "@/lib/load-organization-ui"
import { formatClp } from "@/lib/ors-map-ui"
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
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
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
import { Textarea } from "@/components/ui/textarea"

type DispatchPlanPickingAssignmentPanelProps = {
  planId: number
  planStatus?: DispatchPlanStatus
  planningCode?: string | null
  onMessage: (msg: string) => void
  onReloadDashboard?: () => Promise<void>
}

export function DispatchPlanPickingAssignmentPanel({
  planId,
  planStatus,
  planningCode,
  onMessage,
  onReloadDashboard,
}: DispatchPlanPickingAssignmentPanelProps) {
  const [loading, setLoading] = useState(true)
  const [busy, setBusy] = useState<string | null>(null)
  const [batches, setBatches] = useState<DispatchPlanLoadBatch[]>([])
  const [assignments, setAssignments] = useState<DispatchPlanDocumentAssignment[]>([])
  const [draftAssignments, setDraftAssignments] = useState<
    Record<number, number | null>
  >({})
  const [hasPicking, setHasPicking] = useState(false)
  const [pickingVersion, setPickingVersion] = useState<number | null>(null)
  const [events, setEvents] = useState<DispatchPlanOrderEvent[]>([])
  const [newBatchName, setNewBatchName] = useState("")
  const [newBatchDesc, setNewBatchDesc] = useState("")
  const [searchQ, setSearchQ] = useState("")
  const [searchHits, setSearchHits] = useState<DispatchPlanOrderSearchHit[]>([])
  const [addPreview, setAddPreview] = useState<SupplementalAddPreview | null>(null)
  const [selectedHits, setSelectedHits] = useState<DispatchPlanOrderSearchHit[]>([])
  const [impactPreview, setImpactPreview] = useState<SupplementalAddPreview | null>(null)
  const [addReason, setAddReason] = useState("")
  const [ackIncomplete, setAckIncomplete] = useState(false)
  const [ackUnevaluable, setAckUnevaluable] = useState(false)
  const [showConfirmAddDialog, setShowConfirmAddDialog] = useState(false)
  const [showManualSaveDialog, setShowManualSaveDialog] = useState(false)
  const [orgStatus, setOrgStatus] = useState<string>("legacy")
  const [docRecon, setDocRecon] = useState<DocumentLoadReconciliationResponse | null>(
    null,
  )
  const [advancedOpen, setAdvancedOpen] = useState(false)

  const canAddOrders = planStatus !== "dispatched" && planStatus !== "delivered"
  const organized =
    orgStatus === "organized" || Boolean(docRecon?.organization_ready)
  const needsAdvancedReview =
    (docRecon?.documents_unassigned ?? 0) > 0 ||
    (docRecon?.conflicts?.length ?? 0) > 0 ||
    docRecon?.document_assignment_complete === false

  const loadAll = useCallback(async () => {
    setLoading(true)
    try {
      const [data, log, preview, org] = await Promise.all([
        getDispatchPlanPickingAssignments(planId),
        getDispatchPlanPickingRegenerationLog(planId),
        previewAddDispatchPlanOrder(planId),
        getDispatchPlanLoadOrganization(planId).catch(() => null),
      ])
      setBatches(data.batches)
      setAssignments(data.assignments)
      setDraftAssignments(
        Object.fromEntries(
          data.assignments.map((a) => [a.related_document_id, a.load_batch_id]),
        ),
      )
      setHasPicking(data.has_picking)
      setPickingVersion(data.picking_version ?? null)
      setEvents(log.items)
      setAddPreview(preview)
      setDocRecon(
        (data.document_reconciliation as DocumentLoadReconciliationResponse | undefined) ??
          null,
      )
      if (org) {
        setOrgStatus(String(org.load_organization_status || "legacy"))
      }
    } catch (e: unknown) {
      onMessage(e instanceof Error ? e.message : "Error al cargar asignaciones")
    } finally {
      setLoading(false)
    }
  }, [planId, onMessage])

  useEffect(() => {
    void loadAll()
  }, [loadAll])

  useEffect(() => {
    if (needsAdvancedReview && organized) {
      // Solo sugiere; no abre solo (apertura explícita).
    }
  }, [needsAdvancedReview, organized])

  const dirty = useMemo(() => {
    return assignments.some(
      (a) => (draftAssignments[a.related_document_id] ?? null) !== (a.load_batch_id ?? null),
    )
  }, [assignments, draftAssignments])

  const changedManualCount = useMemo(() => {
    return assignments.filter(
      (a) => (draftAssignments[a.related_document_id] ?? null) !== (a.load_batch_id ?? null),
    ).length
  }, [assignments, draftAssignments])

  const persistAssignments = async () => {
    setBusy("save")
    setShowManualSaveDialog(false)
    try {
      const payload = assignments.map((a) => ({
        related_document_id: a.related_document_id,
        load_batch_id: draftAssignments[a.related_document_id] ?? null,
        oc_document_id: a.oc_document_id,
        document_number: a.document_number,
        client_name: a.client_name,
        document_total: a.document_total,
      }))
      const data = await saveDispatchPlanPickingAssignments(planId, payload)
      setAssignments(data.assignments)
      setDraftAssignments(
        Object.fromEntries(
          data.assignments.map((a) => [a.related_document_id, a.load_batch_id]),
        ),
      )
      onMessage(
        "Asignaciones manuales guardadas. La conciliación automática no las sobrescribe.",
      )
    } catch (e: unknown) {
      onMessage(e instanceof Error ? e.message : "Error al guardar")
    } finally {
      setBusy(null)
    }
  }

  const requestSaveAssignments = () => {
    if (!dirty) return
    setShowManualSaveDialog(true)
  }

  const createBatch = async () => {
    const name = newBatchName.trim()
    if (!name) {
      onMessage("Ingrese un nombre para el picking.")
      return
    }
    setBusy("batch")
    try {
      await createDispatchPlanLoadBatch(planId, {
        name,
        description: newBatchDesc.trim() || undefined,
      })
      setNewBatchName("")
      setNewBatchDesc("")
      await loadAll()
      onMessage(`Picking "${name}" creado.`)
    } catch (e: unknown) {
      onMessage(e instanceof Error ? e.message : "Error al crear picking")
    } finally {
      setBusy(null)
    }
  }

  const removeBatch = async (batchId: number) => {
    setBusy(`del-${batchId}`)
    try {
      await deleteDispatchPlanLoadBatch(planId, batchId)
      await loadAll()
      onMessage("Picking eliminado.")
    } catch (e: unknown) {
      onMessage(e instanceof Error ? e.message : "Error al eliminar")
    } finally {
      setBusy(null)
    }
  }

  const runSearch = async () => {
    const q = searchQ.trim()
    if (q.length < 2) {
      onMessage("Ingrese al menos 2 caracteres.")
      return
    }
    setBusy("search")
    try {
      const r = await searchDispatchPlanOrders(planId, q)
      setSearchHits(r.items)
      if (!r.items.length) onMessage("Sin resultados.")
    } catch (e: unknown) {
      onMessage(e instanceof Error ? e.message : "Error al buscar")
    } finally {
      setBusy(null)
    }
  }

  const selectedIds = useMemo(
    () => selectedHits.map((h) => h.oc_document_id),
    [selectedHits],
  )

  const refreshImpactPreview = useCallback(
    async (ids: number[]) => {
      if (!ids.length) {
        setImpactPreview(null)
        setAckIncomplete(false)
        setAckUnevaluable(false)
        return
      }
      try {
        const p = await previewAddDispatchPlanOrder(planId, { ocDocumentIds: ids })
        setImpactPreview(p)
        if (!p.requires_incomplete_weight_ack) setAckIncomplete(false)
        if (!p.requires_unevaluable_weight_ack) setAckUnevaluable(false)
      } catch (e: unknown) {
        onMessage(e instanceof Error ? e.message : "Error al previsualizar OC")
      }
    },
    [planId, onMessage],
  )

  const toggleSelectHit = (hit: DispatchPlanOrderSearchHit) => {
    setSelectedHits((prev) => {
      const exists = prev.some((x) => x.oc_document_id === hit.oc_document_id)
      const next = exists
        ? prev.filter((x) => x.oc_document_id !== hit.oc_document_id)
        : [...prev, hit]
      void refreshImpactPreview(next.map((x) => x.oc_document_id))
      return next
    })
  }

  const requestConfirmAdd = () => {
    if (!canAddOrders) {
      onMessage("No se pueden agregar órdenes a un plan despachado.")
      return
    }
    if (!selectedIds.length) {
      onMessage("Seleccione al menos una OC.")
      return
    }
    if (addReason.trim().length < 3) {
      onMessage("Indique un motivo de incorporación (mín. 3 caracteres).")
      return
    }
    const p = impactPreview
    if (p?.requires_incomplete_weight_ack && !ackIncomplete) {
      onMessage("Confirme el control de peso incompleto para continuar.")
      return
    }
    if (p?.requires_unevaluable_weight_ack && !ackUnevaluable) {
      onMessage("Confirme el control de peso no evaluable para continuar.")
      return
    }
    if (p && p.can_add === false) {
      onMessage(p.blocked_reason || "No se pueden agregar las OC seleccionadas.")
      return
    }
    setShowConfirmAddDialog(true)
  }

  const confirmAddOrders = async () => {
    setBusy("add")
    setShowConfirmAddDialog(false)
    const usesSupp = Boolean(
      impactPreview?.uses_supplemental_group || organized,
    )
    const legacyNeedsRegen = Boolean(
      (impactPreview?.has_picking || addPreview?.has_picking) && !usesSupp,
    )
    try {
      const r = await addDispatchPlanOrder(planId, {
        oc_document_ids: selectedIds,
        regenerate_picking: legacyNeedsRegen,
        reason: addReason.trim(),
        acknowledge_incomplete_weight: ackIncomplete,
        acknowledge_unevaluable_weight: ackUnevaluable,
      })
      if (r.requires_regenerate) {
        onMessage(
          "El plan legacy requiere regenerar pickings. Confirme de nuevo tras revisar.",
        )
        return
      }
      if (r.added) {
        onMessage(r.message_es || "OC agregadas al plan.")
        setSearchHits([])
        setSearchQ("")
        setSelectedHits([])
        setImpactPreview(null)
        setAddReason("")
        setAckIncomplete(false)
        setAckUnevaluable(false)
        await loadAll()
        await onReloadDashboard?.()
      }
    } catch (e: unknown) {
      onMessage(e instanceof Error ? e.message : "Error al agregar OC")
    } finally {
      setBusy(null)
    }
  }

  const exportBatchPdf = async (batch: DispatchPlanLoadBatch, kind: "cliente" | "producto") => {
    setBusy(`pdf-${batch.id}-${kind}`)
    try {
      if (kind === "cliente") {
        const data = await getDispatchPlanPickingCliente(planId, {
          loadBatchId: batch.id,
        })
        if (!data.header || !data.clients?.length) {
          onMessage("Sin documentos asignados a este picking.")
          return
        }
        await exportDispatchPlanPickingClientePdf({
          header: data.header,
          clients: data.clients,
          warnings: data.warnings,
          version: data.version,
          generatedAt: data.generated_at,
        })
      } else {
        const data = await getDispatchPlanPickingProducto(planId, {
          loadBatchId: batch.id,
        })
        if (!data.header || !data.items?.length) {
          onMessage("Sin productos para este picking.")
          return
        }
        await exportDispatchPlanPickingProductoPdf({
          header: data.header,
          items: data.items,
          warnings: data.warnings,
          version: data.version,
          generatedAt: data.generated_at,
        })
      }
      onMessage(`PDF ${kind} — ${batch.name} generado.`)
    } catch (e: unknown) {
      onMessage(e instanceof Error ? e.message : "Error al exportar PDF")
    } finally {
      setBusy(null)
    }
  }

  if (loading) {
    return (
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <Loader2 className="size-4 animate-spin" />
        Cargando asignación de pickings…
      </div>
    )
  }

  const showLegacyBatchUi = !organized
  const showAdvancedBody = !organized || advancedOpen

  const batchesAndDocsSection = (
    <>
      <section className="space-y-3 rounded-lg border p-4">
        <h3 className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
          Pickings configurables
        </h3>
        <div className="flex flex-wrap gap-2">
          {batches.map((b) => (
            <div
              key={b.id}
              className="flex min-w-[200px] flex-col gap-2 rounded-md border bg-muted/20 p-3 text-xs"
            >
              <div className="flex items-start justify-between gap-2">
                <div>
                  <p className="font-semibold">{b.name}</p>
                  {stripBatchRoleDescription(b.description) ? (
                    <p className="text-muted-foreground">
                      {stripBatchRoleDescription(b.description)}
                    </p>
                  ) : null}
                </div>
                {batches.length > 1 ? (
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon"
                    className="size-7 shrink-0"
                    disabled={!!busy}
                    onClick={() => void removeBatch(b.id)}
                  >
                    <Trash2 className="size-3.5" />
                  </Button>
                ) : null}
              </div>
              {/* PDF por lote: en planes organizados se usan los de Organización de carga */}
              {showLegacyBatchUi ? (
                <div className="flex flex-wrap gap-1">
                  <Button
                    type="button"
                    variant="secondary"
                    size="sm"
                    className="h-7 text-[11px]"
                    disabled={!!busy || !hasPicking}
                    onClick={() => void exportBatchPdf(b, "cliente")}
                  >
                    <FileText className="mr-1 size-3" />
                    PDF cliente
                  </Button>
                  <Button
                    type="button"
                    variant="secondary"
                    size="sm"
                    className="h-7 text-[11px]"
                    disabled={!!busy || !hasPicking}
                    onClick={() => void exportBatchPdf(b, "producto")}
                  >
                    <FileText className="mr-1 size-3" />
                    PDF producto
                  </Button>
                </div>
              ) : (
                <p className="text-[11px] text-muted-foreground">
                  Exportes por grupo: sección Organización de carga.
                </p>
              )}
            </div>
          ))}
        </div>
        <div className="grid gap-2 sm:grid-cols-[1fr_1fr_auto]">
          <Input
            placeholder="Nombre (ej. Picking 2)"
            value={newBatchName}
            onChange={(e) => setNewBatchName(e.target.value)}
          />
          <Input
            placeholder="Descripción opcional"
            value={newBatchDesc}
            onChange={(e) => setNewBatchDesc(e.target.value)}
          />
          <Button type="button" size="sm" disabled={!!busy} onClick={() => void createBatch()}>
            <Plus className="mr-1 size-3.5" />
            Agregar picking
          </Button>
        </div>
      </section>

      <section className="space-y-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <h3 className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            Documentos del picking
          </h3>
          {dirty ? (
            <Button
              type="button"
              size="sm"
              disabled={!!busy}
              onClick={() => requestSaveAssignments()}
            >
              <Save className="mr-1 size-3.5" />
              Guardar asignaciones
            </Button>
          ) : null}
        </div>
        {!assignments.length ? (
          <p className="text-sm text-muted-foreground">
            Genere el picking para ver documentos facturados asignables a lotes.
          </p>
        ) : (
          <div className="space-y-2">
            <p className="text-[11px] text-muted-foreground">
              Cambio manual de lote: la conciliación automática no lo sobrescribe. Confirme
              explícitamente al guardar.
            </p>
            <div className="overflow-x-auto rounded-md border">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Documento</TableHead>
                    <TableHead>Cliente</TableHead>
                    <TableHead className="text-right">Monto</TableHead>
                    <TableHead>Lote físico</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {assignments.map((a) => {
                    const batchId = draftAssignments[a.related_document_id] ?? null
                    return (
                      <TableRow key={a.related_document_id}>
                        <TableCell className="font-mono text-xs">
                          {a.document_number ?? a.related_document_id}
                        </TableCell>
                        <TableCell className="text-xs">{a.client_name ?? "—"}</TableCell>
                        <TableCell className="text-right text-xs tabular-nums">
                          {formatClp(Number(a.document_total ?? 0))}
                        </TableCell>
                        <TableCell>
                          <Select
                            value={batchId != null ? String(batchId) : "none"}
                            onValueChange={(v) =>
                              setDraftAssignments((prev) => ({
                                ...prev,
                                [a.related_document_id]: v === "none" ? null : Number(v),
                              }))
                            }
                          >
                            <SelectTrigger className="h-8 text-xs">
                              <SelectValue placeholder="Pendiente de lote" />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="none">Pendiente de lote físico</SelectItem>
                              {batches.map((b) => (
                                <SelectItem key={b.id} value={String(b.id)}>
                                  {b.name}
                                </SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </TableCell>
                      </TableRow>
                    )
                  })}
                </TableBody>
              </Table>
            </div>
          </div>
        )}
      </section>
    </>
  )

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <h2 className="text-sm font-semibold">
            {organized ? "Herramientas operativas" : "Lotes físicos de carga"}
          </h2>
          <p className="text-xs text-muted-foreground">
            {dispatchPlanVersionLabel(planningCode ?? null, planId, pickingVersion)}
            {hasPicking
              ? " · picking generado"
              : " · sin picking persistido"}
            {organized ? " · organización anticipada activa" : null}
          </p>
        </div>
        <Button type="button" variant="outline" size="sm" onClick={() => void loadAll()}>
          <RefreshCw className="mr-1 size-3.5" />
          Actualizar
        </Button>
      </div>

      {organized ? (
        <Alert className="border-border/80 bg-muted/20">
          <AlertTitle className="text-sm">Organización anticipada en uso</AlertTitle>
          <AlertDescription className="space-y-2 text-xs">
            <p>
              Los grupos y PDF/Excel de productos por grupo están en «Organización de
              carga». Aquí puede agregar OC al plan o abrir la asignación documental
              avanzada solo si necesita excepciones.
            </p>
            {needsAdvancedReview ? (
              <p className="flex items-start gap-1.5 text-amber-800 dark:text-amber-200">
                <AlertTriangle className="mt-0.5 size-3.5 shrink-0" />
                Hay documentos sin lote o conflictos de conciliación
                {(docRecon?.documents_unassigned ?? 0) > 0
                  ? ` (${docRecon?.documents_unassigned} pendientes)`
                  : ""}
                . Revise en asignación avanzada.
              </p>
            ) : null}
          </AlertDescription>
        </Alert>
      ) : null}

      {/* Agregar OC: siempre visible (operativo) */}
      <section className="space-y-3 rounded-lg border p-4">
        <h3 className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
          Agregar OC al plan
        </h3>
        {!canAddOrders ? (
          <Alert>
            <AlertTitle>Plan despachado</AlertTitle>
            <AlertDescription>
              No se pueden incorporar órdenes después del despacho.
            </AlertDescription>
          </Alert>
        ) : (
          <>
            {organized ? (
              <Alert className="border-sky-200 bg-sky-50/70 dark:border-sky-900 dark:bg-sky-950/30">
                <AlertTitle>Grupo suplementario</AlertTitle>
                <AlertDescription className="text-sm">
                  Con organización completa, las OC nuevas forman un grupo «OC adicionales»
                  sin rehacer el picking físico de los grupos originales. El picking de
                  productos del suplemento exige facturación vigente.
                </AlertDescription>
              </Alert>
            ) : addPreview?.warning ? (
              <Alert className="border-amber-200 bg-amber-50/80 dark:border-amber-900 dark:bg-amber-950/30">
                <AlertTitle>Pickings existentes</AlertTitle>
                <AlertDescription className="text-sm">{addPreview.warning}</AlertDescription>
              </Alert>
            ) : null}
            <div className="flex flex-wrap gap-2">
              <Input
                className="max-w-xs"
                placeholder="Número documento o cliente"
                value={searchQ}
                onChange={(e) => setSearchQ(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") void runSearch()
                }}
              />
              <Button
                type="button"
                variant="secondary"
                size="sm"
                disabled={!!busy}
                onClick={() => void runSearch()}
              >
                Buscar OC
              </Button>
            </div>
            {searchHits.length > 0 ? (
              <div className="overflow-x-auto rounded-md border">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead className="w-10" />
                      <TableHead>OC</TableHead>
                      <TableHead>Cliente</TableHead>
                      <TableHead className="text-right">Monto</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {searchHits.map((h) => {
                      const checked = selectedIds.includes(h.oc_document_id)
                      return (
                        <TableRow key={h.oc_document_id}>
                          <TableCell>
                            <Checkbox
                              checked={checked}
                              onCheckedChange={() => toggleSelectHit(h)}
                              aria-label={`Seleccionar OC ${h.oc_number ?? h.oc_document_id}`}
                            />
                          </TableCell>
                          <TableCell className="font-mono text-xs">
                            {h.oc_number ?? h.oc_document_id}
                          </TableCell>
                          <TableCell className="text-xs">{h.client_name ?? "—"}</TableCell>
                          <TableCell className="text-right text-xs tabular-nums">
                            {formatClp(Number(h.oc_total_amount ?? 0))}
                          </TableCell>
                        </TableRow>
                      )
                    })}
                  </TableBody>
                </Table>
              </div>
            ) : null}
            {impactPreview ? (
              <div className="space-y-2 rounded-md border bg-muted/20 p-3 text-xs">
                {impactPreview.uses_supplemental_group ? (
                  <p>
                    Grupo a crear:{" "}
                    <span className="font-medium">
                      {impactPreview.proposed_supplemental_group_name || "OC adicionales"}
                    </span>
                  </p>
                ) : null}
                {impactPreview.capacity ? (
                  <p>
                    Peso conocido proyectado:{" "}
                    {Number(impactPreview.capacity.known_weight_kg ?? 0).toFixed(1)} kg
                    {impactPreview.capacity.truck_max_weight_kg != null
                      ? ` / ${impactPreview.capacity.truck_max_weight_kg} kg`
                      : ""}
                    {impactPreview.capacity.utilization_pct != null
                      ? ` (${impactPreview.capacity.utilization_pct}% uso)`
                      : ""}
                    {impactPreview.capacity.overloaded ? " — sobrecarga conocida" : ""}
                  </p>
                ) : null}
                {impactPreview.picking_pending_invoice ? (
                  <p className="text-amber-800 dark:text-amber-200">
                    Algunas OC aún no tienen factura/boleta usable: el picking de productos
                    del suplemento quedará pendiente de facturación.
                  </p>
                ) : null}
                {impactPreview.blocked_reason ? (
                  <p className="text-destructive">{impactPreview.blocked_reason}</p>
                ) : null}
                {impactPreview.requires_incomplete_weight_ack ? (
                  <label className="flex items-start gap-2">
                    <Checkbox
                      checked={ackIncomplete}
                      onCheckedChange={(v) => setAckIncomplete(v === true)}
                    />
                    <span>
                      Confirmo control operacional con cobertura de peso incompleta (no
                      autoriza salida a ruta).
                    </span>
                  </label>
                ) : null}
                {impactPreview.requires_unevaluable_weight_ack ? (
                  <label className="flex items-start gap-2">
                    <Checkbox
                      checked={ackUnevaluable}
                      onCheckedChange={(v) => setAckUnevaluable(v === true)}
                    />
                    <span>
                      Confirmo control operacional con OC sin peso usable (no autoriza
                      salida a ruta).
                    </span>
                  </label>
                ) : null}
              </div>
            ) : null}
            <div className="space-y-1">
              <Label htmlFor="add-reason" className="text-xs">
                Motivo de incorporación (obligatorio)
              </Label>
              <Textarea
                id="add-reason"
                rows={2}
                className="text-xs"
                value={addReason}
                onChange={(e) => setAddReason(e.target.value)}
                placeholder="Ej. OC urgente ingresada 15:00"
              />
            </div>
            <Button
              type="button"
              size="sm"
              disabled={!!busy || selectedIds.length === 0}
              onClick={requestConfirmAdd}
            >
              Revisar e incorporar ({selectedIds.length})
            </Button>
          </>
        )}
      </section>

      {organized ? (
        <div className="rounded-lg border border-dashed border-border/80">
          <button
            type="button"
            className="flex w-full items-center justify-between gap-2 px-4 py-3 text-left text-sm font-medium hover:bg-muted/30"
            onClick={() => setAdvancedOpen((v) => !v)}
            aria-expanded={advancedOpen}
          >
            <span className="flex items-center gap-2">
              {advancedOpen ? (
                <ChevronDown className="size-4" />
              ) : (
                <ChevronRight className="size-4" />
              )}
              Asignación documental avanzada
              {needsAdvancedReview ? (
                <Badge variant="outline" className="text-[10px]">
                  Revisar
                </Badge>
              ) : null}
            </span>
            <span className="text-xs font-normal text-muted-foreground">
              {advancedOpen ? "Ocultar" : "Abrir explícitamente"}
            </span>
          </button>
          {showAdvancedBody ? (
            <div className="space-y-6 border-t px-4 py-4">{batchesAndDocsSection}</div>
          ) : null}
        </div>
      ) : (
        batchesAndDocsSection
      )}

      <section className="space-y-2">
        <h3 className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
          Historial / versiones
        </h3>
        {!events.length ? (
          <p className="text-xs text-muted-foreground">Sin eventos registrados.</p>
        ) : (
          <div className="space-y-2">
            {events.slice(0, 15).map((ev) => (
              <div
                key={ev.id}
                className="flex flex-wrap items-center gap-2 rounded-md border px-3 py-2 text-xs"
              >
                <Badge variant="outline">{ev.action}</Badge>
                {ev.picking_version ? (
                  <span className="font-mono">v{ev.picking_version}</span>
                ) : null}
                {ev.oc_number ? <span>OC {ev.oc_number}</span> : null}
                {ev.reason ? (
                  <span className="text-muted-foreground">— {ev.reason}</span>
                ) : null}
                {ev.user_name ? (
                  <span className="text-muted-foreground">{ev.user_name}</span>
                ) : null}
                {ev.created_at ? (
                  <span className="ml-auto text-muted-foreground">
                    {new Date(ev.created_at).toLocaleString("es-CL")}
                  </span>
                ) : null}
              </div>
            ))}
          </div>
        )}
      </section>

      <AlertDialog open={showConfirmAddDialog} onOpenChange={setShowConfirmAddDialog}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Confirmar incorporación de OC</AlertDialogTitle>
            <AlertDialogDescription asChild>
              <div className="space-y-2 text-sm text-muted-foreground">
                <p>
                  Se incorporarán {selectedIds.length} OC
                  {impactPreview?.proposed_supplemental_group_name
                    ? ` al grupo «${impactPreview.proposed_supplemental_group_name}»`
                    : ""}
                  .
                </p>
                {impactPreview?.uses_supplemental_group ? (
                  <p>
                    Los grupos originales y sus pickings físicos no se rehacen. Si hay
                    facturación, se versiona el picking general y el export de productos del
                    suplemento usa solo las cantidades nuevas.
                  </p>
                ) : (
                  <p>
                    Plan sin organización completa: se conserva el flujo clásico (puede
                    regenerar el picking vigente).
                  </p>
                )}
                {impactPreview?.picking_pending_invoice ? (
                  <p>
                    No se declarará picking de productos listo mientras falte facturación.
                  </p>
                ) : null}
              </div>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancelar</AlertDialogCancel>
            <AlertDialogAction onClick={() => void confirmAddOrders()}>
              Confirmar
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog open={showManualSaveDialog} onOpenChange={setShowManualSaveDialog}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Confirmar asignación manual</AlertDialogTitle>
            <AlertDialogDescription>
              Va a guardar {changedManualCount} cambio(s) manual(es) de lote. Esto no se
              sobrescribe en silencio por la conciliación automática OC→grupo. Use solo para
              excepciones auditables.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancelar</AlertDialogCancel>
            <AlertDialogAction onClick={() => void persistAssignments()}>
              Guardar manualmente
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
