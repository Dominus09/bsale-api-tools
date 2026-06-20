"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { FileText, Loader2, Plus, RefreshCw, Save, Trash2 } from "lucide-react"

import {
  addDispatchPlanOrder,
  createDispatchPlanLoadBatch,
  deleteDispatchPlanLoadBatch,
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
} from "@/lib/api"
import {
  exportDispatchPlanPickingClientePdf,
  exportDispatchPlanPickingProductoPdf,
} from "@/lib/dispatch-plan-picking-pdf"
import { dispatchPlanVersionLabel } from "@/lib/dispatch-plan-operational-status"
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
  const [addPreview, setAddPreview] = useState<{
    can_add: boolean
    has_picking: boolean
    warning?: string | null
    blocked_reason?: string | null
  } | null>(null)
  const [pendingAdd, setPendingAdd] = useState<DispatchPlanOrderSearchHit | null>(null)
  const [addReason, setAddReason] = useState("")
  const [showRegenerateDialog, setShowRegenerateDialog] = useState(false)

  const canAddOrders = planStatus !== "dispatched" && planStatus !== "delivered"

  const loadAll = useCallback(async () => {
    setLoading(true)
    try {
      const [data, log, preview] = await Promise.all([
        getDispatchPlanPickingAssignments(planId),
        getDispatchPlanPickingRegenerationLog(planId),
        previewAddDispatchPlanOrder(planId),
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
    } catch (e: unknown) {
      onMessage(e instanceof Error ? e.message : "Error al cargar asignaciones")
    } finally {
      setLoading(false)
    }
  }, [planId, onMessage])

  useEffect(() => {
    void loadAll()
  }, [loadAll])

  const dirty = useMemo(() => {
    return assignments.some(
      (a) => (draftAssignments[a.related_document_id] ?? null) !== (a.load_batch_id ?? null),
    )
  }, [assignments, draftAssignments])

  const saveAssignments = async () => {
    setBusy("save")
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
      onMessage("Asignaciones guardadas.")
    } catch (e: unknown) {
      onMessage(e instanceof Error ? e.message : "Error al guardar")
    } finally {
      setBusy(null)
    }
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

  const requestAddOrder = (hit: DispatchPlanOrderSearchHit) => {
    if (!canAddOrders) {
      onMessage("No se pueden agregar órdenes a un plan despachado.")
      return
    }
    setPendingAdd(hit)
    if (addPreview?.has_picking) {
      setShowRegenerateDialog(true)
    } else {
      void confirmAddOrder(hit, false)
    }
  }

  const confirmAddOrder = async (
    hit: DispatchPlanOrderSearchHit,
    regenerate: boolean,
  ) => {
    setBusy("add")
    setShowRegenerateDialog(false)
    try {
      const r = await addDispatchPlanOrder(planId, {
        oc_document_id: hit.oc_document_id,
        regenerate_picking: regenerate,
        reason: addReason.trim() || undefined,
      })
      if (r.requires_regenerate) {
        setPendingAdd(hit)
        setShowRegenerateDialog(true)
        return
      }
      if (r.added) {
        onMessage(
          regenerate
            ? `OC agregada y picking regenerado (v${(r.picking as { version?: number })?.version ?? "?"}).`
            : "OC agregada al plan.",
        )
        setSearchHits([])
        setSearchQ("")
        setAddReason("")
        setPendingAdd(null)
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

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <h2 className="text-sm font-semibold">Asignación pickings</h2>
          <p className="text-xs text-muted-foreground">
            {dispatchPlanVersionLabel(planningCode ?? null, planId, pickingVersion)}
            {hasPicking ? " · pickings generados" : " · sin picking persistido"}
          </p>
        </div>
        <Button type="button" variant="outline" size="sm" onClick={() => void loadAll()}>
          <RefreshCw className="mr-1 size-3.5" />
          Actualizar
        </Button>
      </div>

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
                  {b.description ? (
                    <p className="text-muted-foreground">{b.description}</p>
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
            Documentos
          </h3>
          {dirty ? (
            <Button
              type="button"
              size="sm"
              disabled={!!busy}
              onClick={() => void saveAssignments()}
            >
              <Save className="mr-1 size-3.5" />
              Guardar asignaciones
            </Button>
          ) : null}
        </div>
        {!assignments.length ? (
          <p className="text-sm text-muted-foreground">
            Genere el picking para ver documentos facturados asignables.
          </p>
        ) : (
          <div className="overflow-x-auto rounded-md border">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Documento</TableHead>
                  <TableHead>Cliente</TableHead>
                  <TableHead className="text-right">Monto</TableHead>
                  <TableHead>Picking asignado</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {assignments.map((a) => (
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
                        value={
                          draftAssignments[a.related_document_id] != null
                            ? String(draftAssignments[a.related_document_id])
                            : "none"
                        }
                        onValueChange={(v) =>
                          setDraftAssignments((prev) => ({
                            ...prev,
                            [a.related_document_id]: v === "none" ? null : Number(v),
                          }))
                        }
                      >
                        <SelectTrigger className="h-8 text-xs">
                          <SelectValue placeholder="Sin asignar" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="none">Sin asignar</SelectItem>
                          {batches.map((b) => (
                            <SelectItem key={b.id} value={String(b.id)}>
                              {b.name}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}
      </section>

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
            {addPreview?.warning ? (
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
                      <TableHead>OC</TableHead>
                      <TableHead>Cliente</TableHead>
                      <TableHead className="text-right">Monto</TableHead>
                      <TableHead />
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {searchHits.map((h) => (
                      <TableRow key={h.oc_document_id}>
                        <TableCell className="font-mono text-xs">{h.oc_number ?? h.oc_document_id}</TableCell>
                        <TableCell className="text-xs">{h.client_name ?? "—"}</TableCell>
                        <TableCell className="text-right text-xs tabular-nums">
                          {formatClp(Number(h.oc_total_amount ?? 0))}
                        </TableCell>
                        <TableCell className="text-right">
                          <Button
                            type="button"
                            size="sm"
                            variant="outline"
                            className="h-7 text-xs"
                            disabled={!!busy}
                            onClick={() => requestAddOrder(h)}
                          >
                            Agregar
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            ) : null}
            <div className="space-y-1">
              <Label htmlFor="add-reason" className="text-xs">
                Motivo (opcional, recomendado si regenera pickings)
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
          </>
        )}
      </section>

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

      <AlertDialog open={showRegenerateDialog} onOpenChange={setShowRegenerateDialog}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Regenerar pickings</AlertDialogTitle>
            <AlertDialogDescription>
              Esta planificación ya posee pickings generados. Agregar nuevas órdenes obligará a
              regenerar los documentos (nueva versión).
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel onClick={() => setPendingAdd(null)}>Cancelar</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => {
                if (pendingAdd) void confirmAddOrder(pendingAdd, true)
              }}
            >
              Regenerar
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
