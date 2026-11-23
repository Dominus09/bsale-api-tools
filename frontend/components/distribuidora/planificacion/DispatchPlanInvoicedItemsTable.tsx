"use client"

import { useState } from "react"
import type { DispatchPlanInvoicedRow } from "@/lib/api"
import {
  excludeDispatchPlanOrder,
  previewDispatchPlanOrderExclude,
} from "@/lib/api"
import { DispatchPlanInvoicedStatusBadge } from "@/components/distribuidora/planificacion/DispatchPlanInvoicedStatusBadge"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Textarea } from "@/components/ui/textarea"
import { hasManagementAccess, type StaffUserLike } from "@/lib/permissions"
import { Loader2 } from "lucide-react"

function formatClp(n: number | null | undefined): string {
  if (n == null || Number.isNaN(Number(n))) return "—"
  return `$${Math.round(Number(n)).toLocaleString("es-CL")}`
}

export function DispatchPlanInvoicedItemsTable({
  items,
  planId,
  staff,
  onExcluded,
}: {
  items: DispatchPlanInvoicedRow[]
  planId?: number
  staff?: StaffUserLike | null
  onExcluded?: () => void | Promise<void>
}) {
  const canAdmin = hasManagementAccess(staff || null)
  const [busyId, setBusyId] = useState<number | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [dialogOpen, setDialogOpen] = useState(false)
  const [reason, setReason] = useState("")
  const [target, setTarget] = useState<DispatchPlanInvoicedRow | null>(null)
  const [previewNote, setPreviewNote] = useState<string | null>(null)
  const [blockers, setBlockers] = useState<string[]>([])

  if (!items.length) return null

  async function openExclude(row: DispatchPlanInvoicedRow) {
    if (!planId || !canAdmin) return
    setError(null)
    setBusyId(row.oc_document_id)
    setTarget(row)
    setReason("")
    setBlockers([])
    setPreviewNote(null)
    try {
      const prev = await previewDispatchPlanOrderExclude(planId, row.oc_document_id)
      setPreviewNote(prev.note_es || null)
      if (prev.already_excluded) {
        setError("Esta OC ya está excluida de la planificación.")
        setDialogOpen(false)
        return
      }
      if (!prev.can_exclude) {
        setBlockers((prev.blockers || []).map((b) => b.message || b.code || "Bloqueo"))
        setDialogOpen(true)
        return
      }
      setDialogOpen(true)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al evaluar exclusión")
    } finally {
      setBusyId(null)
    }
  }

  async function confirmExclude() {
    if (!planId || !target) return
    const trimmed = reason.trim()
    if (trimmed.length < 5) {
      setError("Indique un motivo de al menos 5 caracteres.")
      return
    }
    setBusyId(target.oc_document_id)
    setError(null)
    try {
      await excludeDispatchPlanOrder(planId, target.oc_document_id, trimmed)
      setDialogOpen(false)
      setTarget(null)
      setReason("")
      await onExcluded?.()
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "No se pudo excluir")
    } finally {
      setBusyId(null)
    }
  }

  return (
    <div className="space-y-2">
      {error ? (
        <p className="rounded-md border border-red-500/30 bg-red-500/10 px-3 py-2 text-xs text-red-700 dark:text-red-200">
          {error}
        </p>
      ) : null}
      <div className="overflow-x-auto rounded-md border border-border/70">
        <table className="w-full min-w-[760px] text-left text-xs">
          <thead className="border-b bg-muted/40 text-[11px] uppercase tracking-wide text-muted-foreground">
            <tr>
              <th className="px-2 py-2">OC</th>
              <th className="px-2 py-2">Estado</th>
              <th className="px-2 py-2">Origen</th>
              <th className="px-2 py-2">Doc. relacionado</th>
              <th className="px-2 py-2">Monto doc.</th>
              <th className="px-2 py-2">Score</th>
              {canAdmin && planId ? <th className="px-2 py-2">Acción</th> : null}
            </tr>
          </thead>
          <tbody>
            {items.map((row) => (
              <tr
                key={row.oc_document_id}
                className="border-b border-border/40 last:border-0"
              >
                <td className="px-2 py-1.5 tabular-nums font-medium">
                  {row.oc_number ?? row.oc_document_id}
                </td>
                <td className="px-2 py-1.5">
                  <DispatchPlanInvoicedStatusBadge row={row} compact />
                </td>
                <td className="px-2 py-1.5 text-muted-foreground">
                  {row.relation_source ?? "—"}
                </td>
                <td className="px-2 py-1.5 tabular-nums">
                  {row.related_document_number ?? "—"}
                </td>
                <td className="px-2 py-1.5 tabular-nums text-muted-foreground">
                  {formatClp(row.document_total)}
                </td>
                <td className="px-2 py-1.5 tabular-nums text-muted-foreground">
                  {row.probable_score != null ? String(row.probable_score) : "—"}
                </td>
                {canAdmin && planId ? (
                  <td className="px-2 py-1.5">
                    {row.status === "missing" ? (
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        className="h-7 text-[11px]"
                        disabled={busyId === row.oc_document_id}
                        onClick={() => void openExclude(row)}
                      >
                        {busyId === row.oc_document_id ? (
                          <Loader2 className="size-3 animate-spin" />
                        ) : (
                          "Quitar de planificación"
                        )}
                      </Button>
                    ) : (
                      <span className="text-[11px] text-muted-foreground">—</span>
                    )}
                  </td>
                ) : null}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>Quitar OC de la planificación</DialogTitle>
            <DialogDescription>
              OC {target?.oc_number ?? target?.oc_document_id}
              {target?.document_total != null
                ? ` · doc. ${formatClp(target.document_total)}`
                : ""}
              . La OC no se elimina de Bsale.
            </DialogDescription>
          </DialogHeader>
          {blockers.length > 0 ? (
            <div className="space-y-2 text-sm text-amber-800 dark:text-amber-200">
              <p className="font-medium">No se puede excluir ahora:</p>
              <ul className="list-disc space-y-1 pl-4">
                {blockers.map((b) => (
                  <li key={b}>{b}</li>
                ))}
              </ul>
            </div>
          ) : (
            <div className="space-y-2">
              {previewNote ? (
                <p className="text-xs text-muted-foreground">{previewNote}</p>
              ) : null}
              <label className="block text-sm font-medium">
                Motivo (obligatorio)
                <Textarea
                  className="mt-1"
                  value={reason}
                  onChange={(e) => setReason(e.target.value)}
                  placeholder="Ej.: pedido cancelado por vendedor"
                  rows={3}
                />
              </label>
            </div>
          )}
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => setDialogOpen(false)}>
              Cancelar
            </Button>
            {blockers.length === 0 ? (
              <Button
                type="button"
                disabled={!!busyId || reason.trim().length < 5}
                onClick={() => void confirmExclude()}
              >
                {busyId ? <Loader2 className="mr-1 size-3.5 animate-spin" /> : null}
                Confirmar exclusión
              </Button>
            ) : null}
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
