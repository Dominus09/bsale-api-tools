"use client"

import { useEffect, useState } from "react"
import { AlertTriangle, Loader2 } from "lucide-react"

import {
  applyDocumentLoadReassignment,
  previewDocumentLoadReassignment,
  type DocumentLoadReassignmentPreview,
} from "@/lib/api"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"

type Banner = {
  count: number
  fromBatchIds: number[]
  toBatchIds: number[]
  movedOcDocumentIds: number[]
  relatedDocumentIds: number[]
  hasPicking: boolean
  pickingVersion: number | null
} | null

type Props = {
  open: boolean
  onOpenChange: (open: boolean) => void
  planId: number
  planStatus: string
  banner: Banner
  onMessage: (msg: string) => void
  onApplied: () => void
}

export function DocumentLoadReassignmentDialog({
  open,
  onOpenChange,
  planId,
  planStatus,
  banner,
  onMessage,
  onApplied,
}: Props) {
  const [loading, setLoading] = useState(false)
  const [applying, setApplying] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [preview, setPreview] = useState<DocumentLoadReassignmentPreview | null>(null)

  useEffect(() => {
    if (!open || !banner) {
      setPreview(null)
      setError(null)
      return
    }
    let cancelled = false
    setLoading(true)
    setError(null)
    void previewDocumentLoadReassignment(planId, {
      oc_document_ids: banner.movedOcDocumentIds,
      related_document_ids: banner.relatedDocumentIds,
    })
      .then((p) => {
        if (!cancelled) setPreview(p)
      })
      .catch((e: unknown) => {
        if (!cancelled) {
          setError(e instanceof Error ? e.message : "No se pudo previsualizar el traslado")
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [open, banner, planId])

  const apply = async () => {
    if (!banner || !preview?.can_apply) return
    setApplying(true)
    setError(null)
    try {
      const res = await applyDocumentLoadReassignment(planId, {
        oc_document_ids: banner.movedOcDocumentIds,
        related_document_ids: banner.relatedDocumentIds,
        confirm: true,
      })
      if (!res.applied && !res.idempotent) {
        setError(res.message_es || "No se confirmó la persistencia del traslado")
        return
      }
      onMessage(res.message_es || "Traslado documental aplicado")
      onOpenChange(false)
      onApplied()
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al aplicar traslado")
    } finally {
      setApplying(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[90vh] overflow-y-auto sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>Revisar traslado de documentos</DialogTitle>
          <DialogDescription>
            Plan {planStatus}. Confirme solo si desea alinear facturas/boletas al grupo
            actual de las OC movidas.
          </DialogDescription>
        </DialogHeader>

        {loading ? (
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="size-4 animate-spin" /> Preparando vista previa…
          </div>
        ) : null}

        {error ? (
          <Alert className="border-red-500/40 bg-red-500/5">
            <AlertTriangle className="size-4" />
            <AlertTitle className="text-sm">No se pudo completar</AlertTitle>
            <AlertDescription className="text-xs">{error}</AlertDescription>
          </Alert>
        ) : null}

        {preview ? (
          <div className="space-y-3 text-sm">
            <p>{preview.message_es}</p>

            {(preview.warnings_es || []).length ? (
              <Alert className="border-amber-500/40 bg-amber-500/5">
                <AlertTriangle className="size-4" />
                <AlertTitle className="text-sm">Advertencias</AlertTitle>
                <AlertDescription>
                  <ul className="mt-1 list-inside list-disc text-xs">
                    {(preview.warnings_es || []).map((w) => (
                      <li key={w}>{w}</li>
                    ))}
                  </ul>
                </AlertDescription>
              </Alert>
            ) : null}

            {preview.blocked ? (
              <Alert className="border-red-500/40 bg-red-500/5">
                <AlertTitle className="text-sm">Operación bloqueada</AlertTitle>
                <AlertDescription className="text-xs">
                  {preview.blocked_reason_es}
                </AlertDescription>
              </Alert>
            ) : null}

            {(preview.hard_conflicts || []).length ? (
              <Alert className="border-red-500/40 bg-red-500/5">
                <AlertTitle className="text-sm">Conflictos cruzados</AlertTitle>
                <AlertDescription>
                  <ul className="mt-1 list-inside list-disc text-xs">
                    {(preview.hard_conflicts || []).map((c) => (
                      <li key={`${c.related_document_id}-${c.code}`}>
                        {c.message_es || `Documento ${c.related_document_id}`}
                      </li>
                    ))}
                  </ul>
                </AlertDescription>
              </Alert>
            ) : null}

            <div className="rounded-md border p-3">
              <p className="mb-2 text-xs font-medium text-muted-foreground">
                Documentos afectados ({preview.moves?.length ?? 0})
              </p>
              <ul className="max-h-48 space-y-2 overflow-y-auto text-xs">
                {(preview.moves || []).map((m) => (
                  <li key={m.related_document_id} className="border-b border-border/40 pb-2">
                    <div className="font-medium">
                      Doc {m.related_document_id}
                      {m.document_number != null ? ` · N° ${m.document_number}` : ""}
                      {m.client_name ? ` · ${m.client_name}` : ""}
                    </div>
                    <div className="text-muted-foreground">
                      OC {m.oc_document_id}:{" "}
                      {m.from_load_batch_name || m.from_load_batch_id || "sin grupo"} →{" "}
                      {m.to_load_batch_name || m.to_load_batch_id}
                    </div>
                  </li>
                ))}
                {!preview.moves?.length ? (
                  <li className="text-muted-foreground">Sin movimientos pendientes.</li>
                ) : null}
              </ul>
            </div>

            {(preview.effects_es || []).length ? (
              <ul className="list-inside list-disc text-xs text-muted-foreground">
                {(preview.effects_es || []).map((e) => (
                  <li key={e}>{e}</li>
                ))}
              </ul>
            ) : null}
          </div>
        ) : null}

        <DialogFooter className="gap-2 sm:gap-0">
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
            Cancelar
          </Button>
          <Button
            type="button"
            disabled={
              applying || loading || !preview?.can_apply || Boolean(preview?.blocked)
            }
            onClick={() => void apply()}
          >
            {applying ? (
              <Loader2 className="mr-1.5 size-4 animate-spin" />
            ) : null}
            Confirmar traslado documental
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
