"use client"

import { Loader2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import type { PublishEvaluation } from "@/lib/warehouse/api"

type Props = {
  open: boolean
  evaluating: boolean
  publishing: boolean
  evaluation: PublishEvaluation | null
  onClose: () => void
  onConfirm: () => void
}

function IssueList({
  title,
  items,
  tone,
}: {
  title: string
  items: Array<Record<string, unknown>>
  tone: string
}) {
  if (!items.length) return null
  return (
    <div className="space-y-1">
      <div className={`text-xs font-semibold ${tone}`}>{title} ({items.length})</div>
      <ul className="max-h-28 space-y-0.5 overflow-y-auto text-xs text-muted-foreground">
        {items.slice(0, 40).map((i, idx) => (
          <li key={idx}>• {String(i.message || i.code || "issue")}</li>
        ))}
        {items.length > 40 ? <li>… y {items.length - 40} más</li> : null}
      </ul>
    </div>
  )
}

export function PublishDialog({
  open,
  evaluating,
  publishing,
  evaluation,
  onClose,
  onConfirm,
}: Props) {
  if (!open) return null
  const can = evaluation?.can_publish === true
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/45 p-4">
      <div
        role="dialog"
        aria-modal
        className="w-full max-w-lg rounded-lg border border-border bg-card shadow-xl"
      >
        <div className="border-b border-border px-4 py-3">
          <h2 className="text-base font-semibold text-foreground">Publicar plano</h2>
          <p className="mt-1 text-xs text-muted-foreground">
            Congela la revisión como layout oficial del piso. No certifica seguridad ni autoriza
            apilamiento o almacenamiento especial (p. ej. BAE).
          </p>
        </div>
        <div className="space-y-3 px-4 py-3">
          {evaluating || !evaluation ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" /> Validando plano persistido…
            </div>
          ) : (
            <>
              <div
                className={`rounded-md px-3 py-2 text-sm ${
                  can ? "bg-lq-success/10 text-lq-success" : "bg-destructive/10 text-destructive"
                }`}
              >
                {can
                  ? "Validación OK para publicación (pueden quedar pendientes / advertencias)."
                  : "Publicación bloqueada: corrija integridad o conflictos geométricos."}
              </div>
              <IssueList
                title="Bloqueantes"
                items={evaluation.blockers || []}
                tone="text-destructive"
              />
              <IssueList
                title="Advertencias"
                items={evaluation.warnings || []}
                tone="text-lq-warning"
              />
              <IssueList
                title="Pendiente de validación"
                items={evaluation.pending_validation || []}
                tone="text-muted-foreground"
              />
              <p className="text-[11px] text-muted-foreground">{evaluation.note_es}</p>
            </>
          )}
        </div>
        <div className="flex justify-end gap-2 border-t border-border px-4 py-3">
          <Button type="button" variant="outline" onClick={onClose} disabled={publishing}>
            Cancelar
          </Button>
          <Button
            type="button"
            onClick={onConfirm}
            disabled={!can || evaluating || publishing}
            className="bg-primary hover:bg-primary/90"
          >
            {publishing ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" /> Publicando…
              </>
            ) : (
              "Confirmar publicación"
            )}
          </Button>
        </div>
      </div>
    </div>
  )
}
