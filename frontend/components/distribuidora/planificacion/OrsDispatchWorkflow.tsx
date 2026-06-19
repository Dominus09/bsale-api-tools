"use client"

import { useState } from "react"
import {
  CheckCircle2,
  ClipboardList,
  Download,
  FileSearch,
  Package,
  Loader2,
} from "lucide-react"

import type { DispatchPlanSummary } from "@/lib/api"
import {
  downloadDispatchPlanBillingExcel,
  generateDispatchPlanPicking,
  getDispatchPlanInvoicedDocuments,
} from "@/lib/api"
import { Button } from "@/components/ui/button"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"

type OrsDispatchWorkflowProps = {
  plan: DispatchPlanSummary | null | undefined
  canConfirm: boolean
  defaultPlanningName?: string
  onConfirm: (planningName: string) => Promise<void>
  onPlanUpdated: () => void
}

export function OrsDispatchWorkflow({
  plan,
  canConfirm,
  defaultPlanningName = "",
  onConfirm,
  onPlanUpdated,
}: OrsDispatchWorkflowProps) {
  const [busy, setBusy] = useState<string | null>(null)
  const [invoiceMsg, setInvoiceMsg] = useState<string | null>(null)
  const [invoiceAlert, setInvoiceAlert] = useState<"default" | "destructive">("default")

  const planId = plan?.id
  const status = plan?.status
  const isPlanned = status && status !== "draft"

  async function run(label: string, fn: () => Promise<void>) {
    setBusy(label)
    try {
      await fn()
    } finally {
      setBusy(null)
    }
  }

  return (
    <div className="shrink-0 space-y-2 border-t border-border/70 bg-card/95 p-3 shadow-[0_-4px_12px_rgba(0,0,0,0.06)]">
      <p className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        Flujo operativo
      </p>

      {!isPlanned ? (
        <>
          <label className="block space-y-1">
            <span className="text-[10px] text-muted-foreground">Nombre planificación</span>
            <input
              type="text"
              className="h-8 w-full rounded-md border border-input bg-background px-2 text-xs"
              placeholder="Ej. Castro Sur"
              defaultValue={defaultPlanningName}
              id="ors-planning-name"
            />
          </label>
          <Button
            type="button"
            size="sm"
            className="h-8 w-full gap-1.5 text-xs"
            disabled={!canConfirm || busy != null}
            onClick={() =>
              void run("confirm", async () => {
                try {
                  const el = document.getElementById("ors-planning-name") as HTMLInputElement | null
                  const name = el?.value?.trim() || defaultPlanningName || "Ruta"
                  await onConfirm(name)
                  setInvoiceMsg("Planificación confirmada y guardada en historial.")
                  setInvoiceAlert("default")
                } catch (e: unknown) {
                  setInvoiceMsg(e instanceof Error ? e.message : "Error al confirmar")
                  setInvoiceAlert("destructive")
                }
              })
            }
          >
          {busy === "confirm" ? (
            <Loader2 className="size-3.5 animate-spin" aria-hidden />
          ) : (
            <CheckCircle2 className="size-3.5" aria-hidden />
          )}
          Confirmar planificación
        </Button>
        </>
      ) : plan?.planning_code ? (
        <p className="text-[11px] text-muted-foreground">
          Guardado como{" "}
          <span className="font-mono font-medium text-foreground">{plan.planning_code}</span>
          {plan.id ? (
            <>
              {" "}
              ·{" "}
              <a
                href={`/distribuidora/planificaciones/${plan.id}`}
                className="text-primary underline-offset-2 hover:underline"
              >
                Ver historial
              </a>
            </>
          ) : null}
        </p>
      ) : (
        <div className="flex flex-col gap-1.5">
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="h-8 w-full gap-1.5 text-xs"
            disabled={!planId || busy != null}
            onClick={() =>
              void run("excel", async () => {
                if (planId) await downloadDispatchPlanBillingExcel(planId)
              })
            }
          >
            {busy === "excel" ? (
              <Loader2 className="size-3.5 animate-spin" aria-hidden />
            ) : (
              <Download className="size-3.5" aria-hidden />
            )}
            Excel facturación
          </Button>
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="h-8 w-full gap-1.5 text-xs"
            disabled={!planId || busy != null}
            onClick={() =>
              void run("invoice", async () => {
                if (!planId) return
                const r = await getDispatchPlanInvoicedDocuments(planId)
                const auto = r.summary.auto_confirmed ?? 0
                const parts = [
                  `Confirmadas: ${r.summary.confirmed}${
                    auto > 0 ? ` (${auto} auto ≥75)` : ""
                  }`,
                  `Probables (60–74): ${r.summary.probable}`,
                  `Pendientes: ${r.summary.missing}`,
                ]
                setInvoiceAlert(r.summary.missing > 0 ? "destructive" : "default")
                setInvoiceMsg(parts.join(" · "))
                if (r.warnings.length) {
                  setInvoiceMsg(
                    `${parts.join(" · ")}. ${r.warnings.length} OC(s) sin documento facturado asociado.`,
                  )
                }
              })
            }
          >
            {busy === "invoice" ? (
              <Loader2 className="size-3.5 animate-spin" aria-hidden />
            ) : (
              <FileSearch className="size-3.5" aria-hidden />
            )}
            Revisar facturación
          </Button>
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="h-8 w-full gap-1.5 text-xs"
            disabled={!planId || busy != null}
            onClick={() =>
              void run("pick-client", async () => {
                if (!planId) return
                const r = await generateDispatchPlanPicking(planId)
                if (r.ready === false) {
                  setInvoiceMsg(
                    r.reason ??
                      "Los pickings estarán disponibles una vez existan documentos facturados o relacionados.",
                  )
                  setInvoiceAlert("destructive")
                  return
                }
                onPlanUpdated()
                setInvoiceMsg(
                  `Picking v${r.version ?? "?"} persistido (${r.clients?.length ?? 0} paradas).`,
                )
                setInvoiceAlert("default")
              })
            }
          >
            {busy === "pick-client" ? (
              <Loader2 className="size-3.5 animate-spin" aria-hidden />
            ) : (
              <ClipboardList className="size-3.5" aria-hidden />
            )}
            Picking por cliente
          </Button>
          <Button
            type="button"
            variant="outline"
            size="sm"
            className="h-8 w-full gap-1.5 text-xs"
            disabled={!planId || busy != null}
            onClick={() =>
              void run("pick-product", async () => {
                if (!planId) return
                const r = await generateDispatchPlanPicking(planId)
                if (r.ready === false) {
                  setInvoiceMsg(
                    r.reason ??
                      "Los pickings estarán disponibles una vez existan documentos facturados o relacionados.",
                  )
                  setInvoiceAlert("destructive")
                  return
                }
                setInvoiceMsg(
                  `Picking producto v${r.version ?? "?"} (${r.items?.length ?? 0} líneas).`,
                )
                setInvoiceAlert("default")
              })
            }
          >
            {busy === "pick-product" ? (
              <Loader2 className="size-3.5 animate-spin" aria-hidden />
            ) : (
              <Package className="size-3.5" aria-hidden />
            )}
            Picking por producto
          </Button>
        </div>
      )}

      {invoiceMsg ? (
        <Alert variant={invoiceAlert} className="py-2">
          <AlertTitle className="text-xs">Facturación / picking</AlertTitle>
          <AlertDescription className="text-[11px]">{invoiceMsg}</AlertDescription>
        </Alert>
      ) : null}
    </div>
  )
}
