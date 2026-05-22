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
  getDispatchPlanInvoicedDocuments,
  getDispatchPlanPickingByClient,
  getDispatchPlanPickingByProduct,
  markDispatchPlanPickingGenerated,
} from "@/lib/api"
import { Button } from "@/components/ui/button"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"

type OrsDispatchWorkflowProps = {
  plan: DispatchPlanSummary | null | undefined
  canConfirm: boolean
  onConfirm: () => Promise<void>
  onPlanUpdated: () => void
}

export function OrsDispatchWorkflow({
  plan,
  canConfirm,
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
    <div className="shrink-0 space-y-2 border-t border-border/70 bg-card/90 p-3">
      <p className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        Flujo operativo
      </p>

      {!isPlanned ? (
        <Button
          type="button"
          size="sm"
          className="h-8 w-full gap-1.5 text-xs"
          disabled={!canConfirm || busy != null}
          onClick={() =>
            void run("confirm", async () => {
              try {
                await onConfirm()
                onPlanUpdated()
                setInvoiceMsg("Planificación confirmada. Puede descargar Excel de facturación.")
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
                const parts = [
                  `Confirmadas: ${r.summary.confirmed}`,
                  `Probables: ${r.summary.probable}`,
                  `Sin documento: ${r.summary.missing}`,
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
                await getDispatchPlanPickingByClient(planId)
                await markDispatchPlanPickingGenerated(planId)
                onPlanUpdated()
                setInvoiceMsg("Picking por cliente generado (documentos reales confirmados).")
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
                await getDispatchPlanPickingByProduct(planId)
                setInvoiceMsg("Picking por producto consolidado (boletas/facturas reales).")
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
