"use client"

import Link from "next/link"
import { useCallback, useState } from "react"
import { Loader2 } from "lucide-react"

import {
  DISPATCH_PLAN_PICKING_NO_CONFIRMED_MESSAGE,
  DISPATCH_PLAN_PICKING_WAIT_MESSAGE,
  generateDispatchPlanPicking,
  getDispatchPlanInvoicedDocuments,
  getDispatchPlanPickingCliente,
  getDispatchPlanPickingProducto,
  type DispatchPlanDashboard,
  type DispatchPlanPickingClientResponse,
  type DispatchPlanPickingProductResponse,
} from "@/lib/api"
import { DispatchPlanInvoicingDashboard } from "@/components/distribuidora/planificacion/DispatchPlanInvoicingDashboard"
import { DispatchPlanCuadraturaPanel } from "@/components/distribuidora/planificacion/DispatchPlanCuadraturaPanel"
import { DispatchPlanPickingAssignmentPanel } from "@/components/distribuidora/planificacion/DispatchPlanPickingAssignmentPanel"
import { DispatchPlanInvoicedItemsTable } from "@/components/distribuidora/planificacion/DispatchPlanInvoicedItemsTable"
import {
  DispatchPlanPickingClientePanel,
  DispatchPlanPickingProductoPanel,
} from "@/components/distribuidora/planificacion/DispatchPlanPickingPanel"
import type { DispatchPlanInvoicedRow } from "@/lib/api"
import { Button } from "@/components/ui/button"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"

type DispatchPlanDetailTabsProps = {
  planId: number
  dashboard: DispatchPlanDashboard
  dashboardLoading: boolean
  pickingReady: boolean
  pickingReason?: string | null
  onReloadDashboard: (opts?: { include_margin?: boolean }) => Promise<void>
  onMessage: (msg: string) => void
}

export function DispatchPlanDetailTabs({
  planId,
  dashboard,
  dashboardLoading,
  pickingReady,
  pickingReason,
  onReloadDashboard,
  onMessage,
}: DispatchPlanDetailTabsProps) {
  const pickingBlockedMessage =
    pickingReason?.trim() ||
    DISPATCH_PLAN_PICKING_NO_CONFIRMED_MESSAGE ||
    DISPATCH_PLAN_PICKING_WAIT_MESSAGE
  const [tab, setTab] = useState("facturacion")
  const [busy, setBusy] = useState<string | null>(null)
  const [pickingClient, setPickingClient] = useState<DispatchPlanPickingClientResponse | null>(
    null,
  )
  const [pickingProduct, setPickingProduct] = useState<DispatchPlanPickingProductResponse | null>(
    null,
  )
  const [pickingClientLoading, setPickingClientLoading] = useState(false)
  const [pickingProductLoading, setPickingProductLoading] = useState(false)
  const [pickingClientLoaded, setPickingClientLoaded] = useState(false)
  const [pickingProductLoaded, setPickingProductLoaded] = useState(false)
  const [reviewItems, setReviewItems] = useState<DispatchPlanInvoicedRow[] | null>(null)
  const [reviewSummary, setReviewSummary] = useState<{
    confirmed: number
    auto_confirmed?: number
    probable: number
    missing: number
  } | null>(null)

  const refreshPickingFromDb = useCallback(async () => {
    const [pc, pp] = await Promise.all([
      getDispatchPlanPickingCliente(planId),
      getDispatchPlanPickingProducto(planId),
    ])
    setPickingClient(pc)
    setPickingProduct(pp)
    setPickingClientLoaded(true)
    setPickingProductLoaded(true)
    return { pc, pp }
  }, [planId])

  const generatePicking = useCallback(
    async (opts: { includeProbable: boolean }) => {
      setPickingClientLoading(true)
      setPickingProductLoading(true)
      try {
        const r = await generateDispatchPlanPicking(planId, {
          validate: pickingReady,
          includeProbable: opts.includeProbable,
        })
        setPickingClient({
          ...r,
          clients: r.clients ?? [],
        })
        setPickingProduct({
          dispatch_plan_id: r.dispatch_plan_id,
          picking_id: r.picking_id,
          version: r.version,
          ready: r.ready,
          header: r.header,
          items: r.items ?? [],
          warnings: r.warnings,
          totals: r.totals,
        })
        setPickingClientLoaded(true)
        setPickingProductLoaded(true)
        if (r.ready) {
          onMessage(
            `Picking v${r.version ?? "?"} persistido: ${r.clients?.length ?? 0} paradas, ${r.items?.length ?? 0} líneas producto.`,
          )
          await onReloadDashboard()
        } else {
          onMessage(r.reason ?? "No se pudo generar picking")
        }
      } catch (e: unknown) {
        onMessage(e instanceof Error ? e.message : "Error al generar picking")
      } finally {
        setPickingClientLoading(false)
        setPickingProductLoading(false)
      }
    },
    [planId, pickingReady, onMessage, onReloadDashboard],
  )

  const loadPickingFromDb = useCallback(async () => {
    setPickingClientLoading(true)
    setPickingProductLoading(true)
    try {
      await refreshPickingFromDb()
    } catch (e: unknown) {
      onMessage(e instanceof Error ? e.message : "Error al cargar picking")
    } finally {
      setPickingClientLoading(false)
      setPickingProductLoading(false)
    }
  }, [refreshPickingFromDb, onMessage])

  const onTabChange = (value: string) => {
    setTab(value)
    if (
      (value === "picking-cliente" || value === "picking-producto") &&
      !pickingClientLoaded
    ) {
      void loadPickingFromDb()
    }
  }

  return (
    <Tabs value={tab} onValueChange={onTabChange} className="w-full">
      <TabsList className="flex h-auto flex-wrap gap-1">
        <TabsTrigger value="facturacion">Facturación</TabsTrigger>
        <TabsTrigger value="picking-cliente">Picking cliente</TabsTrigger>
        <TabsTrigger value="picking-producto">Picking producto</TabsTrigger>
        <TabsTrigger value="asignacion-pickings">Asignación pickings</TabsTrigger>
        <TabsTrigger value="cuadratura">Cuadratura</TabsTrigger>
      </TabsList>

      <TabsContent value="facturacion" className="mt-4 space-y-4">
        {dashboardLoading ? (
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="size-4 animate-spin" />
            Actualizando facturación…
          </div>
        ) : (
          <DispatchPlanInvoicingDashboard data={dashboard} />
        )}
        <div className="flex flex-wrap gap-2 border-t pt-4">
          <Button
            type="button"
            variant="secondary"
            size="sm"
            disabled={!!busy || dashboardLoading}
            onClick={() => void onReloadDashboard()}
          >
            Actualizar facturación
          </Button>
          <Button
            type="button"
            variant="outline"
            size="sm"
            disabled={!!busy || dashboardLoading}
            onClick={() => void onReloadDashboard({ include_margin: true })}
          >
            Calcular margen
          </Button>
          <Button
            type="button"
            variant="secondary"
            size="sm"
            disabled={!!busy}
            onClick={() =>
              void (async () => {
                setBusy("inv")
                try {
                  const r = await getDispatchPlanInvoicedDocuments(planId)
                  setReviewItems(r.items)
                  setReviewSummary(r.summary)
                  const auto = r.summary.auto_confirmed ?? 0
                  const autoPart =
                    auto > 0 ? ` (${auto} auto-confirmadas ≥75)` : ""
                  onMessage(
                    `Facturación: ${r.summary.confirmed} confirmadas${autoPart}, ${r.summary.probable} probables (60–74), ${r.summary.missing} pendientes.`,
                  )
                } catch (e: unknown) {
                  onMessage(e instanceof Error ? e.message : "Error")
                } finally {
                  setBusy(null)
                }
              })()
            }
          >
            Revisar facturación
          </Button>
        </div>
        {reviewItems && reviewItems.length > 0 ? (
          <div className="space-y-2 border-t pt-4">
            <p className="text-xs font-medium text-muted-foreground">
              Revisión facturación
              {reviewSummary
                ? ` — ${reviewSummary.confirmed} confirmadas`
                : ""}
              {reviewSummary?.auto_confirmed
                ? ` (${reviewSummary.auto_confirmed} auto)`
                : ""}
            </p>
            <DispatchPlanInvoicedItemsTable items={reviewItems} />
          </div>
        ) : null}
      </TabsContent>

      <TabsContent value="picking-cliente" className="mt-4">
        <DispatchPlanPickingClientePanel
          planId={planId}
          pickingReady={pickingReady}
          blockedMessage={pickingBlockedMessage}
          data={pickingClient}
          loading={pickingClientLoading}
          onGenerate={(opts) => void generatePicking(opts)}
          onRefresh={() => void loadPickingFromDb()}
          onMessage={onMessage}
        />
      </TabsContent>

      <TabsContent value="picking-producto" className="mt-4">
        <DispatchPlanPickingProductoPanel
          planId={planId}
          pickingReady={pickingReady}
          blockedMessage={pickingBlockedMessage}
          data={pickingProduct}
          loading={pickingProductLoading}
          onRefresh={() => void loadPickingFromDb()}
          onMessage={onMessage}
        />
      </TabsContent>

      <TabsContent value="asignacion-pickings" className="mt-4">
        <DispatchPlanPickingAssignmentPanel
          planId={planId}
          planStatus={dashboard.plan.status}
          planningCode={dashboard.plan.planning_code}
          onMessage={onMessage}
          onReloadDashboard={onReloadDashboard}
        />
      </TabsContent>

      <TabsContent value="cuadratura" className="mt-4">
        <p className="mb-3 text-xs text-muted-foreground">
          Cuadratura documental v2. También disponible en{" "}
          <Link href={`/distribuidora/cuadraturas/${planId}`} className="text-primary hover:underline">
            módulo Cuadraturas
          </Link>
          .
        </p>
        <DispatchPlanCuadraturaPanel planId={planId} onMessage={onMessage} />
      </TabsContent>
    </Tabs>
  )
}
