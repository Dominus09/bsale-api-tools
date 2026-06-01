"use client"

import { useCallback, useState } from "react"
import { Loader2 } from "lucide-react"

import {
  DISPATCH_PLAN_PICKING_NO_CONFIRMED_MESSAGE,
  DISPATCH_PLAN_PICKING_WAIT_MESSAGE,
  getDispatchPlanInvoicedDocuments,
  getDispatchPlanPickingCliente,
  getDispatchPlanPickingProducto,
  markDispatchPlanPickingGenerated,
  type DispatchPlanDashboard,
  type DispatchPlanPickingClientResponse,
  type DispatchPlanPickingProductResponse,
} from "@/lib/api"
import { DispatchPlanInvoicingDashboard } from "@/components/distribuidora/planificacion/DispatchPlanInvoicingDashboard"
import { DispatchPlanInvoicedItemsTable } from "@/components/distribuidora/planificacion/DispatchPlanInvoicedItemsTable"
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

  const loadPickingCliente = useCallback(async () => {
    if (pickingClientLoaded && pickingClient) return
    setPickingClientLoading(true)
    try {
      const r = await getDispatchPlanPickingCliente(planId, { validate: true })
      setPickingClient(r)
      setPickingClientLoaded(true)
    } catch (e: unknown) {
      onMessage(e instanceof Error ? e.message : "Error al cargar picking cliente")
    } finally {
      setPickingClientLoading(false)
    }
  }, [planId, pickingClientLoaded, pickingClient, onMessage])

  const loadPickingProducto = useCallback(async () => {
    if (pickingProductLoaded && pickingProduct) return
    setPickingProductLoading(true)
    try {
      const r = await getDispatchPlanPickingProducto(planId, { validate: true })
      setPickingProduct(r)
      setPickingProductLoaded(true)
    } catch (e: unknown) {
      onMessage(e instanceof Error ? e.message : "Error al cargar picking producto")
    } finally {
      setPickingProductLoading(false)
    }
  }, [planId, pickingProductLoaded, pickingProduct, onMessage])

  const onTabChange = (value: string) => {
    setTab(value)
    if (value === "picking-cliente") void loadPickingCliente()
    if (value === "picking-producto") void loadPickingProducto()
  }

  return (
    <Tabs value={tab} onValueChange={onTabChange} className="w-full">
      <TabsList className="flex h-auto flex-wrap gap-1">
        <TabsTrigger value="facturacion">Facturación</TabsTrigger>
        <TabsTrigger value="picking-cliente">Picking cliente</TabsTrigger>
        <TabsTrigger value="picking-producto">Picking producto</TabsTrigger>
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

      <TabsContent value="picking-cliente" className="mt-4 space-y-3">
        {!pickingReady ? (
          <Alert>
            <AlertTitle>Picking no disponible</AlertTitle>
            <AlertDescription className="text-sm">{pickingBlockedMessage}</AlertDescription>
          </Alert>
        ) : null}
        {pickingClient?.ready === false ? (
          <Alert variant="default">
            <AlertDescription className="text-sm">
              {pickingClient.reason ?? pickingBlockedMessage}
            </AlertDescription>
          </Alert>
        ) : null}
        {pickingClientLoading ? (
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="size-4 animate-spin" />
            Cargando picking por cliente…
          </div>
        ) : pickingClient ? (
          <p className="text-sm">
            <strong>{pickingClient.clients.length}</strong> paradas con documentos confirmados.
            {pickingClient.degraded ? " (respuesta degradada)" : ""}
          </p>
        ) : (
          <Button type="button" size="sm" variant="outline" onClick={() => void loadPickingCliente()}>
            Cargar picking cliente
          </Button>
        )}
        <Button
          type="button"
          size="sm"
          disabled={!!busy || !pickingReady || pickingClientLoading}
          onClick={() =>
            void (async () => {
              setBusy("gen-pc")
              try {
                const r = await getDispatchPlanPickingCliente(planId)
                setPickingClient(r)
                setPickingClientLoaded(true)
                await markDispatchPlanPickingGenerated(planId)
                onMessage(`Picking cliente: ${r.clients.length} paradas.`)
                await onReloadDashboard()
              } catch (e: unknown) {
                onMessage(e instanceof Error ? e.message : "Error picking")
              } finally {
                setBusy(null)
              }
            })()
          }
        >
          Generar y guardar picking cliente
        </Button>
      </TabsContent>

      <TabsContent value="picking-producto" className="mt-4 space-y-3">
        {!pickingReady ? (
          <Alert>
            <AlertTitle>Picking no disponible</AlertTitle>
            <AlertDescription className="text-sm">{pickingBlockedMessage}</AlertDescription>
          </Alert>
        ) : null}
        {pickingProduct?.ready === false ? (
          <Alert variant="default">
            <AlertDescription className="text-sm">
              {pickingProduct.reason ?? pickingBlockedMessage}
            </AlertDescription>
          </Alert>
        ) : null}
        {pickingProductLoading ? (
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="size-4 animate-spin" />
            Cargando picking por producto…
          </div>
        ) : pickingProduct ? (
          <p className="text-sm">
            <strong>{pickingProduct.items.length}</strong> líneas consolidadas.
            {pickingProduct.degraded ? " (respuesta degradada)" : ""}
          </p>
        ) : (
          <Button
            type="button"
            size="sm"
            variant="outline"
            onClick={() => void loadPickingProducto()}
          >
            Cargar picking producto
          </Button>
        )}
        <Button
          type="button"
          size="sm"
          disabled={!!busy || !pickingReady || pickingProductLoading}
          onClick={() =>
            void (async () => {
              setBusy("gen-pp")
              try {
                const r = await getDispatchPlanPickingProducto(planId)
                setPickingProduct(r)
                setPickingProductLoaded(true)
                onMessage(`Picking producto: ${r.items.length} líneas.`)
              } catch (e: unknown) {
                onMessage(e instanceof Error ? e.message : "Error picking")
              } finally {
                setBusy(null)
              }
            })()
          }
        >
          Generar picking producto
        </Button>
      </TabsContent>
    </Tabs>
  )
}
