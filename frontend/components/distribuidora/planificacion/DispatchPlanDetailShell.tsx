"use client"

/**
 * Cabecera compacta + pestañas del detalle de despacho (ORS y simple_list).
 * Conserva componentes existentes; reorganiza la información.
 */

import Link from "next/link"
import { useCallback, useState } from "react"
import { Loader2, RefreshCw } from "lucide-react"

import {
  DISPATCH_PLAN_PICKING_NO_CONFIRMED_MESSAGE,
  DISPATCH_PLAN_PICKING_WAIT_MESSAGE,
  generateDispatchPlanPicking,
  getDispatchPlanInvoicedDocuments,
  getDispatchPlanPickingCliente,
  getDispatchPlanPickingProducto,
  type DispatchPlanDashboard,
  type DispatchPlanInvoicedRow,
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
import { DispatchPlanDriverAssign } from "@/components/distribuidora/planificacion/DispatchPlanDriverAssign"
import { DispatchPlanOrdersWeightTable } from "@/components/distribuidora/planificacion/DispatchPlanOrdersWeightTable"
import { formatClp } from "@/lib/ors-map-ui"
import { dispatchPlanOperationalLabel } from "@/lib/dispatch-plan-operational-status"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"

type Props = {
  planId: number
  dashboard: DispatchPlanDashboard
  dashboardLoading: boolean
  onReloadDashboard: (opts?: { include_margin?: boolean }) => Promise<void>
  onRefreshCommercial: () => Promise<void>
  onMessage: (msg: string) => void
  refreshBusy?: boolean
}

function Meta({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="min-w-0">
      <p className="text-[10px] uppercase tracking-wide text-muted-foreground">{label}</p>
      <p className="truncate text-sm font-semibold tabular-nums">{value}</p>
      {sub ? <p className="text-[11px] text-muted-foreground">{sub}</p> : null}
    </div>
  )
}

export function DispatchPlanDetailShell({
  planId,
  dashboard,
  dashboardLoading,
  onReloadDashboard,
  onRefreshCommercial,
  onMessage,
  refreshBusy,
}: Props) {
  const plan = dashboard.plan
  const ls = dashboard.load_summary
  const inv = dashboard.invoicing
  const margin = dashboard.margin
  const costs = ls?.costs
  const pickingReady = Boolean(dashboard.picking?.ready)
  const pickingBlockedMessage =
    dashboard.picking?.reason?.trim() ||
    DISPATCH_PLAN_PICKING_NO_CONFIRMED_MESSAGE ||
    DISPATCH_PLAN_PICKING_WAIT_MESSAGE

  const [tab, setTab] = useState("resumen")
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
  const [reviewItems, setReviewItems] = useState<DispatchPlanInvoicedRow[] | null>(null)

  const refreshPickingFromDb = useCallback(async () => {
    const [pc, pp] = await Promise.all([
      getDispatchPlanPickingCliente(planId),
      getDispatchPlanPickingProducto(planId),
    ])
    setPickingClient(pc)
    setPickingProduct(pp)
    setPickingClientLoaded(true)
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
        setPickingClient({ ...r, clients: r.clients ?? [] })
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
        if (r.ready) {
          onMessage(
            `Picking v${r.version ?? "?"} persistido: ${r.clients?.length ?? 0} paradas.`,
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

  const onTabChange = (value: string) => {
    setTab(value)
    if ((value === "picking" || value === "picking-cliente") && !pickingClientLoaded) {
      void refreshPickingFromDb().catch(() => undefined)
    }
  }

  const originLabel =
    plan.planning_origin === "simple_list" ? "Lista / Excel" : "ORS tradicional"
  const routeCoverage = costs?.route_coverage
  const routeCalculated = plan.ors_metrics_calculated !== false
  const routeLabel =
    plan.ors_metrics_calculated === false
      ? "Ruta no calculada"
      : routeCoverage === "partial"
        ? "Ruta parcial"
        : plan.ors_metrics_calculated
          ? "Ruta completa"
          : "—"

  const ocCount = inv.total_orders
  const clients = ls?.kpis?.clients_from_oc ?? ls?.kpis?.clients ?? "—"
  const weightKg = ls?.weight?.weight_total_kg ?? plan.weight_total_kg
  const cobertura = ls?.weight?.cobertura_pct ?? plan.weight_cobertura_pct
  const pickingSales = ls?.kpis?.picking_sales_clp
  const pickingStatus = ls?.picking?.has_snapshot
    ? `Picking generado${ls.picking.version != null ? ` v${ls.picking.version}` : ""}`
    : "Picking pendiente"
  const cuadraturaLabel =
    dashboard.cuadratura?.label ||
    (dashboard.cuadratura?.started ? "Cuadratura iniciada" : "Cuadratura no iniciada")
  const amountsBasis =
    inv.amounts_basis_label_es || "Total documento (con IVA)"

  return (
    <div className="space-y-4">
      <section className="rounded-xl border border-border/80 bg-card/60 p-4">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <p className="font-mono text-xs text-muted-foreground">
              {plan.planning_code ?? `PLAN-${plan.id}`}
            </p>
            <h2 className="text-lg font-semibold leading-tight">
              {plan.planning_name ?? plan.route_name}
            </h2>
            <p className="text-sm text-muted-foreground">
              {plan.truck_name} ·{" "}
              {plan.planning_date?.slice(0, 10) ?? plan.created_at?.slice(0, 10)} · {originLabel}
            </p>
            <div className="mt-2 flex flex-wrap gap-1.5">
              <Badge variant="secondary">{dispatchPlanOperationalLabel(plan.status)}</Badge>
              <Badge variant="outline">{routeLabel}</Badge>
              {ls?.operational_status_label ? (
                <Badge variant="outline">{ls.operational_status_label}</Badge>
              ) : null}
              <Badge variant={dashboard.cuadratura?.started ? "outline" : "secondary"}>
                {cuadraturaLabel}
              </Badge>
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            <Button asChild size="sm" variant="secondary">
              <Link href={`/distribuidora/planificaciones/${planId}/mapa`}>Mapa entregas</Link>
            </Button>
            <Button
              type="button"
              size="sm"
              variant="outline"
              disabled={!!refreshBusy || dashboardLoading}
              onClick={() => void onRefreshCommercial()}
            >
              {refreshBusy ? (
                <Loader2 className="mr-1 size-3.5 animate-spin" />
              ) : (
                <RefreshCw className="mr-1 size-3.5" />
              )}
              Actualizar datos comerciales
            </Button>
          </div>
        </div>

        <div className="mt-4 grid gap-3 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-6 border-t border-border/50 pt-3">
          <Meta label="OC / clientes" value={`${ocCount} · ${clients}`} sub={originLabel} />
          <Meta
            label="Venta OC"
            value={formatClp(inv.total_oc_amount_clp)}
            sub="Órdenes del plan"
          />
          <Meta
            label="Venta facturada"
            value={formatClp(inv.confirmed.amount_clp)}
            sub={`${inv.confirmed.count} docs · ${amountsBasis}`}
          />
          <Meta
            label="Venta picking"
            value={pickingSales != null ? formatClp(pickingSales) : "—"}
            sub={pickingStatus}
          />
          <Meta
            label="Margen comercial"
            value={
              margin?.visible && margin.commercial_margin_clp != null
                ? formatClp(margin.commercial_margin_clp)
                : margin?.restricted
                  ? "Restringido"
                  : "—"
            }
            sub={
              margin?.cost_basis
                ? `variant_cost · ${margin.cost_basis}`
                : margin?.partial
                  ? "Parcial / incompleto"
                  : "No descuenta NC autom."
            }
          />
          <Meta
            label="Peso / chofer"
            value={
              weightKg != null
                ? `${Number(weightKg).toLocaleString("es-CL", { maximumFractionDigits: 1 })} kg`
                : "—"
            }
            sub={[
              cobertura != null
                ? `Cobertura ${Number(cobertura).toLocaleString("es-CL", {
                    maximumFractionDigits: 0,
                  })}%`
                : null,
              plan.driver_name ||
                (plan.driver_user_id ? `Chofer #${plan.driver_user_id}` : "Chofer pendiente"),
            ]
              .filter(Boolean)
              .join(" · ")}
          />
        </div>

        {costs?.note ? (
          <Alert className="mt-3 border-amber-500/30 bg-amber-500/5">
            <AlertTitle className="text-sm">Ruta / transporte</AlertTitle>
            <AlertDescription className="text-xs">{costs.note}</AlertDescription>
          </Alert>
        ) : null}
      </section>

      <Tabs value={tab} onValueChange={onTabChange} className="w-full">
        <TabsList className="flex h-auto w-full flex-wrap justify-start gap-1">
          <TabsTrigger value="resumen">Resumen</TabsTrigger>
          <TabsTrigger value="ruta">Ruta y costos</TabsTrigger>
          <TabsTrigger value="picking">Picking y carga</TabsTrigger>
          <TabsTrigger value="chofer">Chofer y auditoría</TabsTrigger>
          <TabsTrigger value="cierre">Cierre de ruta y cuadratura</TabsTrigger>
        </TabsList>

        <TabsContent value="resumen" className="mt-4 space-y-4">
          {dashboardLoading ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="size-4 animate-spin" /> Actualizando…
            </div>
          ) : null}
          <div className="flex flex-wrap gap-2">
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
              disabled={!!busy}
              onClick={() =>
                void (async () => {
                  setBusy("inv")
                  try {
                    const r = await getDispatchPlanInvoicedDocuments(planId)
                    setReviewItems(r.items)
                    onMessage(
                      `Facturación: ${r.summary.confirmed} confirmadas, ${r.summary.probable} probables, ${r.summary.missing} pendientes.`,
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
          <DispatchPlanInvoicingDashboard data={dashboard} />
          {reviewItems?.length ? (
            <div className="space-y-2">
              <p className="text-sm font-medium">Detalle documental</p>
              <DispatchPlanInvoicedItemsTable items={reviewItems} />
            </div>
          ) : null}
          {margin?.visible && margin.calculated_at ? (
            <p className="text-[11px] text-muted-foreground">
              Margen comercial vivo · calculado {margin.calculated_at}
              {margin.cost_basis ? ` · base costo: ${margin.cost_basis}` : ""}
              {margin.cost_coverage
                ? ` · costos ${margin.cost_coverage.lines_with_cost ?? "—"}/${margin.cost_coverage.lines_total ?? "—"} líneas`
                : ""}
              {" · "}no es margen neto definitivo (NC no descontadas automáticamente).
            </p>
          ) : null}
          {dashboard.plan_orders?.length ? (
            <div className="space-y-2">
              <p className="text-sm font-medium">Órdenes / peso congelado</p>
              <DispatchPlanOrdersWeightTable orders={dashboard.plan_orders} />
            </div>
          ) : null}
        </TabsContent>

        <TabsContent value="ruta" className="mt-4 space-y-3">
          {!routeCalculated ? (
            <Alert className="border-amber-500/30 bg-amber-500/5">
              <AlertTitle className="text-sm">Ruta no calculada</AlertTitle>
              <AlertDescription className="text-xs">
                Este despacho no tiene métricas ORS. No se inventan km, duración ni costo de
                transporte. Las rutas históricas no se alteran.
              </AlertDescription>
            </Alert>
          ) : null}
          <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-4 text-sm">
            <div className="rounded-md border px-3 py-2">
              <p className="text-[11px] text-muted-foreground">Km</p>
              <p className="font-semibold tabular-nums">
                {!routeCalculated ? "Ruta no calculada" : Number(plan.km_total ?? 0).toFixed(1)}
              </p>
              {routeCoverage === "partial" ? (
                <p className="text-[11px] text-amber-700 dark:text-amber-300">Solo paradas con geo</p>
              ) : null}
            </div>
            <div className="rounded-md border px-3 py-2">
              <p className="text-[11px] text-muted-foreground">Duración</p>
              <p className="font-semibold tabular-nums">
                {!routeCalculated || plan.duration_min == null
                  ? "Ruta no calculada"
                  : `${Math.round(Number(plan.duration_min))} min`}
              </p>
            </div>
            <div className="rounded-md border px-3 py-2">
              <p className="text-[11px] text-muted-foreground">Costo ruta</p>
              <p className="font-semibold tabular-nums">
                {!routeCalculated
                  ? "Ruta no calculada"
                  : formatClp(plan.total_route_cost_clp ?? 0)}
              </p>
            </div>
            <div className="rounded-md border px-3 py-2">
              <p className="text-[11px] text-muted-foreground">Después del transporte</p>
              <p className="font-semibold tabular-nums">
                {margin?.net_operational_clp == null
                  ? "No disponible"
                  : formatClp(margin.net_operational_clp)}
              </p>
              <p className="text-[11px] text-muted-foreground">
                Requiere margen comercial y ruta ORS completa
              </p>
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            <Button asChild variant="secondary" size="sm">
              <Link href={`/distribuidora/planificaciones/${planId}/mapa`}>
                Abrir mapa de entregas
              </Link>
            </Button>
          </div>
          {costs?.clients_pending != null && Number(costs.clients_pending) > 0 ? (
            <p className="text-xs text-amber-800 dark:text-amber-200">
              Clientes sin georreferencia: {costs.clients_pending}
              {costs.clients_total != null ? ` de ${costs.clients_total}` : ""}
            </p>
          ) : null}
        </TabsContent>

        <TabsContent value="picking" className="mt-4 space-y-6">
          <DispatchPlanPickingClientePanel
            planId={planId}
            pickingReady={pickingReady}
            blockedMessage={pickingBlockedMessage}
            data={pickingClient}
            loading={pickingClientLoading}
            onGenerate={(opts) => void generatePicking(opts)}
            onRefresh={() => void refreshPickingFromDb()}
            onMessage={onMessage}
          />
          <DispatchPlanPickingProductoPanel
            planId={planId}
            pickingReady={pickingReady}
            blockedMessage={pickingBlockedMessage}
            data={pickingProduct}
            loading={pickingProductLoading}
            onRefresh={() => void refreshPickingFromDb()}
            onMessage={onMessage}
          />
          <DispatchPlanPickingAssignmentPanel
            planId={planId}
            planStatus={dashboard.plan.status}
            planningCode={dashboard.plan.planning_code}
            onMessage={onMessage}
            onReloadDashboard={onReloadDashboard}
          />
        </TabsContent>

        <TabsContent value="chofer" className="mt-4 space-y-4">
          <DispatchPlanDriverAssign
            planId={planId}
            planningOrigin={plan.planning_origin}
            onAssigned={() => {
              void onReloadDashboard()
            }}
          />
          <p className="text-xs text-muted-foreground">
            La cuadratura y el cierre de ruta están en la pestaña «Cierre de ruta y cuadratura».
          </p>
        </TabsContent>

        <TabsContent value="cierre" className="mt-4 space-y-4">
          <div className="space-y-1">
            <h3 className="text-sm font-semibold">Cierre de ruta y cuadratura</h3>
            <p className="text-xs text-muted-foreground">
              Datos del cierre de ruta. No sustituyen la venta OC ni la venta facturada del
              resumen comercial.
            </p>
          </div>
          <DispatchPlanCuadraturaPanel planId={planId} onMessage={onMessage} />
        </TabsContent>
      </Tabs>
    </div>
  )
}
