"use client"

/**
 * Detalle de despacho — cabecera compacta + 5 pestañas grandes.
 * Solo reorganiza presentación; reutiliza paneles y acciones existentes.
 */

import Link from "next/link"
import { useSearchParams } from "next/navigation"
import { useCallback, useEffect, useState } from "react"
import {
  ArrowLeft,
  LayoutDashboard,
  Loader2,
  Map,
  PackageCheck,
  RefreshCw,
  Truck,
  Wallet,
  Wrench,
} from "lucide-react"

import {
  DISPATCH_PLAN_PICKING_NO_CONFIRMED_MESSAGE,
  DISPATCH_PLAN_PICKING_WAIT_MESSAGE,
  downloadDispatchPlanBillingExcel,
  generateDispatchPlanPicking,
  getDispatchPlanInvoicedDocuments,
  getDispatchPlanPickingCliente,
  getDispatchPlanPickingProducto,
  repairDispatchPlanSnapshot,
  type DispatchPlanDashboard,
  type DispatchPlanInvoicedRow,
  type DispatchPlanPickingClientResponse,
  type DispatchPlanPickingProductResponse,
} from "@/lib/api"
import { DispatchPlanInvoicingDashboard } from "@/components/distribuidora/planificacion/DispatchPlanInvoicingDashboard"
import { DispatchPlanCuadraturaPanel } from "@/components/distribuidora/planificacion/DispatchPlanCuadraturaPanel"
import { DispatchPlanPickingAssignmentPanel } from "@/components/distribuidora/planificacion/DispatchPlanPickingAssignmentPanel"
import { DispatchPlanLoadOrganizationPanel } from "@/components/distribuidora/planificacion/DispatchPlanLoadOrganizationPanel"
import { DispatchPlanInvoicedItemsTable } from "@/components/distribuidora/planificacion/DispatchPlanInvoicedItemsTable"
import {
  DispatchPlanPickingClientePanel,
  DispatchPlanPickingProductoPanel,
} from "@/components/distribuidora/planificacion/DispatchPlanPickingPanel"
import { DispatchPlanDriverAssign } from "@/components/distribuidora/planificacion/DispatchPlanDriverAssign"
import { DispatchPlanOrdersWeightTable } from "@/components/distribuidora/planificacion/DispatchPlanOrdersWeightTable"
import { formatClp } from "@/lib/ors-map-ui"
import { formatPeonetasDisplay } from "@/lib/peonetas-display"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { cn } from "@/lib/utils"

type Props = {
  planId: number
  dashboard: DispatchPlanDashboard
  dashboardLoading: boolean
  onReloadDashboard: (opts?: { include_margin?: boolean }) => Promise<void>
  onRefreshCommercial: () => Promise<void>
  onMessage: (msg: string) => void
  refreshBusy?: boolean
}

function KpiCard({
  label,
  value,
  sub,
}: {
  label: string
  value: string
  sub?: string
}) {
  return (
    <div className="rounded-lg border border-border/70 bg-card/80 px-4 py-3.5">
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className="mt-1 text-lg font-semibold tabular-nums leading-tight sm:text-xl">{value}</p>
      {sub ? <p className="mt-1 text-[12px] text-muted-foreground">{sub}</p> : null}
    </div>
  )
}

function CompactStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0 rounded-md border border-border/60 bg-muted/15 px-3 py-2.5">
      <p className="text-[11px] text-muted-foreground">{label}</p>
      <p className="truncate text-sm font-medium tabular-nums">{value}</p>
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

  const searchParams = useSearchParams()
  const [tab, setTab] = useState("resumen")
  /** Montaje diferido: evita API/carga pesada hasta la 1ª visita; luego se conserva el estado. */
  const [visitedTabs, setVisitedTabs] = useState<Record<string, boolean>>({
    resumen: true,
  })
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

  const onTabChange = useCallback(
    (value: string) => {
      setTab(value)
      setVisitedTabs((prev) => (prev[value] ? prev : { ...prev, [value]: true }))
      if ((value === "picking" || value === "picking-cliente") && !pickingClientLoaded) {
        void refreshPickingFromDb().catch(() => undefined)
      }
    },
    [pickingClientLoaded, refreshPickingFromDb],
  )

  useEffect(() => {
    const raw = (searchParams.get("tab") || "").trim().toLowerCase()
    const allowed = new Set(["resumen", "ruta", "picking", "chofer", "cierre"])
    if (raw && allowed.has(raw)) {
      onTabChange(raw)
    }
    const panel = (searchParams.get("panel") || "").trim().toLowerCase()
    if (panel === "organizar" || raw === "picking") {
      const t = window.setTimeout(() => {
        document.getElementById("organizar-carga")?.scrollIntoView({
          behavior: "smooth",
          block: "start",
        })
      }, 350)
      return () => window.clearTimeout(t)
    }
    return undefined
  }, [searchParams, onTabChange])

  const tabPanelClass =
    "mt-4 space-y-4 focus-visible:outline-none data-[state=inactive]:hidden"

  const navCardClass = cn(
    "h-[68px] min-w-[120px] flex-1 flex-col gap-1 rounded-xl border px-2 py-2",
    "bg-card text-muted-foreground shadow-none",
    "border-border/70 hover:border-border hover:bg-muted/40",
    "transition-[background-color,border-color,color] duration-150",
    "focus-visible:ring-[3px] focus-visible:ring-ring/50 focus-visible:outline-none",
    "data-[state=active]:border-primary data-[state=active]:bg-primary/15",
    "data-[state=active]:text-white data-[state=active]:shadow-none",
    "dark:data-[state=active]:border-primary dark:data-[state=active]:bg-primary/20",
    "dark:data-[state=active]:text-white",
    "[&_svg]:size-5 [&_svg]:text-muted-foreground",
    "data-[state=active]:[&_svg]:text-red-300",
  )

  const routeCalculated = plan.ors_metrics_calculated !== false
  const routeCoverage = costs?.route_coverage
  const ocCount = inv.total_orders
  const clients = ls?.kpis?.clients_from_oc ?? ls?.kpis?.clients ?? "—"
  const weightKg = ls?.weight?.weight_total_kg ?? plan.weight_total_kg
  const cobertura = ls?.weight?.cobertura_pct ?? plan.weight_cobertura_pct
  const pickingSales = ls?.kpis?.picking_sales_clp
  const pickingStatusLabel = ls?.picking?.has_snapshot
    ? `Picking generado${ls.picking.version != null ? ` v${ls.picking.version}` : ""}`
    : "Picking pendiente"
  const cuadraturaLabel =
    dashboard.cuadratura?.label ||
    (dashboard.cuadratura?.started ? "Cuadratura iniciada" : "Cuadratura no iniciada")
  const amountsBasis = inv.amounts_basis_label_es || "Total documento (con IVA)"
  const planCode = plan.planning_code ?? `PLAN-${plan.id}`
  const planTitle = plan.planning_name ?? plan.route_name ?? "Despacho"
  const planDate = plan.planning_date?.slice(0, 10) ?? plan.created_at?.slice(0, 10) ?? "—"
  const driverLabel =
    plan.driver_name ||
    (plan.driver_user_id ? `Chofer #${plan.driver_user_id}` : "Sin chofer")
  const peonetasLabel = formatPeonetasDisplay({
    assistant_count: plan.assistant_count,
    peonetas_recorded: plan.peonetas_recorded,
  })
  const truckLabel = plan.truck_name || "Sin camión"
  const invoicingStatus = `${inv.confirmed.count} fact. · ${inv.probable.count} prob. · ${inv.pending.count} pend.`

  return (
    <div className="space-y-3 sm:space-y-4">
      {/* Cabecera compacta (~90–115px) */}
      <header className="space-y-1.5">
        <Button asChild variant="ghost" size="sm" className="-ml-2 h-8 px-2 text-xs">
          <Link href="/distribuidora/planificaciones">
            <ArrowLeft className="mr-1 size-3.5" aria-hidden />
            Volver a Planificaciones
          </Link>
        </Button>

        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0 flex-1 space-y-0.5">
            <h1 className="break-words text-xl font-semibold leading-snug tracking-tight sm:text-2xl">
              <span className="font-mono text-[0.85em] text-muted-foreground">{planCode}</span>
              <span className="text-muted-foreground"> · </span>
              <span>{planTitle}</span>
            </h1>
            <p className="text-sm text-muted-foreground">
              {truckLabel} · {planDate} · Chofer: {driverLabel} · Peonetas:{" "}
              {peonetasLabel}
            </p>
          </div>
          <div className="flex flex-wrap gap-1.5">
            <Badge variant={ls?.picking?.has_snapshot ? "outline" : "secondary"}>
              {pickingStatusLabel}
            </Badge>
            <Badge variant={dashboard.cuadratura?.started ? "outline" : "secondary"}>
              {cuadraturaLabel}
            </Badge>
          </div>
        </div>
      </header>

      <Tabs value={tab} onValueChange={onTabChange} className="w-full gap-0">
        <div className="overflow-x-auto overscroll-x-contain [-ms-overflow-style:none] [scrollbar-width:none] [&::-webkit-scrollbar]:hidden">
          <TabsList
            className={cn(
              "flex h-auto w-full min-w-max gap-2.5 bg-transparent p-0",
              "rounded-none border-0 shadow-none sm:min-w-0 sm:gap-3",
            )}
          >
            <TabsTrigger value="resumen" className={navCardClass}>
              <LayoutDashboard aria-hidden />
              <span className="text-sm font-medium leading-tight">Resumen</span>
            </TabsTrigger>
            <TabsTrigger value="ruta" className={navCardClass}>
              <Map aria-hidden />
              <span className="text-sm font-medium leading-tight">Ruta</span>
            </TabsTrigger>
            <TabsTrigger value="picking" className={navCardClass}>
              <PackageCheck aria-hidden />
              <span className="text-sm font-medium leading-tight">Picking</span>
            </TabsTrigger>
            <TabsTrigger value="chofer" className={navCardClass}>
              <Truck aria-hidden />
              <span className="text-sm font-medium leading-tight">Chofer</span>
            </TabsTrigger>
            <TabsTrigger value="cierre" className={navCardClass}>
              <Wallet aria-hidden />
              <span className="text-sm font-medium leading-tight">Cuadratura</span>
            </TabsTrigger>
          </TabsList>
        </div>

        {/* ——— RESUMEN ——— */}
        <TabsContent value="resumen" forceMount className={tabPanelClass}>
          {dashboardLoading ? (
            <div className="flex items-center gap-2 text-sm text-muted-foreground">
              <Loader2 className="size-4 animate-spin" /> Actualizando…
            </div>
          ) : null}

          <section className="space-y-3">
            <h2 className="text-base font-semibold sm:text-lg">Indicadores comerciales</h2>
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
              <KpiCard
                label="Venta OC"
                value={formatClp(inv.total_oc_amount_clp)}
                sub={`${ocCount} órdenes · ${amountsBasis}`}
              />
              <KpiCard
                label="Venta facturada real"
                value={formatClp(inv.confirmed.amount_clp)}
                sub={`${inv.confirmed.count} docs · ${amountsBasis}`}
              />
              <KpiCard
                label="Venta picking"
                value={pickingSales != null ? formatClp(pickingSales) : "—"}
                sub={pickingStatusLabel}
              />
              <KpiCard
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
                    ? `Neto · ${margin.cost_basis}`
                    : margin?.partial
                      ? "Parcial / incompleto"
                      : "No descuenta NC autom."
                }
              />
            </div>
            {costs?.note ? (
              <Alert className="border-amber-500/30 bg-amber-500/5">
                <AlertTitle className="text-sm">Ruta / transporte</AlertTitle>
                <AlertDescription className="text-xs">{costs.note}</AlertDescription>
              </Alert>
            ) : null}
            {margin?.visible && margin.calculated_at ? (
              <p className="text-[12px] text-muted-foreground">
                Margen comercial vivo · calculado {margin.calculated_at}
                {margin.cost_basis ? ` · base costo: ${margin.cost_basis}` : ""}
                {margin.cost_coverage
                  ? ` · costos ${margin.cost_coverage.lines_with_cost ?? "—"}/${margin.cost_coverage.lines_total ?? "—"} líneas`
                  : ""}
                {" · "}no es margen neto definitivo (NC no descontadas automáticamente).
              </p>
            ) : null}
          </section>

          <section className="space-y-3">
            <h2 className="text-base font-semibold sm:text-lg">Estado documental y logístico</h2>
            <div className="grid grid-cols-2 gap-2 lg:grid-cols-4">
              <CompactStat label="OC y clientes" value={`${ocCount} · ${clients}`} />
              <CompactStat
                label="Peso / cobertura"
                value={
                  weightKg != null
                    ? `${Number(weightKg).toLocaleString("es-CL", { maximumFractionDigits: 1 })} kg${
                        cobertura != null
                          ? ` · ${Number(cobertura).toLocaleString("es-CL", {
                              maximumFractionDigits: 0,
                            })}%`
                          : ""
                      }`
                    : "—"
                }
              />
              <CompactStat label="Facturación" value={invoicingStatus} />
              <CompactStat label="Picking" value={pickingStatusLabel} />
            </div>
          </section>

          <section className="space-y-3">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <h2 className="text-base font-semibold sm:text-lg">Órdenes y documentos</h2>
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
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
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
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  disabled={!!busy}
                  onClick={() =>
                    void (async () => {
                      setBusy("excel")
                      try {
                        await downloadDispatchPlanBillingExcel(planId)
                        onMessage("Excel descargado desde snapshot congelado.")
                      } catch (e: unknown) {
                        onMessage(e instanceof Error ? e.message : "Error Excel")
                      } finally {
                        setBusy(null)
                      }
                    })()
                  }
                >
                  Excel de facturación
                </Button>
              </div>
            </div>
            <DispatchPlanInvoicingDashboard data={dashboard} />
            {reviewItems?.length ? (
              <div className="space-y-2">
                <p className="text-sm font-medium">Detalle documental</p>
                <DispatchPlanInvoicedItemsTable items={reviewItems} />
              </div>
            ) : null}
            {dashboard.plan_orders?.length ? (
              <div className="space-y-2">
                <p className="text-sm font-medium">Órdenes / peso congelado</p>
                <DispatchPlanOrdersWeightTable orders={dashboard.plan_orders} />
              </div>
            ) : null}
            <div className="border-t border-border/50 pt-3">
              <Button
                type="button"
                variant="ghost"
                size="sm"
                className="h-8 text-xs text-muted-foreground"
                disabled={!!busy}
                onClick={() =>
                  void (async () => {
                    setBusy("repair")
                    try {
                      await repairDispatchPlanSnapshot(planId)
                      await onReloadDashboard()
                      onMessage("Snapshot reparado (solo campos vacíos).")
                    } catch (e: unknown) {
                      onMessage(e instanceof Error ? e.message : "Error reparación")
                    } finally {
                      setBusy(null)
                    }
                  })()
                }
              >
                <Wrench className="mr-1 size-3.5" aria-hidden />
                Reparar snapshot (administrativo)
              </Button>
            </div>
          </section>
        </TabsContent>

        {/* ——— RUTA ——— */}
        <TabsContent
          value="ruta"
          forceMount={Boolean(visitedTabs.ruta)}
          className={tabPanelClass}
        >
          {visitedTabs.ruta ? (
            <>
              <h2 className="text-base font-semibold sm:text-lg">Ruta y costos</h2>
              <p className="text-xs text-muted-foreground">
                {truckLabel} · {planDate} · {driverLabel}
              </p>

              <section className="space-y-3 rounded-lg border border-border/70 bg-card/50 p-4">
                <h3 className="text-sm font-semibold">Mapa y cobertura</h3>
                <div className="flex flex-wrap items-center gap-3">
                  <Button asChild variant="secondary" size="sm">
                    <Link href={`/distribuidora/planificaciones/${planId}/mapa`}>
                      Abrir mapa de entregas
                    </Link>
                  </Button>
                  {costs?.clients_pending != null && Number(costs.clients_pending) > 0 ? (
                    <p className="text-sm text-amber-800 dark:text-amber-200">
                      Clientes sin georreferencia: {costs.clients_pending}
                      {costs.clients_total != null ? ` de ${costs.clients_total}` : ""}
                    </p>
                  ) : (
                    <p className="text-sm text-muted-foreground">
                      Cobertura geo disponible según ORS.
                    </p>
                  )}
                </div>
              </section>

              <section className="space-y-3">
                <h3 className="text-sm font-semibold">Métricas de ruta</h3>
                {!routeCalculated ? (
                  <Alert className="border-amber-500/30 bg-amber-500/5">
                    <AlertTitle className="text-sm">Ruta no calculada</AlertTitle>
                    <AlertDescription className="text-sm">
                      Este despacho no tiene métricas ORS. No se inventan km, duración ni costo de
                      transporte.
                    </AlertDescription>
                  </Alert>
                ) : null}
                <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                  <KpiCard
                    label="Kilómetros"
                    value={
                      !routeCalculated
                        ? "Ruta no calculada"
                        : Number(plan.km_total ?? 0).toFixed(1)
                    }
                    sub={
                      routeCoverage === "partial" ? "Solo paradas con geo" : undefined
                    }
                  />
                  <KpiCard
                    label="Duración"
                    value={
                      !routeCalculated || plan.duration_min == null
                        ? "Ruta no calculada"
                        : `${Math.round(Number(plan.duration_min))} min`
                    }
                  />
                  <KpiCard
                    label="Costo de transporte"
                    value={
                      !routeCalculated
                        ? "Ruta no calculada"
                        : formatClp(plan.total_route_cost_clp ?? 0)
                    }
                  />
                  <KpiCard
                    label="Después del transporte"
                    value={
                      margin?.net_operational_clp == null
                        ? "No disponible"
                        : formatClp(margin.net_operational_clp)
                    }
                    sub="Requiere margen comercial y ruta ORS completa"
                  />
                </div>
              </section>
            </>
          ) : null}
        </TabsContent>

        {/* ——— PICKING ——— */}
        <TabsContent
          value="picking"
          forceMount={Boolean(visitedTabs.picking)}
          className={cn(tabPanelClass, "space-y-5")}
        >
          {visitedTabs.picking ? (
            <>
              <h2 className="text-base font-semibold sm:text-lg">Picking y carga</h2>
              <p className="text-xs text-muted-foreground">
                {truckLabel} · {pickingStatusLabel}
              </p>

              <div className="grid gap-4 lg:grid-cols-2">
                <section className="space-y-2 rounded-lg border border-border/70 bg-card/50 p-4">
                  <h3 className="text-sm font-semibold">Picking clientes</h3>
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
                </section>
                <section className="space-y-2 rounded-lg border border-border/70 bg-card/50 p-4">
                  <h3 className="text-sm font-semibold">Picking productos</h3>
                  <DispatchPlanPickingProductoPanel
                    planId={planId}
                    pickingReady={pickingReady}
                    blockedMessage={pickingBlockedMessage}
                    data={pickingProduct}
                    loading={pickingProductLoading}
                    onRefresh={() => void refreshPickingFromDb()}
                    onMessage={onMessage}
                  />
                </section>
              </div>

              <section
                id="organizar-carga"
                className="scroll-mt-4 space-y-2 rounded-lg border border-border/70 bg-card/40 p-4"
              >
                <DispatchPlanLoadOrganizationPanel
                  planId={planId}
                  onMessage={onMessage}
                />
              </section>

              <section className="space-y-2 rounded-lg border border-border/70 bg-card/40 p-4">
                <h3 className="text-sm font-semibold">Asignación documental (post-factura)</h3>
                <p className="text-xs text-muted-foreground">
                  Asigna boletas/facturas ya emitidas a grupos de picking. La organización
                  anticipada por OC está arriba.
                </p>
                <DispatchPlanPickingAssignmentPanel
                  planId={planId}
                  planStatus={dashboard.plan.status}
                  planningCode={dashboard.plan.planning_code}
                  onMessage={onMessage}
                  onReloadDashboard={onReloadDashboard}
                />
              </section>
            </>
          ) : null}
        </TabsContent>

        {/* ——— CHOFER ——— */}
        <TabsContent
          value="chofer"
          forceMount={Boolean(visitedTabs.chofer)}
          className={tabPanelClass}
        >
          {visitedTabs.chofer ? (
            <>
              <h2 className="text-base font-semibold sm:text-lg">Chofer y auditoría</h2>
              <p className="text-xs text-muted-foreground">
                {planCode} · {truckLabel} · {planDate}
              </p>
              <DispatchPlanDriverAssign
                planId={planId}
                planningOrigin={plan.planning_origin}
                onAssigned={() => {
                  void onReloadDashboard()
                }}
              />
            </>
          ) : null}
        </TabsContent>

        {/* ——— CIERRE ——— */}
        <TabsContent
          value="cierre"
          forceMount={Boolean(visitedTabs.cierre)}
          className={tabPanelClass}
        >
          {visitedTabs.cierre ? (
            <DispatchPlanCuadraturaPanel
              planId={planId}
              onMessage={onMessage}
              contextLine={{
                driver: driverLabel,
                truck: truckLabel,
                planCode,
              }}
            />
          ) : null}
        </TabsContent>
      </Tabs>
    </div>
  )
}
