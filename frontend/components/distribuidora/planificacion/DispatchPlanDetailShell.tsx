"use client"

/**
 * Detalle de despacho — navegación por etapas operativas.
 * Solo reorganiza presentación; reutiliza paneles y acciones existentes.
 */

import Link from "next/link"
import { useSearchParams } from "next/navigation"
import { useCallback, useEffect, useMemo, useState } from "react"
import {
  ArrowLeft,
  ClipboardList,
  LayoutDashboard,
  Loader2,
  Map,
  PackageCheck,
  PlusCircle,
  RefreshCw,
  Route,
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
  type DocumentLoadReconciliationResponse,
} from "@/lib/api"
import { DispatchPlanInvoicingDashboard } from "@/components/distribuidora/planificacion/DispatchPlanInvoicingDashboard"
import { DispatchPlanCuadraturaPanel } from "@/components/distribuidora/planificacion/DispatchPlanCuadraturaPanel"
import { DispatchPlanPickingAssignmentPanel } from "@/components/distribuidora/planificacion/DispatchPlanPickingAssignmentPanel"
import { DispatchPlanLoadOrganizationPanel } from "@/components/distribuidora/planificacion/DispatchPlanLoadOrganizationPanel"
import { DocumentLoadReassignmentDialog } from "@/components/distribuidora/planificacion/DocumentLoadReassignmentDialog"
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

type StageId =
  | "resumen"
  | "organizar"
  | "picking"
  | "ruta"
  | "oc-adicionales"
  | "chofer"
  | "cierre"

type StageVisualStatus =
  | "pendiente"
  | "completo"
  | "revision"
  | "conflicto"
  | "soporte"

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

function stageStatusBadge(status: StageVisualStatus) {
  switch (status) {
    case "completo":
      return (
        <span className="rounded bg-emerald-500/15 px-1.5 py-0.5 text-[10px] font-medium text-emerald-800 dark:text-emerald-200">
          Completo
        </span>
      )
    case "revision":
      return (
        <span className="rounded bg-amber-500/15 px-1.5 py-0.5 text-[10px] font-medium text-amber-900 dark:text-amber-100">
          Requiere revisión
        </span>
      )
    case "conflicto":
      return (
        <span className="rounded bg-red-500/15 px-1.5 py-0.5 text-[10px] font-medium text-red-800 dark:text-red-200">
          Conflicto documental
        </span>
      )
    case "soporte":
      return null
    default:
      return (
        <span className="rounded bg-muted px-1.5 py-0.5 text-[10px] font-medium text-muted-foreground">
          Pendiente
        </span>
      )
  }
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
  const [tab, setTab] = useState<StageId>("resumen")
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
  const [orgRefreshToken, setOrgRefreshToken] = useState(0)
  const [docConflictBanner, setDocConflictBanner] = useState<{
    count: number
    fromBatchIds: number[]
    toBatchIds: number[]
    movedOcDocumentIds: number[]
    relatedDocumentIds: number[]
    hasPicking: boolean
    pickingVersion: number | null
  } | null>(null)
  const [reassignmentOpen, setReassignmentOpen] = useState(false)

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
          totals: undefined,
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
      const next = value as StageId
      setTab(next)
      setVisitedTabs((prev) => (prev[next] ? prev : { ...prev, [next]: true }))
      if ((next === "picking" || next === "organizar") && !pickingClientLoaded) {
        void refreshPickingFromDb().catch(() => undefined)
      }
    },
    [pickingClientLoaded, refreshPickingFromDb],
  )

  useEffect(() => {
    const raw = (searchParams.get("tab") || "").trim().toLowerCase()
    const panel = (searchParams.get("panel") || "").trim().toLowerCase()
    const alias: Record<string, StageId> = {
      resumen: "resumen",
      ruta: "ruta",
      picking: "picking",
      chofer: "chofer",
      cierre: "cierre",
      organizar: "organizar",
      "oc-adicionales": "oc-adicionales",
      oc: "oc-adicionales",
    }
    if (panel === "organizar") {
      onTabChange("organizar")
      return
    }
    if (raw && alias[raw]) {
      onTabChange(alias[raw])
    }
  }, [searchParams, onTabChange])

  const handleDocumentConflicts = useCallback(
    (payload: {
      conflicts: DocumentLoadReconciliationResponse["conflicts"]
      recon: DocumentLoadReconciliationResponse
      movedOcDocumentIds: number[]
    }) => {
      const conflictRows = payload.conflicts || []
      const fromBatchIds = [
        ...new Set(
          conflictRows
            .map((c) => c.current_load_batch_id)
            .filter((n): n is number => n != null),
        ),
      ]
      const toBatchIds = [
        ...new Set(
          conflictRows
            .map((c) => c.suggested_load_batch_id)
            .filter((n): n is number => n != null),
        ),
      ]
      const relatedDocumentIds = [
        ...new Set(conflictRows.map((c) => Number(c.related_document_id))),
      ]
      setDocConflictBanner({
        count: relatedDocumentIds.length,
        fromBatchIds,
        toBatchIds,
        movedOcDocumentIds: payload.movedOcDocumentIds,
        relatedDocumentIds,
        hasPicking: Boolean(payload.recon.has_picking),
        pickingVersion: payload.recon.picking_version ?? null,
      })
    },
    [],
  )

  const tabPanelClass =
    "mt-4 space-y-4 focus-visible:outline-none data-[state=inactive]:hidden"

  const navCardClass = cn(
    "h-auto min-h-[68px] min-w-[108px] flex-1 flex-col gap-1 rounded-xl border px-2 py-2",
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
  const planStatusLabel = String(plan.status || "—")

  const stageStatuses = useMemo(() => {
    const orgStatus = String(
      (ls as { load_organization_status?: string } | null | undefined)
        ?.load_organization_status || "",
    )
    const orgComplete = orgStatus === "organized"
    const pickingComplete = Boolean(ls?.picking?.has_snapshot)
    const hasDocConflict = Boolean(docConflictBanner?.count)
    const routeOk = routeCalculated
    return {
      resumen: "soporte" as StageVisualStatus,
      organizar: hasDocConflict
        ? ("conflicto" as StageVisualStatus)
        : orgComplete
          ? ("completo" as StageVisualStatus)
          : ("pendiente" as StageVisualStatus),
      picking: hasDocConflict
        ? ("conflicto" as StageVisualStatus)
        : pickingComplete
          ? ("completo" as StageVisualStatus)
          : ("pendiente" as StageVisualStatus),
      ruta: routeOk ? ("completo" as StageVisualStatus) : ("pendiente" as StageVisualStatus),
      "oc-adicionales": "soporte" as StageVisualStatus,
      chofer: plan.driver_user_id ? ("completo" as StageVisualStatus) : ("pendiente" as StageVisualStatus),
      cierre: dashboard.cuadratura?.started
        ? ("completo" as StageVisualStatus)
        : ("pendiente" as StageVisualStatus),
    }
  }, [ls, docConflictBanner, routeCalculated, plan.driver_user_id, dashboard.cuadratura?.started])

  const primaryStages: { id: StageId; label: string; icon: typeof LayoutDashboard }[] = [
    { id: "resumen", label: "Resumen", icon: LayoutDashboard },
    { id: "organizar", label: "Organizar carga", icon: ClipboardList },
    { id: "picking", label: "Picking y documentos", icon: PackageCheck },
    { id: "ruta", label: "Orden de ruta", icon: Route },
    { id: "oc-adicionales", label: "OC adicionales", icon: PlusCircle },
  ]
  const supportStages: { id: StageId; label: string; icon: typeof Truck }[] = [
    { id: "chofer", label: "Chofer", icon: Truck },
    { id: "cierre", label: "Cuadratura", icon: Wallet },
  ]

  return (
    <div className="space-y-3 sm:space-y-4">
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
            <Badge variant="outline">Estado: {planStatusLabel}</Badge>
            <Badge variant={ls?.picking?.has_snapshot ? "outline" : "secondary"}>
              {pickingStatusLabel}
            </Badge>
            <Badge variant={dashboard.cuadratura?.started ? "outline" : "secondary"}>
              {cuadraturaLabel}
            </Badge>
          </div>
        </div>
      </header>

      {docConflictBanner ? (
        <Alert className="border-amber-500/40 bg-amber-500/5">
          <AlertTitle className="text-sm">
            {docConflictBanner.count} documento
            {docConflictBanner.count === 1 ? "" : "s"} siguen asociados a otro grupo
          </AlertTitle>
          <AlertDescription className="space-y-2 text-xs">
            <p>
              Sus OC fueron movidas
              {docConflictBanner.fromBatchIds.length
                ? ` (lotes ${docConflictBanner.fromBatchIds.join(", ")} → ${docConflictBanner.toBatchIds.join(", ") || "nuevo grupo"})`
                : ""}
              . El sistema no sabe si Bodega ya preparó físicamente la mercadería.
              {docConflictBanner.hasPicking
                ? ` Hay picking generado${docConflictBanner.pickingVersion != null ? ` v${docConflictBanner.pickingVersion}` : ""}; las exportaciones por grupo pueden quedar desactualizadas.`
                : ""}
            </p>
            <Button
              type="button"
              size="sm"
              onClick={() => setReassignmentOpen(true)}
            >
              Revisar traslado de documentos
            </Button>
          </AlertDescription>
        </Alert>
      ) : null}

      <Tabs value={tab} onValueChange={onTabChange} className="w-full gap-0">
        <div className="sticky top-0 z-20 -mx-1 border-b border-border/60 bg-background/95 px-1 py-2 backdrop-blur supports-[backdrop-filter]:bg-background/80">
          <div className="overflow-x-auto overscroll-x-contain [-ms-overflow-style:none] [scrollbar-width:none] [&::-webkit-scrollbar]:hidden">
            <TabsList
              className={cn(
                "flex h-auto w-full min-w-max gap-2 bg-transparent p-0",
                "rounded-none border-0 shadow-none sm:min-w-0 sm:gap-2.5",
              )}
            >
              {primaryStages.map((s) => {
                const Icon = s.icon
                return (
                  <TabsTrigger key={s.id} value={s.id} className={navCardClass}>
                    <Icon aria-hidden />
                    <span className="text-sm font-medium leading-tight">{s.label}</span>
                    {stageStatusBadge(stageStatuses[s.id])}
                  </TabsTrigger>
                )
              })}
              {supportStages.map((s) => {
                const Icon = s.icon
                return (
                  <TabsTrigger
                    key={s.id}
                    value={s.id}
                    className={cn(navCardClass, "opacity-90")}
                  >
                    <Icon aria-hidden />
                    <span className="text-sm font-medium leading-tight">{s.label}</span>
                    {stageStatusBadge(stageStatuses[s.id])}
                  </TabsTrigger>
                )
              })}
            </TabsList>
          </div>
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

        {/* ——— ORGANIZAR CARGA ——— */}
        <TabsContent
          value="organizar"
          forceMount={visitedTabs.organizar ? true : undefined}
          className={tabPanelClass}
        >
          {visitedTabs.organizar ? (
            <>
              <div className="space-y-1">
                <h2 className="text-base font-semibold sm:text-lg">Organizar carga</h2>
                <p className="text-xs text-muted-foreground">
                  Asigne OC a grupos. Acción principal: guardar distribución. Los PDF de
                  picking por grupo están en la etapa «Picking y documentos».
                </p>
              </div>
              <DispatchPlanLoadOrganizationPanel
                planId={planId}
                onMessage={onMessage}
                refreshToken={orgRefreshToken}
                showOrganizationControls={true}
                showGroupPickingExports={false}
                onDocumentConflicts={handleDocumentConflicts}
              />
            </>
          ) : null}
        </TabsContent>

        {/* ——— PICKING Y DOCUMENTOS ——— */}
        <TabsContent
          value="picking"
          forceMount={visitedTabs.picking ? true : undefined}
          className={cn(tabPanelClass, "space-y-5")}
        >
          {visitedTabs.picking ? (
            <>
              <div className="space-y-1">
                <h2 className="text-base font-semibold sm:text-lg">Picking y documentos</h2>
                <p className="text-xs text-muted-foreground">
                  {truckLabel} · {pickingStatusLabel}. Acción principal: generar o
                  actualizar picking. Exports por grupo debajo.
                </p>
              </div>

              <div className="grid gap-4 lg:grid-cols-2">
                <section className="space-y-2 rounded-lg border border-border/70 bg-card/50 p-4">
                  <h3 className="text-sm font-semibold">Picking clientes</h3>
                  <DispatchPlanPickingClientePanel
                    planId={planId}
                    pickingReady={pickingReady}
                    blockedMessage={pickingBlockedMessage}
                    data={pickingClient}
                    loading={pickingClientLoading}
                    onGenerate={async (opts) => {
                      await generatePicking(opts)
                    }}
                    onRefresh={async () => {
                      await refreshPickingFromDb()
                    }}
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
                    onRefresh={async () => {
                      await refreshPickingFromDb()
                    }}
                    onMessage={onMessage}
                  />
                </section>
              </div>

              <section className="space-y-2 rounded-lg border border-border/70 bg-card/40 p-4">
                <DispatchPlanLoadOrganizationPanel
                  planId={planId}
                  onMessage={onMessage}
                  refreshToken={orgRefreshToken}
                  showOrganizationControls={false}
                  showGroupPickingExports={true}
                />
              </section>
            </>
          ) : null}
        </TabsContent>

        {/* ——— ORDEN DE RUTA ——— */}
        <TabsContent
          value="ruta"
          forceMount={visitedTabs.ruta ? true : undefined}
          className={tabPanelClass}
        >
          {visitedTabs.ruta ? (
            <>
              <h2 className="text-base font-semibold sm:text-lg">Orden de ruta</h2>
              <p className="text-xs text-muted-foreground">
                {truckLabel} · {planDate} · {driverLabel}
              </p>

              <section className="space-y-3 rounded-lg border border-border/70 bg-card/50 p-4">
                <h3 className="text-sm font-semibold">Mapa y cobertura</h3>
                <div className="flex flex-wrap items-center gap-3">
                  <Button asChild size="sm">
                    <Link href={`/distribuidora/planificaciones/${planId}/mapa`}>
                      <Map className="mr-1.5 size-3.5" aria-hidden />
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

              {dashboard.plan_orders?.length ? (
                <section className="space-y-2">
                  <h3 className="text-sm font-semibold">Secuencia / peso congelado</h3>
                  <DispatchPlanOrdersWeightTable orders={dashboard.plan_orders} />
                </section>
              ) : null}
            </>
          ) : null}
        </TabsContent>

        {/* ——— OC ADICIONALES ——— */}
        <TabsContent
          value="oc-adicionales"
          forceMount={visitedTabs["oc-adicionales"] ? true : undefined}
          className={tabPanelClass}
        >
          {visitedTabs["oc-adicionales"] ? (
            <>
              <div className="space-y-1">
                <h2 className="text-base font-semibold sm:text-lg">OC adicionales</h2>
                <p className="text-xs text-muted-foreground">
                  Incorporar OC al plan, historial y asignación documental avanzada.
                </p>
              </div>
              <DispatchPlanPickingAssignmentPanel
                planId={planId}
                planStatus={dashboard.plan.status}
                planningCode={dashboard.plan.planning_code}
                onMessage={onMessage}
                onReloadDashboard={onReloadDashboard}
                onSupplementalOrdersChanged={async () => {
                  setOrgRefreshToken((n) => n + 1)
                  try {
                    await refreshPickingFromDb()
                  } catch {
                    /* loadAll del panel ya avisó; picking puede quedar pendiente */
                  }
                }}
              />
            </>
          ) : null}
        </TabsContent>

        {/* ——— CHOFER ——— */}
        <TabsContent
          value="chofer"
          forceMount={visitedTabs.chofer ? true : undefined}
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
          forceMount={visitedTabs.cierre ? true : undefined}
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

      <DocumentLoadReassignmentDialog
        open={reassignmentOpen}
        onOpenChange={setReassignmentOpen}
        planId={planId}
        planStatus={planStatusLabel}
        banner={docConflictBanner}
        onMessage={onMessage}
        onApplied={() => {
          setDocConflictBanner(null)
          setOrgRefreshToken((n) => n + 1)
          void onReloadDashboard()
        }}
      />
    </div>
  )
}
