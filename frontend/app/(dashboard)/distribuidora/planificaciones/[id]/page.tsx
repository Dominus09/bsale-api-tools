"use client"

import { useEffect, useRef, useState } from "react"
import Link from "next/link"
import { useParams } from "next/navigation"
import { ArrowLeft, Loader2, Wrench } from "lucide-react"

import {
  downloadDispatchPlanBillingExcel,
  getDispatchPlanInvoicedDocuments,
  getDispatchPlanPickingByClient,
  getDispatchPlanPickingByProduct,
  markDispatchPlanPickingGenerated,
  repairDispatchPlanSnapshot,
  type DispatchPlanDashboard,
  type DispatchPlanSummary,
} from "@/lib/api"
import {
  fetchDispatchPlanDashboardDeduped,
  fetchDispatchPlanHeaderDeduped,
  invalidateDashboardCache,
  logFrontendPlanDebug,
  trackPlanPageRender,
} from "@/lib/planificacion-fetch"
import { DispatchPlanInvoicingDashboard } from "@/components/distribuidora/planificacion/DispatchPlanInvoicingDashboard"
import { formatClp } from "@/lib/ors-map-ui"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"

export default function PlanificacionDetallePage() {
  const params = useParams()
  const planId = Number(params.id)

  const [plan, setPlan] = useState<DispatchPlanSummary | null>(null)
  const [dashboard, setDashboard] = useState<DispatchPlanDashboard | null>(null)
  const [headerLoading, setHeaderLoading] = useState(true)
  const [dashboardLoading, setDashboardLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState<string | null>(null)
  const [msg, setMsg] = useState<string | null>(null)

  const abortRef = useRef<AbortController | null>(null)
  const loadedForRef = useRef<number | null>(null)

  trackPlanPageRender(`plan-detail-${planId}`)

  useEffect(() => {
    if (!Number.isFinite(planId) || planId <= 0) return
    if (loadedForRef.current === planId) return
    loadedForRef.current = planId

    abortRef.current?.abort()
    const ac = new AbortController()
    abortRef.current = ac

    setPlan(null)
    setDashboard(null)
    setHeaderLoading(true)
    setDashboardLoading(false)
    setError(null)

    ;(async () => {
      try {
        logFrontendPlanDebug("plan-detail-flow", {
          planning_id: planId,
          phase: "header",
        })
        const { plan: headerPlan } = await fetchDispatchPlanHeaderDeduped(
          planId,
          ac.signal,
        )
        if (ac.signal.aborted) return
        setPlan(headerPlan)
        setHeaderLoading(false)

        setDashboardLoading(true)
        logFrontendPlanDebug("plan-detail-flow", {
          planning_id: planId,
          phase: "dashboard",
        })
        const dash = await fetchDispatchPlanDashboardDeduped(planId, ac.signal)
        if (ac.signal.aborted) return
        setDashboard(dash)
      } catch (e: unknown) {
        if (ac.signal.aborted) return
        setError(e instanceof Error ? e.message : "Error al cargar plan")
      } finally {
        if (!ac.signal.aborted) {
          setHeaderLoading(false)
          setDashboardLoading(false)
        }
      }
    })()

    return () => {
      ac.abort()
    }
  }, [planId])

  const reloadDashboard = async () => {
    if (!Number.isFinite(planId) || planId <= 0) return
    invalidateDashboardCache(planId)
    setDashboardLoading(true)
    try {
      const dash = await fetchDispatchPlanDashboardDeduped(planId)
      setDashboard(dash)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error")
    } finally {
      setDashboardLoading(false)
    }
  }

  if (!Number.isFinite(planId) || planId <= 0) {
    return <p className="p-6 text-sm text-destructive">ID de plan inválido</p>
  }

  const displayPlan = dashboard?.plan ?? plan

  return (
    <div className="space-y-6 p-6">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <Button asChild variant="ghost" size="sm" className="-ml-2 mb-2 h-8 text-xs">
            <Link href="/distribuidora/planificaciones">
              <ArrowLeft className="mr-1 size-3.5" aria-hidden />
              Planificaciones
            </Link>
          </Button>
          {headerLoading && !displayPlan ? (
            <Loader2 className="size-5 animate-spin text-muted-foreground" />
          ) : displayPlan ? (
            <>
              <p className="font-mono text-xs text-muted-foreground">
                {displayPlan.planning_code ?? `PLAN-${displayPlan.id}`}
              </p>
              <h1 className="text-xl font-semibold">
                {displayPlan.planning_name ?? displayPlan.route_name}
              </h1>
              <p className="text-sm text-muted-foreground">
                {displayPlan.truck_name} ·{" "}
                {displayPlan.planning_date?.slice(0, 10) ?? displayPlan.created_at?.slice(0, 10)}
              </p>
              <Badge variant="secondary" className="mt-2">
                {displayPlan.status}
              </Badge>
            </>
          ) : null}
        </div>
        {displayPlan ? (
          <div className="flex flex-wrap gap-2">
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
                    setMsg("Excel descargado desde snapshot congelado.")
                  } catch (e: unknown) {
                    setMsg(e instanceof Error ? e.message : "Error Excel")
                  } finally {
                    setBusy(null)
                  }
                })()
              }
            >
              Excel facturación
            </Button>
            <Button
              type="button"
              variant="outline"
              size="sm"
              disabled={!!busy}
              onClick={() =>
                void (async () => {
                  setBusy("repair")
                  try {
                    await repairDispatchPlanSnapshot(planId)
                    await reloadDashboard()
                    setMsg("Snapshot reparado (solo campos vacíos).")
                  } catch (e: unknown) {
                    setMsg(e instanceof Error ? e.message : "Error reparación")
                  } finally {
                    setBusy(null)
                  }
                })()
              }
            >
              <Wrench className="mr-1 size-3.5" aria-hidden />
              Reparar snapshot
            </Button>
          </div>
        ) : null}
      </div>

      {error ? <p className="text-sm text-destructive">{error}</p> : null}
      {msg ? (
        <Alert>
          <AlertTitle>Operación</AlertTitle>
          <AlertDescription>{msg}</AlertDescription>
        </Alert>
      ) : null}

      {headerLoading && !displayPlan ? (
        <div className="flex items-center gap-2 text-muted-foreground">
          <Loader2 className="size-4 animate-spin" />
          Cargando plan…
        </div>
      ) : displayPlan ? (
        <>
          <div className="grid gap-3 sm:grid-cols-3 text-sm">
            <div className="rounded-md border px-3 py-2">
              <span className="text-muted-foreground">Km ORS</span>
              <p className="font-semibold tabular-nums">
                {Number(displayPlan.km_total ?? 0).toFixed(1)}
              </p>
            </div>
            <div className="rounded-md border px-3 py-2">
              <span className="text-muted-foreground">Costo ruta</span>
              <p className="font-semibold tabular-nums">
                {formatClp(displayPlan.total_route_cost_clp ?? 0)}
              </p>
            </div>
            <div className="rounded-md border px-3 py-2">
              <span className="text-muted-foreground">Tripulación</span>
              <p className="font-semibold">
                {displayPlan.driver_count ?? 1} chofer / {displayPlan.assistant_count ?? 0}{" "}
                peoneta(s)
              </p>
            </div>
          </div>

          {dashboardLoading && !dashboard ? (
            <div className="flex items-center gap-2 rounded-lg border border-dashed border-border px-4 py-8 text-sm text-muted-foreground">
              <Loader2 className="size-4 animate-spin" aria-hidden />
              Cargando facturación y métricas…
            </div>
          ) : dashboard ? (
            <DispatchPlanInvoicingDashboard data={dashboard} />
          ) : null}

          <div className="flex flex-wrap gap-2 border-t pt-4">
            <Button
              type="button"
              variant="secondary"
              size="sm"
              disabled={!!busy || dashboardLoading}
              onClick={() => void reloadDashboard()}
            >
              Actualizar facturación
            </Button>
            <Button
              type="button"
              variant="secondary"
              size="sm"
              disabled={!!busy || !dashboard?.picking.ready}
              onClick={() =>
                void (async () => {
                  setBusy("inv")
                  try {
                    const r = await getDispatchPlanInvoicedDocuments(planId)
                    setMsg(
                      `Facturación: ${r.summary.confirmed} confirmadas, ${r.summary.probable} probables, ${r.summary.missing} pendientes.`,
                    )
                  } catch (e: unknown) {
                    setMsg(e instanceof Error ? e.message : "Error")
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
              variant="secondary"
              size="sm"
              disabled={!!busy || !dashboard?.picking.ready}
              onClick={() =>
                void (async () => {
                  setBusy("pc")
                  try {
                    const r = await getDispatchPlanPickingByClient(planId)
                    await markDispatchPlanPickingGenerated(planId)
                    setMsg(
                      `Picking cliente: ${(r.clients as unknown[]).length} paradas (docs reales).`,
                    )
                    await reloadDashboard()
                  } catch (e: unknown) {
                    setMsg(e instanceof Error ? e.message : "Error picking")
                  } finally {
                    setBusy(null)
                  }
                })()
              }
            >
              Picking cliente
            </Button>
            <Button
              type="button"
              variant="secondary"
              size="sm"
              disabled={!!busy || !dashboard?.picking.ready}
              onClick={() =>
                void (async () => {
                  setBusy("pp")
                  try {
                    const r = await getDispatchPlanPickingByProduct(planId)
                    setMsg(
                      `Picking producto: ${(r.items as unknown[]).length} líneas consolidadas.`,
                    )
                  } catch (e: unknown) {
                    setMsg(e instanceof Error ? e.message : "Error picking")
                  } finally {
                    setBusy(null)
                  }
                })()
              }
            >
              Picking producto
            </Button>
          </div>
        </>
      ) : !headerLoading ? (
        <p className="text-sm text-muted-foreground">Plan no encontrado o sin datos.</p>
      ) : null}
    </div>
  )
}
