"use client"

import { useEffect, useRef, useState } from "react"
import { useParams } from "next/navigation"
import { Loader2 } from "lucide-react"

import {
  refreshDispatchPlanCommercialMargin,
  type DispatchPlanDashboard,
  type DispatchPlanSummary,
} from "@/lib/api"
import { DispatchPlanDetailShell } from "@/components/distribuidora/planificacion/DispatchPlanDetailShell"
import {
  fetchDispatchPlanDashboardDeduped,
  fetchDispatchPlanHeaderDeduped,
  invalidateDashboardCache,
  logFrontendPlanDebug,
  trackPlanPageRender,
} from "@/lib/planificacion-fetch"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"

function emptyDashboardFromPlan(plan: DispatchPlanSummary): DispatchPlanDashboard {
  return {
    plan,
    invoicing: {
      total_orders: 0,
      total_oc_amount_clp: 0,
      confirmed: { count: 0, amount_clp: 0, auto_confirmed_count: 0 },
      probable: { count: 0, amount_clp: 0 },
      pending: { count: 0, amount_clp: 0 },
    },
    invoiced_items: [],
    warnings: [
      {
        oc_document_id: null,
        oc_number: null,
        scope: "global",
        message: "No se pudo cargar el dashboard de facturación.",
      },
    ],
    probable_notes: [],
    margin: null,
    picking: {
      client_endpoint: `/distribuidora/dispatch-plans/${plan.id}/picking-by-client`,
      product_endpoint: `/distribuidora/dispatch-plans/${plan.id}/picking-by-product`,
      ready: false,
    },
    degraded: true,
  }
}

export default function PlanificacionDetallePage() {
  const params = useParams()
  const planId = Number(params.id)

  const [plan, setPlan] = useState<DispatchPlanSummary | null>(null)
  const [dashboard, setDashboard] = useState<DispatchPlanDashboard | null>(null)
  const [headerLoading, setHeaderLoading] = useState(true)
  const [dashboardLoading, setDashboardLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [refreshBusy, setRefreshBusy] = useState(false)
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
        let dash: DispatchPlanDashboard
        try {
          dash = await fetchDispatchPlanDashboardDeduped(planId, ac.signal, {
            include_margin: true,
          })
        } catch (dashErr: unknown) {
          if (ac.signal.aborted) return
          logFrontendPlanDebug("dispatch-plans-dashboard", {
            planning_id: planId,
            fallback: "empty",
            error: dashErr instanceof Error ? dashErr.message : String(dashErr),
          })
          dash = emptyDashboardFromPlan(headerPlan)
        }
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

  const reloadDashboard = async (opts?: { include_margin?: boolean }) => {
    if (!Number.isFinite(planId) || planId <= 0) return
    invalidateDashboardCache(planId)
    setDashboardLoading(true)
    try {
      const dash = await fetchDispatchPlanDashboardDeduped(planId, undefined, {
        include_margin: opts?.include_margin ?? true,
        force: true,
      })
      setDashboard(dash)
      setError(null)
    } catch (e: unknown) {
      if (plan) {
        setDashboard(emptyDashboardFromPlan(plan))
        setMsg(e instanceof Error ? e.message : "No se pudo actualizar facturación")
      } else {
        setError(e instanceof Error ? e.message : "Error")
      }
    } finally {
      setDashboardLoading(false)
    }
  }

  const refreshCommercial = async () => {
    if (!Number.isFinite(planId) || planId <= 0) return
    setRefreshBusy(true)
    try {
      await refreshDispatchPlanCommercialMargin(planId)
      await reloadDashboard({ include_margin: true })
      setMsg("Datos comerciales actualizados desde PostgreSQL (margen vivo persistido).")
    } catch (e: unknown) {
      setMsg(e instanceof Error ? e.message : "No se pudo actualizar margen comercial")
    } finally {
      setRefreshBusy(false)
    }
  }

  if (!Number.isFinite(planId) || planId <= 0) {
    return <p className="p-6 text-sm text-destructive">ID de plan inválido</p>
  }

  const displayPlan = dashboard?.plan ?? plan

  return (
    <div className="space-y-4 p-6">
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
          {dashboard?.degraded ? (
            <Alert
              variant="default"
              className="border-amber-200 bg-amber-50/80 dark:border-amber-900 dark:bg-amber-950/30"
            >
              <AlertTitle>Facturación parcial</AlertTitle>
              <AlertDescription className="text-sm">
                El encabezado del plan cargó correctamente, pero facturación o métricas no
                estuvieron disponibles. Use «Actualizar datos comerciales» o reintente.
              </AlertDescription>
            </Alert>
          ) : null}

          {dashboardLoading && !dashboard ? (
            <div className="flex items-center gap-2 rounded-lg border border-dashed border-border px-4 py-8 text-sm text-muted-foreground">
              <Loader2 className="size-4 animate-spin" aria-hidden />
              Cargando facturación y métricas…
            </div>
          ) : dashboard ? (
            <DispatchPlanDetailShell
              planId={planId}
              dashboard={dashboard}
              dashboardLoading={dashboardLoading}
              onReloadDashboard={reloadDashboard}
              onRefreshCommercial={refreshCommercial}
              onMessage={setMsg}
              refreshBusy={refreshBusy}
            />
          ) : null}
        </>
      ) : !headerLoading ? (
        <p className="text-sm text-muted-foreground">Plan no encontrado o sin datos.</p>
      ) : null}
    </div>
  )
}
