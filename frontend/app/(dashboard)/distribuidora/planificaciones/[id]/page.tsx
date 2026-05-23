"use client"

import { useCallback, useEffect, useState } from "react"
import Link from "next/link"
import { useParams } from "next/navigation"
import { ArrowLeft, Loader2, Wrench } from "lucide-react"

import {
  downloadDispatchPlanBillingExcel,
  getDispatchPlanDashboard,
  getDispatchPlanInvoicedDocuments,
  getDispatchPlanPickingByClient,
  getDispatchPlanPickingByProduct,
  markDispatchPlanPickingGenerated,
  repairDispatchPlanSnapshot,
  type DispatchPlanDashboard,
} from "@/lib/api"
import { DispatchPlanInvoicingDashboard } from "@/components/distribuidora/planificacion/DispatchPlanInvoicingDashboard"
import { formatClp } from "@/lib/ors-map-ui"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"

export default function PlanificacionDetallePage() {
  const params = useParams()
  const planId = Number(params.id)
  const [data, setData] = useState<DispatchPlanDashboard | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState<string | null>(null)
  const [msg, setMsg] = useState<string | null>(null)

  const load = useCallback(async () => {
    if (!Number.isFinite(planId) || planId <= 0) return
    setLoading(true)
    setError(null)
    try {
      const dash = await getDispatchPlanDashboard(planId)
      setData(dash)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error")
    } finally {
      setLoading(false)
    }
  }, [planId])

  useEffect(() => {
    void load()
  }, [load])

  const plan = data?.plan

  if (!Number.isFinite(planId) || planId <= 0) {
    return <p className="p-6 text-sm text-destructive">ID de plan inválido</p>
  }

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
          {loading ? (
            <Loader2 className="size-5 animate-spin text-muted-foreground" />
          ) : plan ? (
            <>
              <p className="font-mono text-xs text-muted-foreground">
                {plan.planning_code ?? `PLAN-${plan.id}`}
              </p>
              <h1 className="text-xl font-semibold">{plan.planning_name ?? plan.route_name}</h1>
              <p className="text-sm text-muted-foreground">
                {plan.truck_name} · {plan.planning_date?.slice(0, 10)}
              </p>
              <Badge variant="secondary" className="mt-2">
                {plan.status}
              </Badge>
            </>
          ) : null}
        </div>
        {plan ? (
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
                    await load()
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

      {!loading && !error && !data ? (
        <p className="text-sm text-muted-foreground">Plan no encontrado o sin datos.</p>
      ) : null}

      {data ? (
        <>
          <div className="grid gap-3 sm:grid-cols-3 text-sm">
            <div className="rounded-md border px-3 py-2">
              <span className="text-muted-foreground">Km ORS</span>
              <p className="font-semibold tabular-nums">{Number(plan?.km_total ?? 0).toFixed(1)}</p>
            </div>
            <div className="rounded-md border px-3 py-2">
              <span className="text-muted-foreground">Costo ruta</span>
              <p className="font-semibold tabular-nums">
                {formatClp(plan?.total_route_cost_clp ?? 0)}
              </p>
            </div>
            <div className="rounded-md border px-3 py-2">
              <span className="text-muted-foreground">Tripulación</span>
              <p className="font-semibold">
                {plan?.driver_count ?? 1} chofer / {plan?.assistant_count ?? 0} peoneta(s)
              </p>
            </div>
          </div>

          <DispatchPlanInvoicingDashboard data={data} />

          <div className="flex flex-wrap gap-2 border-t pt-4">
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
              disabled={!!busy || !data.picking.ready}
              onClick={() =>
                void (async () => {
                  setBusy("pc")
                  try {
                    const r = await getDispatchPlanPickingByClient(planId)
                    await markDispatchPlanPickingGenerated(planId)
                    setMsg(`Picking cliente: ${(r.clients as unknown[]).length} paradas (docs reales).`)
                    await load()
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
              disabled={!!busy || !data.picking.ready}
              onClick={() =>
                void (async () => {
                  setBusy("pp")
                  try {
                    const r = await getDispatchPlanPickingByProduct(planId)
                    setMsg(`Picking producto: ${(r.items as unknown[]).length} líneas consolidadas.`)
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
      ) : loading ? (
        <div className="flex items-center gap-2 text-muted-foreground">
          <Loader2 className="size-4 animate-spin" />
          Cargando plan…
        </div>
      ) : null}
    </div>
  )
}
