"use client"

import type { DispatchPlanDashboard } from "@/lib/api"
import { formatClp } from "@/lib/ors-map-ui"
import { cn } from "@/lib/utils"

type DispatchPlanInvoicingDashboardProps = {
  data: DispatchPlanDashboard
  className?: string
}

function StatCard({
  label,
  count,
  amount,
  dotClass,
}: {
  label: string
  count: number
  amount: number
  dotClass: string
}) {
  return (
    <div className="rounded-lg border border-border/70 bg-card/90 px-4 py-3">
      <div className="flex items-center gap-2">
        <span className={cn("size-2.5 rounded-full", dotClass)} aria-hidden />
        <p className="text-xs font-medium text-foreground">{label}</p>
      </div>
      <p className="mt-2 text-2xl font-semibold tabular-nums">{count}</p>
      <p className="text-xs text-muted-foreground">{formatClp(amount)}</p>
    </div>
  )
}

export function DispatchPlanInvoicingDashboard({
  data,
  className,
}: DispatchPlanInvoicingDashboardProps) {
  const inv = data.invoicing
  const margin = data.margin

  return (
    <div className={cn("space-y-6", className)}>
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
        <div className="rounded-lg border border-border/70 bg-muted/20 px-4 py-3 lg:col-span-1">
          <p className="text-xs text-muted-foreground">Total órdenes de compra</p>
          <p className="mt-1 text-2xl font-semibold tabular-nums">{inv.total_orders}</p>
          <p className="mt-0.5 text-sm font-medium tabular-nums">
            {formatClp(inv.total_oc_amount_clp)}
          </p>
        </div>
        <StatCard
          label="Confirmadas (document_related)"
          count={inv.confirmed.count}
          amount={inv.confirmed.amount_clp}
          dotClass="bg-emerald-500"
        />
        <StatCard
          label="Probables (heurística)"
          count={inv.probable.count}
          amount={inv.probable.amount_clp}
          dotClass="bg-amber-400"
        />
        <StatCard
          label="Pendientes"
          count={inv.pending.count}
          amount={inv.pending.amount_clp}
          dotClass="bg-red-500"
        />
      </div>

      {margin?.visible ? (
        <div className="rounded-lg border border-emerald-500/30 bg-emerald-500/5 px-4 py-3">
          <p className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            Resultado operativo neto (camión)
          </p>
          {margin.unavailable || margin.commercial_margin_clp == null ? (
            <p className="mt-2 text-sm text-amber-800 dark:text-amber-200">
              {margin.message ??
                "Margen comercial no disponible: faltan costos de variante en Bsale. No se muestra un margen estimado."}
            </p>
          ) : (
            <div className="mt-2 grid gap-2 sm:grid-cols-2 lg:grid-cols-4 text-sm">
              <div>
                <span className="text-muted-foreground">Margen comercial facturado</span>
                <p className="font-semibold tabular-nums">
                  {formatClp(margin.commercial_margin_clp ?? 0)}
                </p>
                <p className="text-[10px] text-muted-foreground">
                  Venta {formatClp(margin.invoiced_revenue_clp ?? 0)} − costo{" "}
                  {formatClp(margin.invoiced_cost_clp ?? 0)}
                </p>
              </div>
              <div>
                <span className="text-muted-foreground">Costo logístico ruta</span>
                <p className="font-semibold tabular-nums">
                  {formatClp(margin.route_cost_clp ?? 0)}
                </p>
              </div>
              <div className="lg:col-span-2">
                <span className="text-muted-foreground">Resultado operativo neto</span>
                <p
                  className={cn(
                    "text-lg font-semibold tabular-nums",
                    (margin.net_operational_clp ?? 0) >= 0
                      ? "text-emerald-700 dark:text-emerald-400"
                      : "text-red-600",
                  )}
                >
                  {formatClp(margin.net_operational_clp ?? 0)}
                </p>
                <p className="text-[10px] text-muted-foreground">
                  Fuente: {margin.source ?? "variant_cost"} — solo documentos confirmados
                </p>
              </div>
            </div>
          )}
        </div>
      ) : margin?.restricted ? (
        <p className="rounded-md border border-dashed border-border px-3 py-2 text-xs text-muted-foreground">
          Margen final oculto para su rol. Solicite acceso a finanzas/administración.
        </p>
      ) : null}

      {data.warnings.length > 0 ? (
        <div className="rounded-md border border-red-500/30 bg-red-500/5 px-3 py-2 text-xs text-red-800 dark:text-red-300">
          <p className="font-medium">OCs sin documento facturado asociado</p>
          <ul className="mt-1 list-inside list-disc">
            {data.warnings.slice(0, 8).map((w) => (
              <li key={w.oc_document_id}>
                OC {w.oc_number ?? w.oc_document_id}: {w.message}
              </li>
            ))}
          </ul>
        </div>
      ) : null}

      {data.probable_notes.length > 0 ? (
        <div className="rounded-md border border-amber-500/30 bg-amber-500/5 px-3 py-2 text-xs text-amber-900 dark:text-amber-200">
          <p className="font-medium">Coincidencias probables (no confirmadas)</p>
          <ul className="mt-1 list-inside list-disc">
            {data.probable_notes.slice(0, 6).map((n) => (
              <li key={n.oc_document_id}>
                OC {n.oc_number ?? n.oc_document_id}
                {n.probable_document_number
                  ? ` → doc. ${n.probable_document_number}`
                  : ""}
              </li>
            ))}
          </ul>
        </div>
      ) : null}
    </div>
  )
}
