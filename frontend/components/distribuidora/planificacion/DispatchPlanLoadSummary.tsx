"use client"

import type { DispatchPlanDashboard, DispatchPlanLoadSummary } from "@/lib/api"
import { formatClp } from "@/lib/ors-map-ui"
import { Badge } from "@/components/ui/badge"
import { cn } from "@/lib/utils"

const STATUS_STYLES: Record<string, string> = {
  LISTO_PARA_CARGA: "bg-emerald-600/90 text-white",
  FACTURACION_PENDIENTE: "bg-amber-500/90 text-white",
  PICKING_GENERADO: "bg-blue-600/90 text-white",
  DESPACHADO: "bg-slate-600/90 text-white",
}

type Props = {
  dashboard: DispatchPlanDashboard
  className?: string
}

function KpiCell({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <div className="rounded-md border border-border/70 bg-card/80 px-3 py-2">
      <p className="text-[11px] text-muted-foreground">{label}</p>
      <p className="text-lg font-semibold tabular-nums leading-tight">{value}</p>
      {sub ? <p className="text-[10px] text-muted-foreground">{sub}</p> : null}
    </div>
  )
}

export function DispatchPlanLoadSummaryBlock({ dashboard, className }: Props) {
  const ls: DispatchPlanLoadSummary | undefined = dashboard.load_summary
  if (!ls) return null

  const h = ls.header
  const k = ls.kpis
  const inv = ls.invoicing
  const costs = ls.costs
  const results = ls.results
  const statusClass =
    STATUS_STYLES[ls.operational_status] ?? "bg-muted text-foreground"

  return (
    <section
      className={cn(
        "rounded-xl border border-border/80 bg-gradient-to-b from-muted/30 to-card p-4 shadow-sm",
        className,
      )}
    >
      <div className="flex flex-wrap items-start justify-between gap-3 border-b border-border/60 pb-3">
        <div>
          <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
            Resumen de carga
          </p>
          <p className="font-mono text-sm font-semibold">{h.planning_code}</p>
          <p className="text-sm text-muted-foreground">
            {h.planning_date} · {h.truck_name}
          </p>
          <p className="mt-1 text-sm">
            <span className="text-muted-foreground">Chofer:</span>{" "}
            {h.driver_name || h.driver_label || "—"}
            <span className="mx-2 text-muted-foreground">·</span>
            <span className="text-muted-foreground">Peonetas:</span>{" "}
            {h.assistant_label || "—"}
          </p>
          <p className="text-xs text-muted-foreground">
            {h.route_name}
            {h.communes ? ` · ${h.communes}` : ""}
          </p>
        </div>
        <Badge className={cn("shrink-0 text-xs font-semibold", statusClass)}>
          {ls.operational_status_label}
        </Badge>
      </div>

      <div className="mt-3 grid gap-2 sm:grid-cols-2 lg:grid-cols-4">
        <KpiCell label="Clientes" value={String(k.clients)} />
        <KpiCell label="Documentos" value={String(k.documents)} />
        <KpiCell
          label="Venta OC (plan)"
          value={formatClp(k.oc_total_amount_clp ?? inv.oc_total_amount_clp ?? k.sales_total_clp)}
          sub="Todas las órdenes del plan"
        />
        <KpiCell
          label="Venta confirmada"
          value={formatClp(k.confirmed_sales_clp ?? inv.confirmed_amount_clp)}
          sub="Facturadas + auto ≥75"
        />
        <KpiCell
          label="Venta picking"
          value={formatClp(k.picking_sales_clp ?? 0)}
          sub={ls.picking.has_snapshot ? "Docs en snapshot" : "Sin picking aún"}
        />
        <KpiCell label="Productos distintos" value={String(k.distinct_products)} />
        <KpiCell
          label="Unidades"
          value={k.total_units.toLocaleString("es-CL", { maximumFractionDigits: 0 })}
        />
        <KpiCell
          label="Cajas est."
          value={k.estimated_boxes.toLocaleString("es-CL", { maximumFractionDigits: 0 })}
        />
      </div>

      <div className="mt-4 grid gap-3 lg:grid-cols-3">
        <div className="rounded-lg border border-border/60 px-3 py-2 text-sm">
          <p className="mb-2 text-xs font-semibold uppercase text-muted-foreground">
            Facturación
          </p>
          <ul className="space-y-1 text-xs">
            <li>
              Confirmadas: <strong>{inv.confirmed_total}</strong>
              {inv.confirmed_auto > 0 ? (
                <span className="text-muted-foreground">
                  {" "}
                  ({inv.confirmed_auto} auto ≥75)
                </span>
              ) : null}
            </li>
            <li>
              Probables: <strong>{inv.probable}</strong> ·{" "}
              {formatClp(inv.probable_amount_clp)}
            </li>
            <li>
              Pendientes: <strong>{inv.pending}</strong> ·{" "}
              {formatClp(inv.pending_amount_clp)}
            </li>
          </ul>
        </div>
        <div className="rounded-lg border border-border/60 px-3 py-2 text-sm">
          <p className="mb-2 text-xs font-semibold uppercase text-muted-foreground">Costos ruta</p>
          <ul className="space-y-1 text-xs tabular-nums">
            <li>Combustible: {formatClp(costs.fuel_clp)}</li>
            <li>Tripulación: {formatClp(costs.crew_clp)}</li>
            <li>Peajes: {formatClp(costs.tolls_clp)}</li>
            <li>Ferry: {formatClp(costs.ferry_clp)}</li>
            <li className="font-semibold pt-1 border-t border-border/50">
              Total: {formatClp(costs.route_total_clp)}
            </li>
          </ul>
        </div>
        <div className="rounded-lg border border-emerald-500/25 bg-emerald-500/5 px-3 py-2 text-sm">
          <p className="mb-2 text-xs font-semibold uppercase text-muted-foreground">
            Resultado
          </p>
          {results.margin_visible ? (
            <ul className="space-y-1 text-xs tabular-nums">
              <li>
                Margen comercial:{" "}
                {results.commercial_margin_clp != null
                  ? formatClp(results.commercial_margin_clp)
                  : "—"}
              </li>
              <li className="font-semibold">
                Neto operativo:{" "}
                {results.net_operational_clp != null
                  ? formatClp(results.net_operational_clp)
                  : "—"}
              </li>
            </ul>
          ) : (
            <p className="text-xs text-muted-foreground">
              {results.margin_message ?? "Margen no visible para su rol o pendiente de cálculo."}
            </p>
          )}
          {ls.picking.has_snapshot ? (
            <p className="mt-2 text-[10px] text-muted-foreground">
              Picking v{ls.picking.version ?? "?"} persistido
            </p>
          ) : ls.picking.ready_to_generate ? (
            <p className="mt-2 text-[10px] text-emerald-700 dark:text-emerald-400">
              Listo para generar picking (confirmados + auto)
            </p>
          ) : ls.picking.ready_reason ? (
            <p className="mt-2 text-[10px] text-amber-700 dark:text-amber-400">
              {ls.picking.ready_reason}
            </p>
          ) : null}
        </div>
      </div>
    </section>
  )
}
