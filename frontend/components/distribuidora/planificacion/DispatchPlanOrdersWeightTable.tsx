"use client"

import type { DispatchPlanOrderWeightRow } from "@/lib/api"
import { formatClp } from "@/lib/ors-map-ui"

export function DispatchPlanOrdersWeightTable({
  orders,
}: {
  orders: DispatchPlanOrderWeightRow[]
}) {
  if (!orders.length) return null

  const sorted = [...orders].sort(
    (a, b) => (a.route_order ?? 0) - (b.route_order ?? 0),
  )

  const fmtKg = (n: number | null | undefined) => {
    const v = n != null ? Number(n) : NaN
    return Number.isFinite(v) && v > 0
      ? `${v.toLocaleString("es-CL", { maximumFractionDigits: 0 })} kg`
      : "—"
  }

  const fmtPct = (n: number | null | undefined) => {
    const v = n != null ? Number(n) : NaN
    return Number.isFinite(v) ? `${Math.round(v)} %` : "—"
  }

  return (
    <div className="overflow-x-auto rounded-md border border-border/70">
      <table className="w-full min-w-[720px] text-left text-xs">
        <thead className="border-b bg-muted/40 text-[10px] uppercase tracking-wide text-muted-foreground">
          <tr>
            <th className="px-2 py-2">OC</th>
            <th className="px-2 py-2">Cliente</th>
            <th className="px-2 py-2 text-right">Monto</th>
            <th className="px-2 py-2 text-right">Peso</th>
            <th className="px-2 py-2 text-right">Cobertura</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((row) => (
            <tr
              key={row.oc_document_id}
              className="border-b border-border/40 last:border-0"
            >
              <td className="px-2 py-1.5 tabular-nums font-medium">
                {row.oc_number ?? row.oc_document_id}
              </td>
              <td className="px-2 py-1.5">{row.client_name || "—"}</td>
              <td className="px-2 py-1.5 text-right tabular-nums">
                {formatClp(Number(row.oc_total_amount) || 0)}
              </td>
              <td className="px-2 py-1.5 text-right tabular-nums font-medium">
                {fmtKg(row.peso_total_kg ?? row.weight_kg)}
              </td>
              <td className="px-2 py-1.5 text-right tabular-nums text-muted-foreground">
                {fmtPct(row.cobertura_logistica ?? row.porcentaje_cobertura_peso)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
