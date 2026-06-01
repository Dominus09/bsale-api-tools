"use client"

import type { DispatchPlanInvoicedRow } from "@/lib/api"
import { DispatchPlanInvoicedStatusBadge } from "@/components/distribuidora/planificacion/DispatchPlanInvoicedStatusBadge"

export function DispatchPlanInvoicedItemsTable({
  items,
}: {
  items: DispatchPlanInvoicedRow[]
}) {
  if (!items.length) return null

  return (
    <div className="overflow-x-auto rounded-md border border-border/70">
      <table className="w-full min-w-[640px] text-left text-xs">
        <thead className="border-b bg-muted/40 text-[10px] uppercase tracking-wide text-muted-foreground">
          <tr>
            <th className="px-2 py-2">OC</th>
            <th className="px-2 py-2">Estado</th>
            <th className="px-2 py-2">Origen</th>
            <th className="px-2 py-2">Doc. relacionado</th>
            <th className="px-2 py-2">Score</th>
          </tr>
        </thead>
        <tbody>
          {items.map((row) => (
            <tr
              key={row.oc_document_id}
              className="border-b border-border/40 last:border-0"
            >
              <td className="px-2 py-1.5 tabular-nums font-medium">
                {row.oc_number ?? row.oc_document_id}
              </td>
              <td className="px-2 py-1.5">
                <DispatchPlanInvoicedStatusBadge row={row} compact />
              </td>
              <td className="px-2 py-1.5 text-muted-foreground">
                {row.relation_source ?? "—"}
              </td>
              <td className="px-2 py-1.5 tabular-nums">
                {row.related_document_number ?? "—"}
              </td>
              <td className="px-2 py-1.5 tabular-nums text-muted-foreground">
                {row.probable_score != null ? String(row.probable_score) : "—"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
