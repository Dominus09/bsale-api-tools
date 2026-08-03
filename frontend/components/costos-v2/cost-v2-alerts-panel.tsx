"use client"

import type { CostV2ProductsSummaryBody } from "@/lib/costos-v2/types"
import { cn } from "@/lib/utils"

type AlertKey =
  | "incomplete_tax_context"
  | "missing_cost"
  | "suspicious_outlier"
  | "stored_components_rounding"

export function CostV2AlertsPanel({
  summary,
  loading,
  onSelect,
}: {
  summary: CostV2ProductsSummaryBody | null
  loading?: boolean
  onSelect: (key: AlertKey) => void
}) {
  const rows: { key: AlertKey; label: string; count: number }[] = [
    {
      key: "incomplete_tax_context",
      label: "Contexto tributario incompleto",
      count: summary?.products_incomplete_tax_context ?? 0,
    },
    {
      key: "missing_cost",
      label: "Costo faltante",
      count: summary?.products_missing_cost ?? 0,
    },
    {
      key: "suspicious_outlier",
      label: "Costo atípico",
      count: summary?.products_with_outlier ?? 0,
    },
    {
      key: "stored_components_rounding",
      label: "Diferencias de redondeo",
      count: summary?.products_rounding_warning ?? 0,
    },
  ]

  return (
    <section className="rounded-md border border-border/70">
      <div className="border-b border-border/60 px-3 py-2">
        <h2 className="text-sm font-semibold">Alertas que requieren atención</h2>
      </div>
      <ul className="divide-y divide-border/50">
        {rows.map((r) => (
          <li key={r.key}>
            <button
              type="button"
              className={cn(
                "flex w-full items-center justify-between px-3 py-2.5 text-left text-sm",
                "hover:bg-muted/40",
              )}
              onClick={() => onSelect(r.key)}
              disabled={loading}
            >
              <span>{r.label}</span>
              <span
                className={cn(
                  "tabular-nums font-semibold",
                  r.count > 0 ? "text-red-700 dark:text-red-400" : "text-muted-foreground",
                )}
              >
                {loading ? "…" : r.count}
              </span>
            </button>
          </li>
        ))}
      </ul>
    </section>
  )
}
