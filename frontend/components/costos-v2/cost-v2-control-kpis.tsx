"use client"

import { cn } from "@/lib/utils"

export function CostV2ControlKpis({
  totalProducts,
  relevantChanges,
  needsReview,
  outliers,
  thresholdLabel,
  loading,
  onSelect,
}: {
  totalProducts: number | null
  relevantChanges: number | null
  needsReview: number | null
  outliers: number | null
  thresholdLabel: string
  loading?: boolean
  onSelect: (key: "all" | "changes" | "review" | "outlier") => void
}) {
  const cards = [
    {
      key: "all" as const,
      title: "Productos controlados",
      value: totalProducts,
      hint: "Con recepción V2 en el rango",
    },
    {
      key: "changes" as const,
      title: "Cambios relevantes",
      value: relevantChanges,
      hint: `Variación ≥ ${thresholdLabel}`,
    },
    {
      key: "review" as const,
      title: "Requieren revisión",
      value: needsReview,
      hint: "Sin costo o contexto incompleto",
    },
    {
      key: "outlier" as const,
      title: "Costos atípicos",
      value: outliers,
      hint: "Warning suspicious_outlier",
    },
  ]

  return (
    <div className="grid grid-cols-2 gap-2 lg:grid-cols-4">
      {cards.map((c) => (
        <button
          key={c.key}
          type="button"
          onClick={() => onSelect(c.key)}
          className={cn(
            "rounded-md border border-border/70 bg-card px-3 py-2.5 text-left transition-colors",
            "hover:border-red-700/40 hover:bg-muted/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-red-700/40",
          )}
        >
          <p className="text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
            {c.title}
          </p>
          <p className="mt-1 text-2xl font-semibold tabular-nums tracking-tight">
            {loading ? "…" : (c.value ?? "—")}
          </p>
          <p className="mt-0.5 text-[11px] text-muted-foreground">{c.hint}</p>
        </button>
      ))}
    </div>
  )
}
