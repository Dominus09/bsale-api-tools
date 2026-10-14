"use client"

import { cn } from "@/lib/utils"
import { LIST_SCOPE_LABELS, MARGIN_STATUS_LABELS, MARGIN_STATUS_TONES, PRIMARY_STATUSES } from "@/lib/analytics-v2/margins-labels"
import type { MarginStatus, MarginsSummary } from "@/lib/analytics-v2/margins-types"

export function MarginV2Kpis({
  summary,
  loading,
  onSelectStatus,
}: {
  summary: MarginsSummary | null
  loading: boolean
  onSelectStatus: (status: MarginStatus | null) => void
}) {
  const value = (n: number | undefined) => (loading && !summary ? "…" : (n ?? "—"))
  const cards: { key: string; title: string; value: number | string; hint: string; filter: MarginStatus | null }[] = [
    {
      key: "rows",
      title: "SKU × lista evaluados",
      value: value(summary?.rows_total),
      hint: `Listas ${summary?.meta.evaluated_price_list_ids.join(", ") ?? "—"} · SKUs activos`,
      filter: null,
    },
    {
      key: "bajo",
      title: "Bajo política",
      value: value(summary?.by_status.bajo_politica),
      hint: summary?.meta.promotions_integrated
        ? "Excluye solo promociones y remates verificados"
        : "Calculado; promociones aún no verificables",
      filter: "bajo_politica",
    },
    {
      key: "regla",
      title: "Sin regla",
      value: value(summary?.by_status.sin_regla),
      hint: `${summary?.policy.missing_rule_combinations ?? "—"} combinaciones lista × tipo sin regla`,
      filter: "sin_regla",
    },
    {
      key: "costo",
      title: "Sin costo",
      value: loading && !summary ? "…" : summary ? summary.by_status.sin_costo + summary.by_status.costo_no_verificado : "—",
      hint: "Sin costo base para la base elegida",
      filter: "sin_costo",
    },
    {
      key: "impuestos",
      title: "Revisión tributaria",
      value: value(summary?.by_status.requiere_revision_tributaria),
      hint: "IVA anticipado, más de un ILA o impuesto inconsistente: sin recargo",
      filter: "requiere_revision_tributaria",
    },
  ]
  return (
    <div className="grid grid-cols-2 gap-2 lg:grid-cols-5">
      {cards.map((c) => (
        <button
          key={c.key}
          type="button"
          onClick={() => onSelectStatus(c.filter)}
          className={cn(
            "rounded-md border border-border/70 bg-card px-3 py-2.5 text-left transition-colors",
            "hover:border-red-700/40 hover:bg-muted/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-red-700/40",
          )}
        >
          <p className="text-[11px] font-medium uppercase tracking-wide text-muted-foreground">{c.title}</p>
          <p className="mt-1 text-2xl font-semibold tabular-nums tracking-tight">{c.value}</p>
          <p className="mt-0.5 truncate text-[11px] text-muted-foreground">{c.hint}</p>
        </button>
      ))}
    </div>
  )
}

export function MarginV2ListBreakdown({
  summary,
  onSelect,
}: {
  summary: MarginsSummary | null
  onSelect: (priceListId: number, status: MarginStatus) => void
}) {
  if (!summary) return null
  return (
    <div className="space-y-3">
      <div className="overflow-x-auto rounded-md border">
        <table className="w-full min-w-[820px] text-sm">
          <thead className="bg-muted/40 text-left text-[11px] uppercase tracking-wide text-muted-foreground">
            <tr>
              <th className="px-3 py-2 font-medium">Lista</th>
              {PRIMARY_STATUSES.map((s) => (
                <th key={s} className="px-3 py-2 text-right font-medium">
                  {MARGIN_STATUS_LABELS[s]}
                </th>
              ))}
              <th className="px-3 py-2 text-right font-medium">Provisionales</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border/60">
            {summary.by_price_list.map((l) => (
              <tr key={l.price_list_id}>
                <td className="px-3 py-2">
                  <span className="font-medium">{l.label ?? l.name}</span>
                  <span className="block text-[11px] text-muted-foreground">
                    #{l.price_list_id} · {l.name}
                  </span>
                </td>
                {PRIMARY_STATUSES.map((s) => (
                  <td key={s} className="px-3 py-2 text-right tabular-nums">
                    {l.by_status[s] ? (
                      <button
                        type="button"
                        className={cn("rounded border px-1.5 py-0.5 text-xs hover:underline", MARGIN_STATUS_TONES[s])}
                        onClick={() => onSelect(l.price_list_id, s)}
                      >
                        {l.by_status[s]}
                      </button>
                    ) : (
                      <span className="text-muted-foreground">0</span>
                    )}
                  </td>
                ))}
                <td className="px-3 py-2 text-right tabular-nums text-muted-foreground">
                  {l.provisional} / {l.rows}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="rounded-md border p-3">
        <p className="mb-2 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">Alcance de listas (BSALE RAW)</p>
        <div className="flex flex-wrap gap-1.5">
          {summary.scope.price_lists.map((e) => (
            <span
              key={e.price_list_id}
              className={cn(
                "rounded-md border px-2 py-0.5 text-xs",
                e.status === "evaluada" && "border-emerald-300 bg-emerald-50 text-emerald-800",
                e.discrepancy && "border-red-300 bg-red-50 text-red-800",
                !e.discrepancy && e.status !== "evaluada" && "bg-muted/40 text-muted-foreground",
              )}
              title={LIST_SCOPE_LABELS[e.status]}
            >
              #{e.price_list_id} {e.commercial_label ?? e.name ?? ""} · {LIST_SCOPE_LABELS[e.status]}
            </span>
          ))}
        </div>
      </div>
    </div>
  )
}
