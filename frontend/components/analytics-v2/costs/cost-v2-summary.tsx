"use client"

import { cn } from "@/lib/utils"
import {
  ANOMALY_LABELS,
  MOVEMENT_LABELS,
  RELIABILITY_LABELS,
  TAX_LABELS,
  formatDate,
  formatDateTime,
} from "@/lib/analytics-v2/labels"
import type {
  AnomalyCode,
  CostReliability,
  CostSummary,
  FreshnessResponse,
  TaxProfileStatus,
} from "@/lib/analytics-v2/types"

const RELIABILITY_ORDER: CostReliability[] = [
  "requiere_revision",
  "sin_costo",
  "solo_promedio_bsale",
  "desactualizado",
  "consistente",
]

const RAW_RESOURCES_SHOWN = new Set([
  "stock_receptions",
  "variant_costs",
  "variants",
  "products",
  "product_taxes",
  "variant_prices",
])

export function CostV2Kpis({
  summary,
  loading,
  onSelectReliability,
}: {
  summary: CostSummary | null
  loading: boolean
  onSelectReliability: (status: CostReliability | null) => void
}) {
  const value = (n: number | undefined) => (loading && !summary ? "…" : (n ?? "—"))
  const cards: { key: string; title: string; value: number | string; hint: string; filter: CostReliability | null }[] = [
    {
      key: "active",
      title: "SKUs activos",
      value: value(summary?.skus_active),
      hint: `${summary?.skus_total ?? "—"} SKUs en RAW (incluye inactivos)`,
      filter: null,
    },
    {
      key: "avg",
      title: "Con costo promedio Bsale",
      value: value(summary?.skus_active_with_bsale_average),
      hint: "variant_costs.average_cost > 0",
      filter: null,
    },
    {
      key: "cmp",
      title: "Con compra comparable",
      value: value(summary?.skus_active_with_comparable_reception),
      hint: "Factura o guía desde 01-01-2025",
      filter: null,
    },
    {
      key: "review",
      title: "Requieren revisión",
      value: value(summary?.skus_active_by_reliability.requiere_revision),
      hint: "Anomalía en la última compra",
      filter: "requiere_revision",
    },
  ]
  return (
    <div className="grid grid-cols-2 gap-2 lg:grid-cols-4">
      {cards.map((c) => (
        <button
          key={c.key}
          type="button"
          onClick={() => onSelectReliability(c.filter)}
          className={cn(
            "rounded-md border border-border/70 bg-card px-3 py-2.5 text-left transition-colors",
            "hover:border-red-700/40 hover:bg-muted/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-red-700/40",
          )}
        >
          <p className="text-[11px] font-medium uppercase tracking-wide text-muted-foreground">{c.title}</p>
          <p className="mt-1 text-2xl font-semibold tabular-nums tracking-tight">{c.value}</p>
          <p className="mt-0.5 text-[11px] text-muted-foreground">{c.hint}</p>
        </button>
      ))}
    </div>
  )
}

function CountList<K extends string>({
  title,
  counts,
  labels,
  order,
  onSelect,
}: {
  title: string
  counts: Partial<Record<K, number>>
  labels: Record<K, string>
  order?: K[]
  onSelect?: (key: K) => void
}) {
  const keys = (order ?? (Object.keys(counts) as K[])).filter((k) => (counts[k] ?? 0) > 0)
  return (
    <div className="rounded-md border p-3">
      <h2 className="text-sm font-semibold">{title}</h2>
      {keys.length === 0 ? (
        <p className="mt-2 text-sm text-muted-foreground">Sin registros.</p>
      ) : (
        <ul className="mt-2 divide-y divide-border/60 text-sm">
          {keys.map((k) => (
            <li key={k} className="flex items-center justify-between gap-2 py-1.5">
              {onSelect ? (
                <button type="button" className="text-left hover:underline" onClick={() => onSelect(k)}>
                  {labels[k]}
                </button>
              ) : (
                <span>{labels[k]}</span>
              )}
              <span className="tabular-nums font-medium">{counts[k]}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

export function CostV2SummaryPanels({
  summary,
  freshness,
  onSelectReliability,
  onSelectAnomaly,
  onSelectTax,
}: {
  summary: CostSummary | null
  freshness: FreshnessResponse | null
  onSelectReliability: (status: CostReliability) => void
  onSelectAnomaly: (code: AnomalyCode) => void
  onSelectTax: (status: TaxProfileStatus) => void
}) {
  if (!summary) return null
  const resources = (freshness?.resources ?? []).filter((r) => RAW_RESOURCES_SHOWN.has(r.resource))
  return (
    <div className="space-y-3">
      <section className="grid gap-3 lg:grid-cols-3">
        <CountList
          title="Confiabilidad del costo (SKUs activos)"
          counts={summary.skus_active_by_reliability}
          labels={RELIABILITY_LABELS}
          order={RELIABILITY_ORDER}
          onSelect={onSelectReliability}
        />
        <CountList
          title="Anomalías en la última compra (SKUs activos)"
          counts={summary.skus_active_by_anomaly}
          labels={ANOMALY_LABELS}
          onSelect={onSelectAnomaly}
        />
        <CountList
          title="Perfil tributario (SKUs activos)"
          counts={summary.skus_active_by_tax_profile}
          labels={TAX_LABELS}
          onSelect={onSelectTax}
        />
      </section>
      <section className="grid gap-3 lg:grid-cols-2">
        <div className="rounded-md border p-3">
          <h2 className="text-sm font-semibold">Movimientos de recepción desde 01-01-2025</h2>
          <div className="mt-2 overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="text-left text-[11px] uppercase tracking-wide text-muted-foreground">
                <tr>
                  <th className="py-1 pr-2 font-medium">Tipo</th>
                  <th className="py-1 pr-2 text-right font-medium">Recepciones</th>
                  <th className="py-1 pr-2 text-right font-medium">Líneas</th>
                  <th className="py-1 pr-2 text-right font-medium">Comparables</th>
                  <th className="py-1 text-right font-medium">Costo 0</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border/60">
                {summary.movements.map((m) => (
                  <tr key={m.movement_class}>
                    <td className="py-1.5 pr-2">{MOVEMENT_LABELS[m.movement_class]}</td>
                    <td className="py-1.5 pr-2 text-right tabular-nums">{m.receptions}</td>
                    <td className="py-1.5 pr-2 text-right tabular-nums">{m.lines}</td>
                    <td className="py-1.5 pr-2 text-right tabular-nums">{m.comparable_lines}</td>
                    <td className="py-1.5 text-right tabular-nums">{m.zero_cost_lines}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="mt-2 text-[11px] text-muted-foreground">
            Solo facturas y guías de despacho completas con costo y cantidad positivos se usan como compra
            comparable. Notas de crédito, transferencias, ajustes y movimientos sin documento quedan visibles
            pero no definen costo.
          </p>
        </div>
        <div className="rounded-md border p-3">
          <h2 className="text-sm font-semibold">Cobertura y actualización BSALE RAW</h2>
          {resources.length === 0 ? (
            <p className="mt-2 text-sm text-muted-foreground">Sin información de sincronización.</p>
          ) : (
            <ul className="mt-2 divide-y divide-border/60 text-sm">
              {resources.map((r) => (
                <li key={`${r.resource}-${r.scope}`} className="flex items-center justify-between gap-2 py-1.5">
                  <span className="min-w-0 truncate">
                    {r.resource}
                    {r.scope && r.scope !== "company" ? (
                      <span className="text-muted-foreground"> · {r.scope}</span>
                    ) : null}
                  </span>
                  <span className="shrink-0 text-xs text-muted-foreground">
                    {r.status ?? "—"} · {formatDateTime(r.last_success_at)}
                  </span>
                </li>
              ))}
            </ul>
          )}
          <p className="mt-2 text-[11px] text-muted-foreground">
            Historial desde {formatDate(summary.meta.history_start)}. Cálculo generado{" "}
            {formatDateTime(summary.meta.generated_at)}.
          </p>
        </div>
      </section>
    </div>
  )
}
