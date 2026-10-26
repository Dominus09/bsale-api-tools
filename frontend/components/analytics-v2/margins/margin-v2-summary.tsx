"use client"

import { CheckCircle2, CircleDollarSign, ListChecks, ShieldAlert, TrendingDown, TrendingUp } from "lucide-react"

import { MetricCard, SectionCard, StatusBadge } from "@/components/analytics-v2/shared/analytics-ui"
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
  const cards: {
    key: string
    title: string
    value: number | string
    hint: string
    filter: MarginStatus | null
    tone: "neutral" | "success" | "warning" | "danger" | "info"
    icon: typeof ListChecks
  }[] = [
    {
      key: "rows",
      title: "Combinaciones evaluadas",
      value: value(summary?.rows_total),
      hint: `SKU × lista · ${summary?.by_price_list?.map((l) => l.label ?? l.name).filter(Boolean).join(", ") || "listas evaluadas"}`,
      filter: null,
      tone: "info",
      icon: ListChecks,
    },
    {
      key: "bajo",
      title: "Bajo política",
      value: value(summary?.by_status.bajo_politica),
      hint: "Recargo inferior al mínimo configurado",
      filter: "bajo_politica",
      tone: summary?.by_status.bajo_politica ? "danger" : "success",
      icon: TrendingDown,
    },
    {
      key: "dentro",
      title: "Dentro de política",
      value: value(summary?.by_status.dentro_politica),
      hint: "Recargo dentro del rango configurado",
      filter: "dentro_politica",
      tone: "success",
      icon: CheckCircle2,
    },
    {
      key: "sobre",
      title: "Sobre política",
      value: value(summary?.by_status.sobre_politica),
      hint: "Recargo superior al máximo, cuando existe",
      filter: "sobre_politica",
      tone: "info",
      icon: TrendingUp,
    },
    {
      key: "costo",
      title: "Sin costo utilizable",
      value: value(summary?.by_status.sin_costo),
      hint: "No se puede calcular recargo ni margen",
      filter: "sin_costo",
      tone: summary?.by_status.sin_costo ? "warning" : "success",
      icon: CircleDollarSign,
    },
    {
      key: "impuestos",
      title: "Revisión tributaria",
      value: value(summary?.by_status.requiere_revision_tributaria),
      hint: "IVA o ILA impiden obtener costo bruto",
      filter: "requiere_revision_tributaria",
      tone: summary?.by_status.requiere_revision_tributaria ? "warning" : "success",
      icon: ShieldAlert,
    },
  ]
  return (
    <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-6">
      {cards.map((c) => (
        <MetricCard
          key={c.key}
          label={c.title}
          value={c.value}
          context={c.hint}
          tone={c.tone}
          icon={c.icon}
          onClick={() => onSelectStatus(c.filter)}
        />
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
    <div className="space-y-4">
      <SectionCard
        title="Comparación por lista de precios"
        description="Cada cifra corresponde a combinaciones SKU × lista. Selecciona una cantidad para explorar sus productos."
        contentClassName="p-0"
      >
        <div className="overflow-x-auto">
        <table className="w-full min-w-[780px] text-sm">
          <thead className="border-b border-lq-line-soft bg-lq-canvas text-left text-xs text-lq-muted">
            <tr>
              <th className="px-5 py-3 font-medium">Lista</th>
              {PRIMARY_STATUSES.map((s) => (
                <th key={s} className="px-3 py-2 text-right font-medium">
                  {MARGIN_STATUS_LABELS[s]}
                </th>
              ))}
              <th className="px-5 py-3 text-right font-medium">En validación</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-lq-line-soft">
            {summary.by_price_list.map((l) => (
              <tr key={l.price_list_id} className="hover:bg-lq-canvas">
                <td className="px-5 py-3">
                  <span className="font-medium text-lq-ink">{l.label ?? l.name}</span>
                  <span className="mt-0.5 block text-xs text-lq-muted">
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
                <td className="px-5 py-3 text-right tabular-nums text-lq-muted">
                  {l.provisional} / {l.rows}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        </div>
      </SectionCard>

      <SectionCard title="Alcance de listas Bsale" description="Solo las listas vigentes y autorizadas participan del análisis.">
        <div className="flex flex-wrap gap-1.5">
          {summary.scope.price_lists.map((e) => (
            <StatusBadge
              key={e.price_list_id}
              tone={e.discrepancy ? "danger" : e.status === "evaluada" ? "success" : "neutral"}
            >
              {e.commercial_label ?? e.name ?? "Lista sin nombre"} · {LIST_SCOPE_LABELS[e.status]}
            </StatusBadge>
          ))}
        </div>
      </SectionCard>
    </div>
  )
}
