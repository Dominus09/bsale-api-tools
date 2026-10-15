"use client"

import { History, Loader2 } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { cn } from "@/lib/utils"
import { RELIABILITY_LABELS, RELIABILITY_TONES, formatMoney, formatPct } from "@/lib/analytics-v2/labels"
import {
  COST_BASIS_LABELS,
  HIGHLIGHTED_RISKS,
  MARGIN_RISK_LABELS,
  MARGIN_SORT_LABELS,
  MARGIN_STATUS_LABELS,
  MARGIN_STATUS_TONES,
  PRIMARY_STATUSES,
  PROMOTION_CHECK_LABELS,
  QUALIFIER_STATUSES,
  RULE_ISSUE_LABELS,
  reasonLabel,
} from "@/lib/analytics-v2/margins-labels"
import type { CostBasis, MarginRow, MarginStatus, MarginsFilters } from "@/lib/analytics-v2/margins-types"
import type { CostReliability } from "@/lib/analytics-v2/types"

export type MarginFilterDraft = {
  q: string
  priceListId: string
  productTypeId: string
  status: MarginStatus | ""
  qualifier: MarginStatus | ""
  reliability: CostReliability | ""
  costBasis: CostBasis
  sort: string
}

export const EMPTY_MARGIN_FILTERS: MarginFilterDraft = {
  q: "",
  priceListId: "",
  productTypeId: "",
  status: "",
  qualifier: "",
  reliability: "",
  costBasis: "promedio_bsale",
  sort: "status",
}

const selectClass = "h-9 rounded-md border border-input bg-background px-2 text-sm"
const labelClass = "flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground"

export function MarginV2Filters({
  draft,
  filters,
  loading,
  onChange,
  onApply,
  onClear,
}: {
  draft: MarginFilterDraft
  filters: MarginsFilters | null
  loading: boolean
  onChange: (patch: Partial<MarginFilterDraft>) => void
  onApply: () => void
  onClear: () => void
}) {
  return (
    <form
      className="flex flex-wrap items-end gap-2 rounded-md border p-3"
      onSubmit={(e) => {
        e.preventDefault()
        onApply()
      }}
    >
      <label className={cn(labelClass, "min-w-[220px] flex-1")}>
        Buscar SKU, código o nombre
        <Input value={draft.q} maxLength={100} onChange={(e) => onChange({ q: e.target.value })} placeholder="Ej: 7802… o azúcar" />
      </label>
      <label className={labelClass}>
        Lista
        <select className={selectClass} value={draft.priceListId} onChange={(e) => onChange({ priceListId: e.target.value })}>
          <option value="">Todas las evaluadas</option>
          {(filters?.price_lists ?? []).map((l) => (
            <option key={l.id} value={l.id}>
              {l.label ?? l.name} (#{l.id})
            </option>
          ))}
        </select>
      </label>
      <label className={labelClass}>
        Tipo de producto
        <select className={selectClass} value={draft.productTypeId} onChange={(e) => onChange({ productTypeId: e.target.value })}>
          <option value="">Todos</option>
          {(filters?.product_types ?? []).map((t) => (
            <option key={t.id} value={t.id}>
              {t.name ?? `Tipo ${t.id}`}
            </option>
          ))}
        </select>
      </label>
      <label className={labelClass}>
        Estado
        <select className={selectClass} value={draft.status} onChange={(e) => onChange({ status: e.target.value as MarginStatus | "" })}>
          <option value="">Todos</option>
          {PRIMARY_STATUSES.map((s) => (
            <option key={s} value={s}>
              {MARGIN_STATUS_LABELS[s]}
            </option>
          ))}
        </select>
      </label>
      <label className={labelClass}>
        Advertencia
        <select
          className={selectClass}
          value={draft.qualifier}
          onChange={(e) => onChange({ qualifier: e.target.value as MarginStatus | "" })}
        >
          <option value="">Todas</option>
          {QUALIFIER_STATUSES.map((s) => (
            <option key={s} value={s}>
              {MARGIN_STATUS_LABELS[s]}
            </option>
          ))}
        </select>
      </label>
      <label className={labelClass}>
        Confiabilidad costo
        <select
          className={selectClass}
          value={draft.reliability}
          onChange={(e) => onChange({ reliability: e.target.value as CostReliability | "" })}
        >
          <option value="">Todas</option>
          {(Object.keys(RELIABILITY_LABELS) as CostReliability[]).map((r) => (
            <option key={r} value={r}>
              {RELIABILITY_LABELS[r]}
            </option>
          ))}
        </select>
      </label>
      <label className={labelClass}>
        Base de costo
        <select className={selectClass} value={draft.costBasis} onChange={(e) => onChange({ costBasis: e.target.value as CostBasis })}>
          {(Object.keys(COST_BASIS_LABELS) as CostBasis[]).map((c) => (
            <option key={c} value={c}>
              {COST_BASIS_LABELS[c]}
            </option>
          ))}
        </select>
      </label>
      <label className={labelClass}>
        Orden
        <select className={selectClass} value={draft.sort} onChange={(e) => onChange({ sort: e.target.value })}>
          {Object.entries(MARGIN_SORT_LABELS).map(([k, v]) => (
            <option key={k} value={k}>
              {v}
            </option>
          ))}
        </select>
      </label>
      <div className="flex gap-2">
        <Button type="submit" disabled={loading}>
          {loading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
          Aplicar
        </Button>
        <Button type="button" variant="outline" onClick={onClear} disabled={loading}>
          Limpiar
        </Button>
      </div>
    </form>
  )
}

function StatusCell({ row }: { row: MarginRow }) {
  return (
    <div className="space-y-1">
      <span className={cn("inline-block rounded-md border px-1.5 py-0.5 text-[11px] font-medium", MARGIN_STATUS_TONES[row.status])}>
        {MARGIN_STATUS_LABELS[row.status]}
      </span>
      <span className="block text-[10px] uppercase tracking-wide text-muted-foreground">
        {row.confirmation === "confirmado" ? "Confirmado" : "Calculado"} · {row.validity.status}
      </span>
      {row.qualifiers.length ? (
        <div className="flex flex-wrap gap-1">
          {row.qualifiers.map((q) => (
            <span key={q} className={cn("rounded border px-1 py-px text-[10px]", MARGIN_STATUS_TONES[q])}>
              {MARGIN_STATUS_LABELS[q]}
            </span>
          ))}
        </div>
      ) : null}
      {row.reasons.length ? <p className="text-[11px] text-muted-foreground">{row.reasons.map(reasonLabel).join(" · ")}</p> : null}
      {row.risks?.some((r) => HIGHLIGHTED_RISKS.includes(r)) ? (
        <div className="flex flex-wrap gap-1">
          {row.risks
            .filter((r) => HIGHLIGHTED_RISKS.includes(r))
            .map((r) => (
              <span key={r} className="rounded border border-red-300 bg-red-50 px-1 py-px text-[10px] font-medium text-red-800">
                {MARGIN_RISK_LABELS[r]}
              </span>
            ))}
        </div>
      ) : null}
      {row.promotion.exception?.campaign_name && (row.status === "promocion_vigente" || row.status === "remate_vigente") ? (
        <p className="text-[10px] text-fuchsia-800">
          {row.promotion.exception.campaign_name} · aprobada por {row.promotion.exception.approved_by ?? "—"}
        </p>
      ) : null}
    </div>
  )
}

export function MarginV2Table({
  items,
  loading,
  canOpenCostHistory,
  onOpenCostHistory,
}: {
  items: MarginRow[]
  loading: boolean
  canOpenCostHistory: boolean
  onOpenCostHistory: (variantId: number) => void
}) {
  if (loading && items.length === 0) {
    return (
      <div className="flex justify-center py-10">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    )
  }
  return (
    <div className={cn("overflow-x-auto rounded-md border", loading && "opacity-60")}>
      <table className="w-full min-w-[1280px] text-sm">
        <thead className="bg-muted/40 text-left text-[11px] uppercase tracking-wide text-muted-foreground">
          <tr>
            <th className="px-3 py-2 font-medium">Producto</th>
            <th className="px-3 py-2 font-medium">Lista</th>
            <th className="px-3 py-2 font-medium">Estado</th>
            <th className="px-3 py-2 text-right font-medium">Precio bruto</th>
            <th className="px-3 py-2 text-right font-medium">Costo prom. neto</th>
            <th className="px-3 py-2 text-right font-medium">Costo ref. bruto</th>
            <th className="px-3 py-2 text-right font-medium">Recargo</th>
            <th className="px-3 py-2 text-right font-medium">Política mín / máx</th>
            <th className="px-3 py-2 text-right font-medium">Precio objetivo</th>
            <th className="px-3 py-2 font-medium">Confiabilidad</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border/60">
          {items.map((r) => (
            <tr key={`${r.variant_id}-${r.price_list.id}`} className="align-top">
              <td className="max-w-[260px] px-3 py-2">
                <span className="block truncate font-medium">{r.product_name ?? `Variante ${r.variant_id}`}</span>
                <span className="block truncate text-[11px] text-muted-foreground">
                  {r.code ?? "—"} · {r.barcode ?? "—"}
                  {r.variant_name ? ` · ${r.variant_name}` : ""}
                </span>
                <span className="block truncate text-[11px] text-muted-foreground">{r.product_type.name ?? "Sin tipo"}</span>
                {r.is_pack ? (
                  <span className="mt-1 inline-block rounded border border-indigo-300 bg-indigo-50 px-1 py-px text-[10px] font-medium text-indigo-800">
                    Pack Bsale · {r.pack?.components_count ?? 0} componentes
                  </span>
                ) : null}
                {canOpenCostHistory ? (
                  <button
                    type="button"
                    className="mt-1 inline-flex items-center gap-1 text-[11px] text-primary hover:underline"
                    onClick={() => onOpenCostHistory(r.variant_id)}
                  >
                    <History className="h-3 w-3" /> Historial de costos
                  </button>
                ) : null}
              </td>
              <td className="px-3 py-2 text-xs">
                <span className="font-medium">{r.price_list.label ?? r.price_list.name}</span>
                <span className="block text-[11px] text-muted-foreground">#{r.price_list.id}</span>
              </td>
              <td className="max-w-[220px] px-3 py-2">
                <StatusCell row={r} />
              </td>
              <td className="px-3 py-2 text-right tabular-nums">{formatMoney(r.price.gross)}</td>
              <td className="px-3 py-2 text-right tabular-nums">
                {r.is_pack ? (
                  <span className="text-[11px] text-muted-foreground">Suma de componentes</span>
                ) : (
                  formatMoney(r.costs.bsale_average_net)
                )}
              </td>
              <td className="px-3 py-2 text-right tabular-nums">
                {formatMoney(r.costs.reference.gross)}
                <span className="block text-[10px] text-muted-foreground">
                  {COST_BASIS_LABELS[r.costs.reference.basis]} · neto {formatMoney(r.costs.reference.net)}
                </span>
              </td>
              <td className="px-3 py-2 text-right tabular-nums">
                <span className={cn(r.status === "bajo_politica" && "font-semibold text-red-700")}>{formatPct(r.markup_pct)}</span>
                <span className="block text-[10px] text-muted-foreground">s/venta {formatPct(r.margin_on_sale_pct)}</span>
              </td>
              <td className="px-3 py-2 text-right tabular-nums">
                {r.rule ? (
                  <>
                    {formatPct(r.rule.min_markup_pct)} / {Number(r.rule.max_markup_pct) > 0 ? formatPct(r.rule.max_markup_pct) : "—"}
                    <span className="block text-[10px] text-muted-foreground">{r.rule.rule_key ?? `regla ${r.rule.rule_id}`}</span>
                    {r.rule.issues.map((i) => (
                      <span key={i} className="block text-[10px] text-amber-700">
                        {RULE_ISSUE_LABELS[i]}
                      </span>
                    ))}
                  </>
                ) : (
                  <span className="text-muted-foreground">Sin regla</span>
                )}
              </td>
              <td className="px-3 py-2 text-right tabular-nums">
                {r.target_price.min ? (
                  <>
                    {formatMoney(r.target_price.min)}
                    {r.target_price.provisional ? <span className="block text-[10px] text-amber-700">Provisional</span> : null}
                  </>
                ) : (
                  "—"
                )}
              </td>
              <td className="px-3 py-2">
                <span className={cn("rounded-md border px-1.5 py-0.5 text-[11px]", RELIABILITY_TONES[r.costs.reliability])}>
                  {RELIABILITY_LABELS[r.costs.reliability]}
                </span>
                {r.status === "bajo_politica" ? (
                  <p className="mt-1 max-w-[180px] text-[10px] text-muted-foreground">{PROMOTION_CHECK_LABELS[r.promotion.check]}</p>
                ) : null}
              </td>
            </tr>
          ))}
          {!loading && items.length === 0 ? (
            <tr>
              <td colSpan={10} className="px-3 py-8 text-center text-muted-foreground">
                Sin resultados con los filtros aplicados.
              </td>
            </tr>
          ) : null}
        </tbody>
      </table>
    </div>
  )
}
