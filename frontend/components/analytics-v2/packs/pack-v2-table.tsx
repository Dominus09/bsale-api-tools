"use client"

import { Loader2, PackageSearch } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { cn } from "@/lib/utils"
import { RELIABILITY_LABELS, RELIABILITY_TONES, formatDateTime, formatMoney, formatPct } from "@/lib/analytics-v2/labels"
import { COST_BASIS_LABELS, MARGIN_STATUS_LABELS, MARGIN_STATUS_TONES, PRIMARY_STATUSES } from "@/lib/analytics-v2/margins-labels"
import type { CostBasis, MarginStatus } from "@/lib/analytics-v2/margins-types"
import { PACK_PROBLEM_LABELS, PACK_SORT_LABELS, PACK_STATUS_LABELS, PACK_STATUS_TONES } from "@/lib/analytics-v2/packs-labels"
import type { PackCostStatus, PackProblem, PackRow } from "@/lib/analytics-v2/packs-types"

export type PackFilterDraft = {
  q: string
  priceListId: string
  packStatus: PackCostStatus | ""
  marginStatus: MarginStatus | ""
  problem: PackProblem | ""
  packState: "active" | "inactive" | "all"
  costBasis: CostBasis
  sort: string
}

export const EMPTY_PACK_FILTERS: PackFilterDraft = {
  q: "",
  priceListId: "",
  packStatus: "",
  marginStatus: "",
  problem: "",
  packState: "active",
  costBasis: "promedio_bsale",
  sort: "status",
}

const selectClass = "h-9 rounded-md border border-input bg-background px-2 text-sm"
const labelClass = "flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground"

export function PackV2Filters({
  draft,
  priceLists,
  loading,
  onChange,
  onApply,
  onClear,
}: {
  draft: PackFilterDraft
  priceLists: { id: number; name: string | null; label: string | null }[]
  loading: boolean
  onChange: (patch: Partial<PackFilterDraft>) => void
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
        Buscar pack, SKU o código
        <Input value={draft.q} maxLength={100} onChange={(e) => onChange({ q: e.target.value })} placeholder="Ej: Mistral" />
      </label>
      <label className={labelClass}>
        Lista
        <select className={selectClass} value={draft.priceListId} onChange={(e) => onChange({ priceListId: e.target.value })}>
          <option value="">Todas las evaluadas</option>
          {priceLists.map((l) => (
            <option key={l.id} value={l.id}>
              {l.label ?? l.name} (#{l.id})
            </option>
          ))}
        </select>
      </label>
      <label className={labelClass}>
        Costo del pack
        <select
          className={selectClass}
          value={draft.packStatus}
          onChange={(e) => onChange({ packStatus: e.target.value as PackCostStatus | "" })}
        >
          <option value="">Todos</option>
          {(Object.keys(PACK_STATUS_LABELS) as PackCostStatus[]).map((s) => (
            <option key={s} value={s}>
              {PACK_STATUS_LABELS[s]}
            </option>
          ))}
        </select>
      </label>
      <label className={labelClass}>
        Margen
        <select
          className={selectClass}
          value={draft.marginStatus}
          onChange={(e) => onChange({ marginStatus: e.target.value as MarginStatus | "" })}
        >
          <option value="">Todos</option>
          {PRIMARY_STATUSES.map((s) => (
            <option key={s} value={s}>
              {MARGIN_STATUS_LABELS[s]}
            </option>
          ))}
        </select>
      </label>
      <label className={labelClass}>
        Problema
        <select className={selectClass} value={draft.problem} onChange={(e) => onChange({ problem: e.target.value as PackProblem | "" })}>
          <option value="">Todos</option>
          {(Object.keys(PACK_PROBLEM_LABELS) as PackProblem[]).map((p) => (
            <option key={p} value={p}>
              {PACK_PROBLEM_LABELS[p]}
            </option>
          ))}
        </select>
      </label>
      <label className={labelClass}>
        Packs
        <select
          className={selectClass}
          value={draft.packState}
          onChange={(e) => onChange({ packState: e.target.value as PackFilterDraft["packState"] })}
        >
          <option value="active">Activos</option>
          <option value="inactive">Inactivos / históricos</option>
          <option value="all">Todos</option>
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
          {Object.entries(PACK_SORT_LABELS).map(([k, v]) => (
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

export function PackStatusBadge({ status }: { status: PackCostStatus | null }) {
  if (!status) return <span className="text-muted-foreground">—</span>
  return (
    <span className={cn("inline-block rounded-md border px-1.5 py-0.5 text-[11px] font-medium", PACK_STATUS_TONES[status])}>
      {PACK_STATUS_LABELS[status]}
    </span>
  )
}

export function PackV2Table({
  items,
  loading,
  onOpen,
}: {
  items: PackRow[]
  loading: boolean
  onOpen: (variantId: number) => void
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
      <table className="w-full min-w-[1400px] text-sm">
        <thead className="bg-muted/40 text-left text-[11px] uppercase tracking-wide text-muted-foreground">
          <tr>
            <th className="px-3 py-2 font-medium">Pack</th>
            <th className="px-3 py-2 font-medium">SKU</th>
            <th className="px-3 py-2 font-medium">Estado</th>
            <th className="px-3 py-2 text-right font-medium">Componentes</th>
            <th className="px-3 py-2 font-medium">Lista</th>
            <th className="px-3 py-2 text-right font-medium">Precio bruto</th>
            <th className="px-3 py-2 text-right font-medium">Costo neto total</th>
            <th className="px-3 py-2 text-right font-medium">Costo bruto comercial</th>
            <th className="px-3 py-2 text-right font-medium">Recargo %</th>
            <th className="px-3 py-2 text-right font-medium">Margen s/venta</th>
            <th className="px-3 py-2 font-medium">Confiabilidad</th>
            <th className="px-3 py-2 font-medium">Actualización</th>
            <th className="px-3 py-2 font-medium">Costos faltantes</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border/60">
          {items.map((r) => (
            <tr key={`${r.variant_id}-${r.price_list.id}`} className="align-top">
              <td className="max-w-[260px] px-3 py-2">
                <button
                  type="button"
                  className="inline-flex items-start gap-1 text-left font-medium text-primary hover:underline"
                  onClick={() => onOpen(r.variant_id)}
                >
                  <PackageSearch className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                  <span className="truncate">{r.product_name ?? `Variante ${r.variant_id}`}</span>
                </button>
                {r.variant_name ? <span className="block truncate text-[11px] text-muted-foreground">{r.variant_name}</span> : null}
                {r.problems.length ? (
                  <div className="mt-1 flex flex-wrap gap-1">
                    {r.problems.map((p) => (
                      <span key={p} className="rounded border border-red-200 bg-red-50 px-1 py-px text-[10px] text-red-800">
                        {PACK_PROBLEM_LABELS[p]}
                      </span>
                    ))}
                  </div>
                ) : null}
              </td>
              <td className="px-3 py-2 text-xs">
                {r.code ?? "—"}
                <span className="block text-[11px] text-muted-foreground">#{r.variant_id}</span>
              </td>
              <td className="max-w-[200px] px-3 py-2 space-y-1">
                <PackStatusBadge status={r.pack_status} />
                <span className={cn("block w-fit rounded-md border px-1.5 py-0.5 text-[11px]", MARGIN_STATUS_TONES[r.margin_status])}>
                  {MARGIN_STATUS_LABELS[r.margin_status]}
                </span>
                {r.pack_state === "inactivo" ? <span className="block text-[10px] text-muted-foreground">Pack inactivo</span> : null}
              </td>
              <td className="px-3 py-2 text-right tabular-nums">{r.components_count}</td>
              <td className="px-3 py-2 text-xs">
                <span className="font-medium">{r.price_list.label ?? r.price_list.name}</span>
                <span className="block text-[11px] text-muted-foreground">#{r.price_list.id}</span>
              </td>
              <td className="px-3 py-2 text-right tabular-nums">{formatMoney(r.price_gross)}</td>
              <td className="px-3 py-2 text-right tabular-nums">{formatMoney(r.cost_net_total)}</td>
              <td className="px-3 py-2 text-right tabular-nums">{formatMoney(r.cost_gross_total)}</td>
              <td className="px-3 py-2 text-right tabular-nums">
                <span className={cn(r.margin_status === "bajo_politica" && "font-semibold text-red-700")}>{formatPct(r.markup_pct)}</span>
                {r.rule ? (
                  <span className="block text-[10px] text-muted-foreground">mín {formatPct(r.rule.min_markup_pct)}</span>
                ) : null}
              </td>
              <td className="px-3 py-2 text-right tabular-nums">{formatPct(r.margin_on_sale_pct)}</td>
              <td className="px-3 py-2">
                <span className={cn("rounded-md border px-1.5 py-0.5 text-[11px]", RELIABILITY_TONES[r.reliability])}>
                  {RELIABILITY_LABELS[r.reliability]}
                </span>
                <span className="mt-1 block text-[10px] uppercase text-muted-foreground">{r.validity.status}</span>
              </td>
              <td className="px-3 py-2 text-[11px] text-muted-foreground">
                Precio {formatDateTime(r.updated_at.price_fetched_at)}
                <span className="block">Composición {formatDateTime(r.updated_at.composition_fetched_at)}</span>
              </td>
              <td className="px-3 py-2 text-[11px]">
                {r.missing_cost_variant_ids.length || r.missing_variant_ids.length ? (
                  <>
                    {r.missing_cost_variant_ids.length ? (
                      <span className="block text-red-700">Sin costo: {r.missing_cost_variant_ids.map((id) => `#${id}`).join(", ")}</span>
                    ) : null}
                    {r.missing_variant_ids.length ? (
                      <span className="block text-red-700">Inexistentes: {r.missing_variant_ids.map((id) => `#${id}`).join(", ")}</span>
                    ) : null}
                  </>
                ) : (
                  <span className="text-muted-foreground">—</span>
                )}
              </td>
            </tr>
          ))}
          {!loading && items.length === 0 ? (
            <tr>
              <td colSpan={13} className="px-3 py-8 text-center text-muted-foreground">
                Sin packs con los filtros aplicados.
              </td>
            </tr>
          ) : null}
        </tbody>
      </table>
    </div>
  )
}
