"use client"

import { ArrowRight, Loader2, PackageSearch, Search } from "lucide-react"

import { EmptyState, ProfessionalDataTable, SmartFilterBar } from "@/components/analytics-v2/shared/analytics-ui"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { cn } from "@/lib/utils"
import { RELIABILITY_LABELS, formatMoney, formatPct } from "@/lib/analytics-v2/labels"
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

const selectClass = "h-10 rounded-lg border border-lq-line bg-lq-surface px-3 text-sm text-lq-body shadow-sm outline-none focus:border-primary/50 focus:ring-2 focus:ring-primary/15"
const labelClass = "flex flex-col gap-1.5 text-xs font-medium text-lq-body"

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
  const advanced = (
    <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
      <label className={labelClass}>
        Estado del costo
        <select className={selectClass} value={draft.packStatus} onChange={(e) => onChange({ packStatus: e.target.value as PackCostStatus | "" })}>
          <option value="">Todos</option>
          {(Object.keys(PACK_STATUS_LABELS) as PackCostStatus[]).map((s) => <option key={s} value={s}>{PACK_STATUS_LABELS[s]}</option>)}
        </select>
      </label>
      <label className={labelClass}>
        Estado comercial
        <select className={selectClass} value={draft.marginStatus} onChange={(e) => onChange({ marginStatus: e.target.value as MarginStatus | "" })}>
          <option value="">Todos</option>
          {PRIMARY_STATUSES.map((s) => <option key={s} value={s}>{MARGIN_STATUS_LABELS[s]}</option>)}
        </select>
      </label>
      <label className={labelClass}>
        Problema
        <select className={selectClass} value={draft.problem} onChange={(e) => onChange({ problem: e.target.value as PackProblem | "" })}>
          <option value="">Todos</option>
          {(Object.keys(PACK_PROBLEM_LABELS) as PackProblem[]).map((p) => <option key={p} value={p}>{PACK_PROBLEM_LABELS[p]}</option>)}
        </select>
      </label>
      <label className={labelClass}>
        Vigencia
        <select className={selectClass} value={draft.packState} onChange={(e) => onChange({ packState: e.target.value as PackFilterDraft["packState"] })}>
          <option value="active">Activos</option>
          <option value="inactive">Inactivos / históricos</option>
          <option value="all">Todos</option>
        </select>
      </label>
      <label className={labelClass}>
        Base de costo
        <select className={selectClass} value={draft.costBasis} onChange={(e) => onChange({ costBasis: e.target.value as CostBasis })}>
          {(Object.keys(COST_BASIS_LABELS) as CostBasis[]).map((c) => <option key={c} value={c}>{COST_BASIS_LABELS[c]}</option>)}
        </select>
      </label>
      <label className={labelClass}>
        Orden
        <select className={selectClass} value={draft.sort} onChange={(e) => onChange({ sort: e.target.value })}>
          {Object.entries(PACK_SORT_LABELS).map(([k, v]) => <option key={k} value={k}>{v}</option>)}
        </select>
      </label>
    </div>
  )
  return (
    <form
      onSubmit={(e) => {
        e.preventDefault()
        onApply()
      }}
    >
      <SmartFilterBar advanced={advanced}>
        <div className="flex flex-col gap-3 lg:flex-row lg:items-end">
          <label className={cn(labelClass, "min-w-0 flex-1")}>
            Buscar pack
            <div className="relative">
              <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-lq-faint" />
              <Input className="h-10 rounded-lg border-lq-line pl-9 text-sm shadow-sm" value={draft.q} maxLength={100} onChange={(e) => onChange({ q: e.target.value })} placeholder="Nombre, SKU o código" />
            </div>
          </label>
          <label className={cn(labelClass, "lg:min-w-56")}>
            Lista de precios
            <select className={selectClass} value={draft.priceListId} onChange={(e) => onChange({ priceListId: e.target.value })}>
              <option value="">Todas las listas</option>
              {priceLists.map((l) => <option key={l.id} value={l.id}>{l.label ?? l.name} (#{l.id})</option>)}
            </select>
          </label>
          <div className="flex gap-2">
            <Button type="submit" className="h-10" disabled={loading}>
              {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
              Aplicar
            </Button>
            <Button type="button" variant="outline" className="h-10" onClick={onClear} disabled={loading}>Limpiar</Button>
          </div>
        </div>
      </SmartFilterBar>
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
  const state = (r: PackRow) =>
    r.pack_status === "calculado" ? (
      <span className={cn("inline-flex rounded-full border px-2.5 py-1 text-xs font-medium", MARGIN_STATUS_TONES[r.margin_status])}>
        {MARGIN_STATUS_LABELS[r.margin_status]}
      </span>
    ) : (
      <PackStatusBadge status={r.pack_status} />
    )
  return (
    <ProfessionalDataTable loading={loading}>
      <div className="divide-y divide-lq-line-soft md:hidden">
        {items.map((r) => (
          <article key={`${r.variant_id}-${r.price_list.id}`} className="p-4">
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <p className="truncate text-sm font-semibold text-lq-ink">{r.product_name ?? `Variante ${r.variant_id}`}</p>
                <p className="mt-1 truncate text-xs text-lq-muted">{r.code ?? "Sin SKU"} · {r.price_list.label ?? r.price_list.name}</p>
              </div>
              {state(r)}
            </div>
            <dl className="mt-4 grid grid-cols-2 gap-x-5 gap-y-3">
              <div><dt className="text-xs text-lq-muted">Precio bruto</dt><dd className="mt-0.5 font-medium tabular-nums">{formatMoney(r.price_gross)}</dd></div>
              <div><dt className="text-xs text-lq-muted">Costo bruto</dt><dd className="mt-0.5 font-medium tabular-nums">{formatMoney(r.cost_gross_total)}</dd></div>
              <div><dt className="text-xs text-lq-muted">Recargo</dt><dd className="mt-0.5 font-medium tabular-nums">{formatPct(r.markup_pct)}</dd></div>
              <div><dt className="text-xs text-lq-muted">Margen sobre venta</dt><dd className="mt-0.5 font-medium tabular-nums">{formatPct(r.margin_on_sale_pct)}</dd></div>
            </dl>
            <button type="button" onClick={() => onOpen(r.variant_id)} className="mt-4 inline-flex h-9 w-full items-center justify-center gap-2 rounded-lg border border-lq-line text-sm font-medium text-lq-body hover:bg-lq-canvas">
              Ver análisis completo <ArrowRight className="h-4 w-4" />
            </button>
          </article>
        ))}
      </div>
      <table className="hidden w-full min-w-[920px] table-fixed text-sm md:table">
        <thead className="border-b border-lq-line-soft bg-lq-canvas text-left text-xs text-lq-muted">
          <tr>
            <th className="w-[30%] px-5 py-3 font-medium">Pack / SKU</th>
            <th className="w-[13%] px-3 py-3 text-right font-medium">Precio</th>
            <th className="w-[14%] px-3 py-3 text-right font-medium">Costo bruto</th>
            <th className="w-[11%] px-3 py-3 text-right font-medium">Recargo</th>
            <th className="w-[12%] px-3 py-3 text-right font-medium">Margen venta</th>
            <th className="w-[14%] px-3 py-3 font-medium">Estado</th>
            <th className="w-[6%] px-4 py-3 text-right font-medium"><span className="sr-only">Acción</span></th>
          </tr>
        </thead>
        <tbody className="divide-y divide-lq-line-soft">
          {items.map((r) => (
            <tr key={`${r.variant_id}-${r.price_list.id}`} className="group transition-colors hover:bg-lq-canvas">
              <td className="px-5 py-3.5">
                <button
                  type="button"
                  className="flex max-w-full items-start gap-2 text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/30"
                  onClick={() => onOpen(r.variant_id)}
                >
                  <span className="mt-0.5 rounded-md bg-lq-canvas p-1.5 text-lq-muted group-hover:bg-lq-surface"><PackageSearch className="h-4 w-4" /></span>
                  <span className="min-w-0">
                    <span className="block truncate font-medium text-lq-ink group-hover:text-primary">{r.product_name ?? `Variante ${r.variant_id}`}</span>
                    <span className="mt-0.5 block truncate text-xs font-normal text-lq-muted">
                      {r.code ?? "Sin SKU"} · #{r.variant_id} · {r.components_count} componentes
                    </span>
                  </span>
                </button>
              </td>
              <td className="px-3 py-3.5 text-right tabular-nums">
                <span className="font-medium text-lq-ink">{formatMoney(r.price_gross)}</span>
                <span className="mt-0.5 block truncate text-xs text-lq-muted">{r.price_list.label ?? r.price_list.name}</span>
              </td>
              <td className="px-3 py-3.5 text-right font-medium tabular-nums text-lq-ink">{formatMoney(r.cost_gross_total)}</td>
              <td className="px-3 py-3.5 text-right tabular-nums">
                <span className={cn(r.margin_status === "bajo_politica" && "font-semibold text-red-700 dark:text-red-300")}>{formatPct(r.markup_pct)}</span>
                {r.rule ? <span className="mt-0.5 block text-xs text-lq-muted">mín. {formatPct(r.rule.min_markup_pct)}</span> : null}
              </td>
              <td className="px-3 py-3.5 text-right font-medium tabular-nums text-lq-ink">{formatPct(r.margin_on_sale_pct)}</td>
              <td className="px-3 py-3.5">
                {state(r)}
                <span className="mt-1.5 block truncate text-xs text-lq-muted">
                  {r.validity.status === "provisional" ? "Provisional" : "Validado"} · {RELIABILITY_LABELS[r.reliability]}
                </span>
              </td>
              <td className="px-4 py-3.5 text-right">
                <button type="button" title="Ver análisis completo" aria-label={`Ver detalle de ${r.product_name ?? `variante ${r.variant_id}`}`} onClick={() => onOpen(r.variant_id)} className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-lq-line text-lq-muted transition hover:border-primary/30 hover:bg-lq-surface hover:text-primary focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/30">
                  <ArrowRight className="h-4 w-4" />
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {!loading && items.length === 0 ? <EmptyState title="No encontramos packs" description="Prueba limpiando uno o más filtros para ampliar la búsqueda." /> : null}
    </ProfessionalDataTable>
  )
}
