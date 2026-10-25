"use client"

import { ArrowRight, Loader2, Search } from "lucide-react"

import { EmptyState, ProfessionalDataTable, SmartFilterBar } from "@/components/analytics-v2/shared/analytics-ui"
import { ProductIdentityCell } from "@/components/analytics-v2/shared/product-identity-cell"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { cn } from "@/lib/utils"
import { RELIABILITY_LABELS, formatMoney, formatPct, formatQty } from "@/lib/analytics-v2/labels"
import {
  COST_BASIS_LABELS,
  HIGHLIGHTED_RISKS,
  MARGIN_RISK_LABELS,
  MARGIN_SORT_LABELS,
  MARGIN_STATUS_LABELS,
  MARGIN_STATUS_TONES,
  PRIMARY_STATUSES,
  QUALIFIER_STATUSES,
} from "@/lib/analytics-v2/margins-labels"
import type { CostBasis, MarginRow, MarginStatus, MarginsFilters, StockFilter } from "@/lib/analytics-v2/margins-types"
import type { CostReliability } from "@/lib/analytics-v2/types"

export type MarginFilterDraft = {
  q: string
  priceListId: string
  productTypeId: string
  status: MarginStatus | ""
  qualifier: MarginStatus | ""
  reliability: CostReliability | ""
  stockFilter: StockFilter | ""
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
  stockFilter: "",
  costBasis: "promedio_bsale",
  sort: "status",
}

const selectClass = "h-10 rounded-lg border border-lq-line bg-lq-surface px-3 text-sm text-lq-body shadow-sm outline-none focus:border-primary/50 focus:ring-2 focus:ring-primary/15"
const labelClass = "flex flex-col gap-1.5 text-xs font-medium text-lq-body"

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
  const advanced = (
    <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6">
      <label className={labelClass}>
        Tipo de producto
        <select className={selectClass} value={draft.productTypeId} onChange={(e) => onChange({ productTypeId: e.target.value })}>
          <option value="">Todos</option>
          {(filters?.product_types ?? []).map((t) => <option key={t.id} value={t.id}>{t.name ?? `Tipo ${t.id}`}</option>)}
        </select>
      </label>
      <label className={labelClass}>
        Advertencia secundaria
        <select className={selectClass} value={draft.qualifier} onChange={(e) => onChange({ qualifier: e.target.value as MarginStatus | "" })}>
          <option value="">Todas</option>
          {QUALIFIER_STATUSES.map((s) => <option key={s} value={s}>{MARGIN_STATUS_LABELS[s]}</option>)}
        </select>
      </label>
      <label className={labelClass}>
        Confiabilidad del costo
        <select className={selectClass} value={draft.reliability} onChange={(e) => onChange({ reliability: e.target.value as CostReliability | "" })}>
          <option value="">Todas</option>
          {(Object.keys(RELIABILITY_LABELS) as CostReliability[]).map((r) => <option key={r} value={r}>{RELIABILITY_LABELS[r]}</option>)}
        </select>
      </label>
      <label className={labelClass}>
        Existencias
        <select className={selectClass} value={draft.stockFilter} onChange={(e) => onChange({ stockFilter: e.target.value as StockFilter | "" })}>
          <option value="">Todos</option>
          <option value="with">Con stock</option>
          <option value="without">Sin stock (0 o negativo)</option>
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
          {Object.entries(MARGIN_SORT_LABELS).map(([k, v]) => <option key={k} value={k}>{v}</option>)}
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
            Buscar producto
            <div className="relative">
              <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-lq-faint" />
              <Input className="h-10 rounded-lg border-lq-line pl-9 shadow-sm" value={draft.q} maxLength={100} onChange={(e) => onChange({ q: e.target.value })} placeholder="Nombre, SKU o código de barras" />
            </div>
          </label>
          <label className={cn(labelClass, "lg:min-w-52")}>
            Lista
            <select className={selectClass} value={draft.priceListId} onChange={(e) => onChange({ priceListId: e.target.value })}>
              <option value="">Todas las evaluadas</option>
              {(filters?.price_lists ?? []).map((l) => <option key={l.id} value={l.id}>{l.label ?? l.name} (#{l.id})</option>)}
            </select>
          </label>
          <label className={cn(labelClass, "lg:min-w-48")}>
            Estado comercial
            <select className={selectClass} value={draft.status} onChange={(e) => onChange({ status: e.target.value as MarginStatus | "" })}>
              <option value="">Todos</option>
              {PRIMARY_STATUSES.map((s) => <option key={s} value={s}>{MARGIN_STATUS_LABELS[s]}</option>)}
            </select>
          </label>
          <div className="flex gap-2">
            <Button type="submit" className="h-10" disabled={loading}>{loading ? <Loader2 className="h-4 w-4 animate-spin" /> : null}Aplicar</Button>
            <Button type="button" variant="outline" className="h-10" onClick={onClear} disabled={loading}>Limpiar</Button>
          </div>
        </div>
      </SmartFilterBar>
    </form>
  )
}

/** Stock físico total de la variante (todas las sucursales, sin desglose). */
function StockCell({ row }: { row: MarginRow }) {
  const stock = row.stock
  if (!stock || stock.state === "sin_sincronizar") {
    return (
      <span className="text-lq-faint" title="Sin información de stock sincronizada (no equivale a cero)">
        s/d
      </span>
    )
  }
  return (
    <span
      className={cn("font-medium tabular-nums", stock.state === "negativo" && "text-red-700 dark:text-red-300")}
      title="Stock físico total acumulado de la variante (todas las sucursales)"
    >
      {formatQty(stock.total)}
      {stock.state === "negativo" ? <span className="mt-0.5 block text-xs font-normal">negativo</span> : null}
    </span>
  )
}

function StatusCell({ row }: { row: MarginRow }) {
  const warnings = [
    ...row.qualifiers.map((q) => MARGIN_STATUS_LABELS[q]),
    ...row.risks.filter((r) => HIGHLIGHTED_RISKS.includes(r)).map((r) => MARGIN_RISK_LABELS[r]),
  ]
  return (
    <div>
      <span className={cn("inline-flex rounded-full border px-2.5 py-1 text-xs font-medium", MARGIN_STATUS_TONES[row.status])}>
        {MARGIN_STATUS_LABELS[row.status]}
      </span>
      <p className="mt-1.5 truncate text-xs text-lq-muted" title={warnings.join(" · ")}>
        {warnings.length ? `${warnings[0]}${warnings.length > 1 ? ` · +${warnings.length - 1}` : ""}` : row.validity.status === "provisional" ? "Resultado provisional" : "Sin advertencias"}
      </p>
    </div>
  )
}

export function MarginV2Table({
  items,
  loading,
  onOpenProduct,
}: {
  items: MarginRow[]
  loading: boolean
  onOpenProduct: (variantId: number) => void
}) {
  if (loading && items.length === 0) {
    return (
      <div className="flex justify-center py-10">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    )
  }
  return (
    <ProfessionalDataTable loading={loading}>
      <div className="divide-y divide-lq-line-soft lg:hidden">
        {items.map((r) => (
          <article key={`${r.variant_id}-${r.price_list.id}`} className="p-4">
            <div className="flex items-start justify-between gap-3">
              <ProductIdentityCell
                compact
                className="min-w-0 flex-1"
                productTypeName={r.product_type.name}
                productName={r.product_name}
                variantName={r.variant_name}
                barcode={r.barcode}
              />
              <StatusCell row={r} />
            </div>
            <p className="mt-1 text-xs text-lq-muted">{r.price_list.label ?? r.price_list.name}</p>
            <dl className="mt-4 grid grid-cols-2 gap-3 text-sm">
              <div><dt className="text-xs text-lq-muted">Precio bruto</dt><dd className="mt-0.5 font-medium tabular-nums">{formatMoney(r.price.gross)}</dd></div>
              <div><dt className="text-xs text-lq-muted">Costo bruto</dt><dd className="mt-0.5 font-medium tabular-nums">{formatMoney(r.costs.reference.gross)}</dd></div>
              <div><dt className="text-xs text-lq-muted">Stock Total</dt><dd className="mt-0.5"><StockCell row={r} /></dd></div>
              <div><dt className="text-xs text-lq-muted">Recargo actual</dt><dd className="mt-0.5 font-medium tabular-nums">{formatPct(r.markup_pct)}</dd></div>
              <div><dt className="text-xs text-lq-muted">Mínimo política</dt><dd className="mt-0.5 font-medium tabular-nums">{r.rule ? formatPct(r.rule.min_markup_pct) : "Sin regla"}</dd></div>
            </dl>
            <Button type="button" variant="outline" className="mt-4 w-full" onClick={() => onOpenProduct(r.variant_id)}>Ver análisis <ArrowRight className="h-4 w-4" /></Button>
          </article>
        ))}
      </div>
      <table className="hidden w-full min-w-[1120px] table-fixed text-sm lg:table">
        <thead className="border-b border-lq-line-soft bg-lq-canvas text-left text-xs text-lq-muted">
          <tr>
            <th className="w-[22%] px-5 py-3 font-medium">Identificación</th>
            <th className="w-[11%] px-3 py-3 font-medium">Lista</th>
            <th className="w-[11%] px-3 py-3 text-right font-medium">Precio bruto</th>
            <th className="w-[12%] px-3 py-3 text-right font-medium">Costo bruto</th>
            <th className="w-[9%] px-3 py-3 text-right font-medium">Stock Total</th>
            <th className="w-[10%] px-3 py-3 text-right font-medium">Recargo</th>
            <th className="w-[11%] px-3 py-3 text-right font-medium">Mín. política</th>
            <th className="w-[13%] px-3 py-3 font-medium">Estado comercial</th>
            <th className="w-[5%] px-4 py-3"><span className="sr-only">Acción</span></th>
          </tr>
        </thead>
        <tbody className="divide-y divide-lq-line-soft">
          {items.map((r) => (
            <tr key={`${r.variant_id}-${r.price_list.id}`} className="group hover:bg-lq-canvas">
              <td className="px-5 py-3.5">
                <button type="button" className="block max-w-full text-left" onClick={() => onOpenProduct(r.variant_id)}>
                  <ProductIdentityCell
                    productTypeName={r.product_type.name}
                    productName={r.product_name}
                    variantName={r.variant_name}
                    barcode={r.barcode}
                  />
                  {r.is_pack ? (
                    <span className="mt-0.5 block text-xs text-lq-muted">
                      Pack · {r.pack?.components_count ?? 0} componentes
                    </span>
                  ) : null}
                </button>
              </td>
              <td className="px-3 py-3.5 text-xs">
                <span className="font-medium text-lq-body">{r.price_list.label ?? r.price_list.name}</span>
                <span className="mt-0.5 block text-lq-muted">#{r.price_list.id}</span>
              </td>
              <td className="px-3 py-3.5 text-right font-medium tabular-nums">{formatMoney(r.price.gross)}</td>
              <td className="px-3 py-3.5 text-right font-medium tabular-nums">{formatMoney(r.costs.reference.gross)}</td>
              <td className="px-3 py-3.5 text-right"><StockCell row={r} /></td>
              <td className="px-3 py-3.5 text-right tabular-nums">
                <span className={cn(r.status === "bajo_politica" && "font-semibold text-red-700 dark:text-red-300")}>{formatPct(r.markup_pct)}</span>
                <span className="mt-0.5 block text-xs text-lq-muted">margen {formatPct(r.margin_on_sale_pct)}</span>
              </td>
              <td className="px-3 py-3.5 text-right tabular-nums">{r.rule ? formatPct(r.rule.min_markup_pct) : "Sin regla"}</td>
              <td className="px-3 py-3.5"><StatusCell row={r} /></td>
              <td className="px-4 py-3.5">
                <button type="button" aria-label={`Analizar ${r.product_name ?? `variante ${r.variant_id}`}`} onClick={() => onOpenProduct(r.variant_id)} className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-lq-line text-lq-muted hover:border-primary/30 hover:bg-lq-surface hover:text-primary focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/30"><ArrowRight className="h-4 w-4" /></button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {!loading && items.length === 0 ? <EmptyState title="Sin productos para mostrar" description="Prueba limpiando uno o más filtros para ampliar la búsqueda." /> : null}
    </ProfessionalDataTable>
  )
}
