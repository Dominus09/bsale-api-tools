"use client"

import { ArrowRight, ChevronDown, ChevronLeft, ChevronRight, Loader2, Search } from "lucide-react"

import { EmptyState, ProfessionalDataTable, SmartFilterBar } from "@/components/analytics-v2/shared/analytics-ui"
import { ProductIdentityCell } from "@/components/analytics-v2/shared/product-identity-cell"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { formatOfficeLabel } from "@/lib/analytics-v2/display-labels"
import { cn } from "@/lib/utils"
import {
  ANOMALY_LABELS,
  MOVEMENT_LABELS,
  RELIABILITY_LABELS,
  SORT_LABELS,
  TAX_LABELS,
  formatDate,
  formatMoney,
  formatPct,
  toNumber,
} from "@/lib/analytics-v2/labels"
import type {
  AnomalyCode,
  CostFilters,
  CostProductItem,
  CostReliability,
  MovementClass,
  TaxProfileStatus,
} from "@/lib/analytics-v2/types"

import { MovementBadge, ReliabilityBadge } from "./badges"

import type { StockFilter } from "@/lib/analytics-v2/stock-filter"
import { DEFAULT_STOCK_FILTER, STOCK_FILTER_OPTIONS } from "@/lib/analytics-v2/stock-filter"

export type ProductFilterDraft = {
  q: string
  productTypeId: string
  variantState: "active" | "inactive" | "all"
  reliability: CostReliability | ""
  taxStatus: TaxProfileStatus | ""
  movement: MovementClass | ""
  anomaly: AnomalyCode | ""
  officeId: string
  minChange: string
  stockFilter: StockFilter
  sort: string
}

export const EMPTY_PRODUCT_FILTERS: ProductFilterDraft = {
  q: "",
  productTypeId: "",
  variantState: "active",
  reliability: "",
  taxStatus: "",
  movement: "",
  anomaly: "",
  officeId: "",
  minChange: "",
  stockFilter: DEFAULT_STOCK_FILTER,
  sort: "reliability",
}

const selectClass =
  "h-10 w-full rounded-lg border border-lq-line bg-lq-surface px-3 text-sm shadow-sm outline-none focus:border-primary/50 focus:ring-2 focus:ring-primary/15"

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="flex min-w-0 flex-col gap-1.5 text-xs font-medium text-lq-body">
      {label}
      {children}
    </label>
  )
}

export function CostV2ProductFilters({
  draft,
  filters,
  loading,
  onChange,
  onApply,
  onClear,
}: {
  draft: ProductFilterDraft
  filters: CostFilters | null
  loading: boolean
  onChange: (patch: Partial<ProductFilterDraft>) => void
  onApply: () => void
  onClear: () => void
}) {
  return (
    <form
      onSubmit={(e) => {
        e.preventDefault()
        onApply()
      }}
    >
      <SmartFilterBar>
      <div className="flex flex-col gap-3 sm:flex-row">
        <div className="relative min-w-0 flex-1">
          <Search className="pointer-events-none absolute left-3 top-3 h-4 w-4 text-lq-faint" />
          <Input
            value={draft.q}
            onChange={(e) => onChange({ q: e.target.value })}
            placeholder="Buscar por SKU, código de barras o nombre"
            className="h-10 rounded-lg border-lq-line pl-9 shadow-sm"
            maxLength={100}
          />
        </div>
        <div className="flex gap-2">
          <Button type="submit" className="h-10" disabled={loading}>
            {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
            Buscar
          </Button>
          <Button type="button" variant="outline" className="h-10" onClick={onClear} disabled={loading}>
            Limpiar
          </Button>
        </div>
      </div>
      <details className="group border-t border-lq-line-soft pt-3">
        <summary className="inline-flex cursor-pointer list-none items-center gap-1.5 text-xs font-medium text-lq-body hover:text-lq-ink [&::-webkit-details-marker]:hidden">
          Filtros avanzados <ChevronDown className="h-3.5 w-3.5 transition-transform group-open:rotate-180" />
        </summary>
        <div className="mt-3 space-y-3">
      <div className="grid grid-cols-2 gap-2 md:grid-cols-4 xl:grid-cols-8">
        <Field label="Tipo de producto">
          <select
            className={selectClass}
            value={draft.productTypeId}
            onChange={(e) => onChange({ productTypeId: e.target.value })}
          >
            <option value="">Todos</option>
            {(filters?.product_types ?? []).map((t) => (
              <option key={t.id} value={t.id}>
                {t.name ?? `Tipo ${t.id}`}
              </option>
            ))}
          </select>
        </Field>
        <Field label="Estado SKU">
          <select
            className={selectClass}
            value={draft.variantState}
            onChange={(e) => onChange({ variantState: e.target.value as ProductFilterDraft["variantState"] })}
          >
            <option value="active">Activos</option>
            <option value="inactive">Inactivos</option>
            <option value="all">Todos</option>
          </select>
        </Field>
        <Field label="Existencias (global)">
          <select
            className={selectClass}
            value={draft.stockFilter}
            onChange={(e) => onChange({ stockFilter: e.target.value as StockFilter })}
          >
            {STOCK_FILTER_OPTIONS.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </Field>
        <Field label="Confiabilidad">
          <select
            className={selectClass}
            value={draft.reliability}
            onChange={(e) => onChange({ reliability: e.target.value as CostReliability | "" })}
          >
            <option value="">Todas</option>
            {(Object.keys(RELIABILITY_LABELS) as CostReliability[]).map((k) => (
              <option key={k} value={k}>
                {RELIABILITY_LABELS[k]}
              </option>
            ))}
          </select>
        </Field>
        <Field label="Anomalía">
          <select
            className={selectClass}
            value={draft.anomaly}
            onChange={(e) => onChange({ anomaly: e.target.value as AnomalyCode | "" })}
          >
            <option value="">Todas</option>
            {(Object.keys(ANOMALY_LABELS) as AnomalyCode[]).map((k) => (
              <option key={k} value={k}>
                {ANOMALY_LABELS[k]}
              </option>
            ))}
          </select>
        </Field>
        <Field label="Último movimiento">
          <select
            className={selectClass}
            value={draft.movement}
            onChange={(e) => onChange({ movement: e.target.value as MovementClass | "" })}
          >
            <option value="">Todos</option>
            {(Object.keys(MOVEMENT_LABELS) as MovementClass[]).map((k) => (
              <option key={k} value={k}>
                {MOVEMENT_LABELS[k]}
              </option>
            ))}
          </select>
        </Field>
        <Field label="Impuestos">
          <select
            className={selectClass}
            value={draft.taxStatus}
            onChange={(e) => onChange({ taxStatus: e.target.value as TaxProfileStatus | "" })}
          >
            <option value="">Todos</option>
            {(Object.keys(TAX_LABELS) as TaxProfileStatus[]).map((k) => (
              <option key={k} value={k}>
                {TAX_LABELS[k]}
              </option>
            ))}
          </select>
        </Field>
        <Field label="Sucursal compra">
          <select
            className={selectClass}
            value={draft.officeId}
            onChange={(e) => onChange({ officeId: e.target.value })}
          >
            <option value="">Todas</option>
            {(filters?.offices ?? []).map((o) => (
              <option key={o.id} value={o.id}>
                {formatOfficeLabel(o.name, o.id)}
              </option>
            ))}
          </select>
        </Field>
        <Field label="Variación mín. %">
          <Input
            inputMode="decimal"
            value={draft.minChange}
            onChange={(e) => onChange({ minChange: e.target.value.replace(/[^0-9.]/g, "") })}
            placeholder="Ej: 10"
          />
        </Field>
      </div>
      <div className="flex items-center gap-2">
        <span className="text-xs font-medium text-lq-body">Orden</span>
        <select
          className={cn(selectClass, "w-auto")}
          value={draft.sort}
          onChange={(e) => onChange({ sort: e.target.value })}
        >
          {Object.entries(SORT_LABELS).map(([k, label]) => (
            <option key={k} value={k}>
              {label}
            </option>
          ))}
        </select>
      </div>
        </div>
      </details>
      </SmartFilterBar>
    </form>
  )
}

export function Pager({
  page,
  totalPages,
  total,
  loading,
  onPage,
}: {
  page: number
  totalPages: number
  total: number
  loading?: boolean
  onPage: (page: number) => void
}) {
  return (
    <div className="flex items-center justify-between gap-2 text-sm">
      <span className="text-muted-foreground">
        {total} registro{total === 1 ? "" : "s"} · página {totalPages ? page : 0} de {totalPages}
      </span>
      <div className="flex gap-1">
        <Button
          type="button"
          size="sm"
          variant="outline"
          className="h-10 w-10 p-0 sm:h-8 sm:w-8"
          disabled={loading || page <= 1}
          onClick={() => onPage(page - 1)}
          aria-label="Página anterior"
        >
          <ChevronLeft className="h-4 w-4" />
        </Button>
        <Button
          type="button"
          size="sm"
          variant="outline"
          className="h-10 w-10 p-0 sm:h-8 sm:w-8"
          disabled={loading || page >= totalPages}
          onClick={() => onPage(page + 1)}
          aria-label="Página siguiente"
        >
          <ChevronRight className="h-4 w-4" />
        </Button>
      </div>
    </div>
  )
}

function ChangeCell({ item }: { item: CostProductItem }) {
  const pct = toNumber(item.change?.pct ?? null)
  if (pct === null) return <span className="text-muted-foreground">—</span>
  return (
    <span
      className={cn(
        "tabular-nums",
        pct > 0 ? "text-red-700 dark:text-red-300" : pct < 0 ? "text-emerald-700 dark:text-emerald-300" : "text-muted-foreground",
      )}
    >
      {formatPct(item.change?.pct ?? null)}
      <span className="block text-[11px] text-muted-foreground">{formatMoney(item.change?.amount ?? null)}</span>
    </span>
  )
}

export function CostV2ProductsTable({
  items,
  loading,
  onOpen,
}: {
  items: CostProductItem[]
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
  if (items.length === 0) {
    return <EmptyState title="Sin productos para mostrar" description="Prueba limpiando uno o más filtros para ampliar la búsqueda." />
  }
  return (
    <ProfessionalDataTable loading={loading}>
      <div className="divide-y divide-lq-line-soft md:hidden">
        {items.map((item) => (
          <article key={item.variant_id} className="p-4">
            <div className="flex items-start justify-between gap-3">
              <ProductIdentityCell
                compact
                className="min-w-0 flex-1"
                productTypeName={item.product_type.name}
                productName={item.product_name}
                variantName={item.variant_name}
                barcode={item.barcode}
              />
              <ReliabilityBadge status={item.reliability.status} />
            </div>
            <dl className="mt-4 grid grid-cols-2 gap-3 text-sm">
              <div><dt className="text-xs text-lq-muted">Promedio Bsale</dt><dd className="mt-0.5 font-medium tabular-nums">{formatMoney(item.bsale_average_cost.value)}</dd></div>
              <div><dt className="text-xs text-lq-muted">Última compra</dt><dd className="mt-0.5 font-medium tabular-nums">{formatMoney(item.last_comparable?.cost ?? null)}</dd></div>
              <div><dt className="text-xs text-lq-muted">Variación</dt><dd className="mt-0.5 font-medium"><ChangeCell item={item} /></dd></div>
              <div><dt className="text-xs text-lq-muted">Movimiento</dt><dd className="mt-0.5">{item.last_reception ? <MovementBadge movement={item.last_reception.movement_class} /> : "Sin recepciones"}</dd></div>
            </dl>
            <Button type="button" variant="outline" className="mt-4 w-full" onClick={() => onOpen(item.variant_id)}>Analizar costo <ArrowRight className="h-4 w-4" /></Button>
          </article>
        ))}
      </div>
      <table className="hidden w-full min-w-[960px] table-fixed text-sm md:table">
        <thead className="border-b border-lq-line-soft bg-lq-canvas text-left text-xs text-lq-muted">
          <tr>
            <th className="w-[29%] px-5 py-3 font-medium">Identificación</th>
            <th className="w-[14%] px-3 py-3 text-right font-medium">Promedio Bsale</th>
            <th className="w-[17%] px-3 py-3 text-right font-medium">Última compra</th>
            <th className="w-[12%] px-3 py-3 text-right font-medium">Variación</th>
            <th className="w-[13%] px-3 py-3 font-medium">Movimiento</th>
            <th className="w-[15%] px-3 py-3 font-medium">Estado</th>
            <th className="w-[6%] px-4 py-3"><span className="sr-only">Acción</span></th>
          </tr>
        </thead>
        <tbody className="divide-y divide-lq-line-soft">
          {items.map((item) => (
            <tr
              key={item.variant_id}
              className="group cursor-pointer hover:bg-lq-canvas"
              onClick={() => onOpen(item.variant_id)}
            >
              <td className="px-5 py-3.5">
                <ProductIdentityCell
                  productTypeName={item.product_type.name}
                  productName={item.product_name}
                  variantName={item.variant_name}
                  barcode={item.barcode}
                />
                {item.variant_state !== "activa" ? (
                  <p className="mt-0.5 text-xs text-lq-muted">Inactivo{item.code ? ` · SKU ${item.code}` : ""}</p>
                ) : item.code ? (
                  <p className="mt-0.5 truncate text-xs text-lq-muted" title={`SKU ${item.code}`}>SKU {item.code}</p>
                ) : null}
              </td>
              <td className="px-3 py-3.5 text-right font-medium tabular-nums">
                {formatMoney(item.bsale_average_cost.value)}
              </td>
              <td className="px-3 py-3.5 text-right tabular-nums">
                {formatMoney(item.last_comparable?.cost ?? null)}
                <span className="mt-0.5 block text-xs text-lq-muted">
                  {formatDate(item.last_comparable?.admission_date)}
                  {item.last_comparable?.office_name ? ` · ${item.last_comparable.office_name}` : ""}
                </span>
              </td>
              <td className="px-3 py-3.5 text-right">
                <ChangeCell item={item} />
              </td>
              <td className="px-3 py-3.5">
                {item.last_reception ? (
                  <MovementBadge movement={item.last_reception.movement_class} />
                ) : (
                  <span className="text-xs text-muted-foreground">Sin recepciones</span>
                )}
              </td>
              <td className="px-3 py-3.5">
                <ReliabilityBadge status={item.reliability.status} title={item.reliability.reason} />
                <p className="mt-1.5 truncate text-xs text-lq-muted" title={item.anomalies.map((a) => ANOMALY_LABELS[a.code]).join(" · ")}>
                  {item.anomalies.length ? `${ANOMALY_LABELS[item.anomalies[0].code]}${item.anomalies.length > 1 ? ` · +${item.anomalies.length - 1}` : ""}` : "Sin alertas"}
                </p>
              </td>
              <td className="px-4 py-3.5">
                <span className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-lq-line text-lq-muted group-hover:border-primary/30 group-hover:bg-lq-surface group-hover:text-primary"><ArrowRight className="h-4 w-4" /></span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </ProfessionalDataTable>
  )
}
