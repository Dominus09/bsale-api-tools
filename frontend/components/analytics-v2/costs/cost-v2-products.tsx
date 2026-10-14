"use client"

import { ChevronLeft, ChevronRight, Loader2, Search } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
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

import { AnomalyChips, MovementBadge, ReliabilityBadge } from "./badges"

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
  sort: "reliability",
}

const selectClass =
  "h-9 w-full rounded-md border border-input bg-background px-2 text-sm shadow-xs focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/50"

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="flex min-w-0 flex-col gap-1 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
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
      className="space-y-2 rounded-md border p-3"
      onSubmit={(e) => {
        e.preventDefault()
        onApply()
      }}
    >
      <div className="flex flex-col gap-2 sm:flex-row">
        <div className="relative min-w-0 flex-1">
          <Search className="pointer-events-none absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
          <Input
            value={draft.q}
            onChange={(e) => onChange({ q: e.target.value })}
            placeholder="Buscar por SKU, código de barras o nombre"
            className="pl-8"
            maxLength={100}
          />
        </div>
        <div className="flex gap-2">
          <Button type="submit" disabled={loading}>
            {loading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
            Buscar
          </Button>
          <Button type="button" variant="outline" onClick={onClear} disabled={loading}>
            Limpiar
          </Button>
        </div>
      </div>
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
                {o.name ?? `Oficina ${o.id}`}
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
        <span className="text-[11px] font-medium uppercase tracking-wide text-muted-foreground">Orden</span>
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
        pct > 0 ? "text-red-700" : pct < 0 ? "text-emerald-700" : "text-muted-foreground",
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
    return (
      <p className="rounded-md border border-dashed px-4 py-8 text-center text-sm text-muted-foreground">
        Sin productos para los filtros aplicados.
      </p>
    )
  }
  return (
    <div className={cn("overflow-x-auto rounded-md border", loading && "opacity-60")}>
      <table className="w-full min-w-[1100px] text-sm">
        <thead className="bg-muted/40 text-left text-[11px] uppercase tracking-wide text-muted-foreground">
          <tr>
            <th className="px-3 py-2 font-medium">Producto</th>
            <th className="px-3 py-2 font-medium">Tipo</th>
            <th className="px-3 py-2 text-right font-medium">Promedio Bsale</th>
            <th className="px-3 py-2 text-right font-medium">Última compra</th>
            <th className="px-3 py-2 text-right font-medium">Anterior</th>
            <th className="px-3 py-2 text-right font-medium">Variación</th>
            <th className="px-3 py-2 font-medium">Último movimiento</th>
            <th className="px-3 py-2 font-medium">Confiabilidad</th>
            <th className="px-3 py-2 font-medium">Alertas</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border/60">
          {items.map((item) => (
            <tr
              key={item.variant_id}
              className="cursor-pointer hover:bg-muted/40"
              onClick={() => onOpen(item.variant_id)}
            >
              <td className="max-w-[280px] px-3 py-2">
                <p className="truncate font-medium">{item.product_name ?? "Sin nombre"}</p>
                <p className="truncate text-[11px] text-muted-foreground">
                  {item.code ?? "—"}
                  {item.barcode && item.barcode !== item.code ? ` · ${item.barcode}` : ""}
                  {item.variant_name ? ` · ${item.variant_name}` : ""}
                  {item.variant_state !== "activa" ? " · inactivo" : ""}
                </p>
              </td>
              <td className="px-3 py-2 text-xs">{item.product_type.name ?? "—"}</td>
              <td className="px-3 py-2 text-right tabular-nums">
                {formatMoney(item.bsale_average_cost.value)}
              </td>
              <td className="px-3 py-2 text-right tabular-nums">
                {formatMoney(item.last_comparable?.cost ?? null)}
                <span className="block text-[11px] text-muted-foreground">
                  {formatDate(item.last_comparable?.admission_date)}
                  {item.last_comparable?.office_name ? ` · ${item.last_comparable.office_name}` : ""}
                </span>
              </td>
              <td className="px-3 py-2 text-right tabular-nums">
                {formatMoney(item.previous_comparable?.cost ?? null)}
                <span className="block text-[11px] text-muted-foreground">
                  {formatDate(item.previous_comparable?.admission_date)}
                </span>
              </td>
              <td className="px-3 py-2 text-right">
                <ChangeCell item={item} />
              </td>
              <td className="px-3 py-2">
                {item.last_reception ? (
                  <MovementBadge movement={item.last_reception.movement_class} />
                ) : (
                  <span className="text-xs text-muted-foreground">Sin recepciones</span>
                )}
              </td>
              <td className="px-3 py-2">
                <ReliabilityBadge status={item.reliability.status} title={item.reliability.reason} />
              </td>
              <td className="px-3 py-2">
                <AnomalyChips anomalies={item.anomalies} max={2} />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
