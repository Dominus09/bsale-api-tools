"use client"

import { useEffect, useState } from "react"
import { Loader2 } from "lucide-react"

import { ProfessionalDataTable, SectionCard, SmartFilterBar, StatusBadge } from "@/components/analytics-v2/shared/analytics-ui"
import { ProductIdentityCell } from "@/components/analytics-v2/shared/product-identity-cell"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from "@/components/ui/sheet"
import { getReceptionAnalysis } from "@/lib/analytics-v2/api"
import { formatOfficeLabel } from "@/lib/analytics-v2/display-labels"
import {
  formatDate,
  formatMoney,
  formatPct,
} from "@/lib/analytics-v2/labels"
import {
  formatBarcode,
  formatProductPlusVariant,
  formatProductType,
} from "@/lib/analytics-v2/product-identity"
import {
  RECEPTION_PERIOD_OPTIONS,
  rangeForReceptionPreset,
  type ReceptionPeriodPreset,
} from "@/lib/analytics-v2/chile-dates"
import type { CostFilters, ReceptionAnalysisItem, ReceptionAnalysisResponse } from "@/lib/analytics-v2/types"

import { Pager } from "./cost-v2-products"
import { CostV2StartReviewButton } from "./cost-v2-start-review-button"

const selectClass = "h-9 rounded-md border border-input bg-background px-2 text-sm"

function initialReceptionDates() {
  return rangeForReceptionPreset("current_month")
}

function statusTone(status: string): "neutral" | "success" | "warning" | "danger" | "info" {
  if (status === "alza") return "danger"
  if (status === "baja") return "success"
  if (status === "sin_referencia_anterior") return "warning"
  return "neutral"
}

function moneyDiff(current: string | number | null | undefined, previous: string | number | null | undefined): string {
  if (current == null || previous == null) return "—"
  const a = Number(current)
  const b = Number(previous)
  if (!Number.isFinite(a) || !Number.isFinite(b)) return "—"
  return formatMoney(a - b)
}

function ReceptionDetailSheet({
  companyId,
  item,
  open,
  onOpenChange,
  onOpenProduct,
  onOpenReception,
}: {
  companyId: number
  item: ReceptionAnalysisItem | null
  open: boolean
  onOpenChange: (open: boolean) => void
  onOpenProduct: (variantId: number) => void
  onOpenReception: (receptionId: number, detailId: number) => void
}) {
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent
        side="right"
        className="w-full overflow-y-auto border-l-lq-line bg-lq-canvas p-0 sm:max-w-[min(560px,92vw)]"
      >
        <SheetHeader className="sticky top-0 z-20 border-b border-lq-line bg-lq-surface/95 px-5 py-4 backdrop-blur md:px-6">
          <SheetTitle className="pr-8 text-xl tracking-[-0.015em]">
            {item
              ? formatProductPlusVariant(item.product_name, item.variant_name)
              : "Detalle de recepción"}
          </SheetTitle>
          <SheetDescription>
            {item
              ? `${formatProductType(item.product_type?.name)} · ${formatBarcode(item.barcode)}`
              : "Comparación compra actual vs compra anterior"}
          </SheetDescription>
        </SheetHeader>

        {item ? (
          <div className="space-y-5 px-4 pb-8 pt-4 md:px-6">
            <section className="space-y-2">
              <h3 className="text-[11px] font-semibold uppercase tracking-wide text-lq-muted">
                Compra actual vs compra anterior
              </h3>
              <div className="grid gap-3 sm:grid-cols-2">
                <div className="rounded-xl border border-lq-line bg-lq-surface p-4">
                  <p className="text-[13px] font-medium text-lq-body">Costo neto compra actual</p>
                  <p className="mt-2 text-2xl font-semibold tabular-nums text-lq-ink">
                    {formatMoney(item.cost)}
                  </p>
                </div>
                <div className="rounded-xl border border-lq-line bg-lq-surface p-4">
                  <p className="text-[13px] font-medium text-lq-body">Costo neto compra anterior</p>
                  <p className="mt-2 text-2xl font-semibold tabular-nums text-lq-ink">
                    {item.previous_cost != null ? formatMoney(item.previous_cost) : "Sin referencia"}
                  </p>
                </div>
                <div className="rounded-xl border border-lq-line bg-lq-surface p-4">
                  <p className="text-[13px] font-medium text-lq-body">Diferencia monetaria</p>
                  <p className="mt-2 text-xl font-semibold tabular-nums text-lq-ink">
                    {item.change_amount != null
                      ? formatMoney(item.change_amount)
                      : moneyDiff(item.cost, item.previous_cost)}
                  </p>
                </div>
                <div className="rounded-xl border border-lq-line bg-lq-surface p-4">
                  <p className="text-[13px] font-medium text-lq-body">Diferencia porcentual</p>
                  <p className="mt-2 text-xl font-semibold tabular-nums text-lq-ink">
                    {item.change_pct != null ? formatPct(item.change_pct) : "—"}
                  </p>
                  <div className="mt-2">
                    <StatusBadge tone={statusTone(item.status)}>{item.status_label}</StatusBadge>
                  </div>
                </div>
              </div>
              <p className="text-xs text-lq-muted">
                La comparación usa costo neto unitario entre compras comparables (factura/guía). Distinta
                cantidad no invalida la comparación si la presentación es la misma.
              </p>
            </section>

            <section className="space-y-2">
              <h3 className="text-[11px] font-semibold uppercase tracking-wide text-lq-muted">Respaldo</h3>
              <dl className="grid gap-2 rounded-xl border border-lq-line bg-lq-surface p-4 text-sm sm:grid-cols-2">
                <div>
                  <dt className="text-xs uppercase text-lq-muted">Fecha</dt>
                  <dd>{formatDate(item.admission_date)}</dd>
                </div>
                <div>
                  <dt className="text-xs uppercase text-lq-muted">Documento / factura</dt>
                  <dd>{[item.document, item.document_number].filter(Boolean).join(" ") || "—"}</dd>
                </div>
                <div>
                  <dt className="text-xs uppercase text-lq-muted">Oficina</dt>
                  <dd>
                    {item.office_name?.trim() ||
                      (item.office_id != null
                        ? `Sucursal sin nombre — requiere corrección (ref. ${item.office_id})`
                        : "—")}
                  </dd>
                </div>
                <div>
                  <dt className="text-xs uppercase text-lq-muted">Cantidad</dt>
                  <dd className="tabular-nums">{item.quantity ?? "—"}</dd>
                </div>
                <div className="sm:col-span-2">
                  <dt className="text-xs uppercase text-lq-muted">Identificador de recepción</dt>
                  <dd>
                    <button
                      type="button"
                      className="text-primary underline-offset-2 hover:underline"
                      onClick={() => onOpenReception(item.reception_id, item.detail_id)}
                    >
                      Recepción #{item.reception_id} · detalle {item.detail_id}
                    </button>
                  </dd>
                </div>
                {item.previous_reception_id != null ? (
                  <div className="sm:col-span-2">
                    <dt className="text-xs uppercase text-lq-muted">Compra anterior</dt>
                    <dd>
                      Recepción #{item.previous_reception_id}
                      {item.previous_admission_date ? ` · ${formatDate(item.previous_admission_date)}` : ""}
                    </dd>
                  </div>
                ) : null}
              </dl>
            </section>

            <section className="space-y-2">
              <h3 className="text-[11px] font-semibold uppercase tracking-wide text-lq-muted">
                Compra actual vs promedio Bsale
              </h3>
              <div className="rounded-xl border border-lq-line bg-lq-surface p-4">
                <p className="text-[13px] font-medium text-lq-body">Costo promedio Bsale (referencia)</p>
                <p className="mt-2 text-xl font-semibold tabular-nums text-lq-ink">
                  {formatMoney(item.bsale_average_cost)}
                </p>
                <p className="mt-1 text-xs text-lq-muted">
                  Dato adicional del maestro; no sustituye la comparación compra vs compra.
                </p>
              </div>
              {item.unit_note ? (
                <div className="rounded-xl border border-amber-200/80 bg-amber-50/50 p-4 text-sm">
                  <p className="text-[11px] font-semibold uppercase tracking-wide text-amber-800">
                    Advertencia de unidad / presentación
                  </p>
                  <p className="mt-1 text-lq-body">{item.unit_note}</p>
                </div>
              ) : null}
            </section>

            {item.variant_id != null ? (
              <div className="flex flex-wrap items-start gap-3">
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={() => onOpenProduct(item.variant_id!)}
                >
                  Abrir ficha del producto
                </Button>
                <CostV2StartReviewButton
                  companyId={companyId}
                  payload={{
                    variant_id: item.variant_id,
                    anomaly_code:
                      item.status === "alza"
                        ? "alza_anormal"
                        : item.status === "baja"
                          ? "baja_sospechosa"
                          : "movimiento_no_determinado",
                    reason_label: item.status_label || "Análisis de recepción",
                    priority: item.status === "alza" ? "alta" : "media",
                    source: "reception_analysis",
                    source_ref: {
                      reception_id: item.reception_id,
                      detail_id: item.detail_id,
                      status: item.status,
                    },
                  }}
                />
              </div>
            ) : null}
          </div>
        ) : null}
      </SheetContent>
    </Sheet>
  )
}

export function CostV2ReceptionAnalysis({
  companyId,
  filters,
  stockFilter = "with",
  onOpenProduct,
  onOpenReception,
}: {
  companyId: number
  filters: CostFilters | null
  stockFilter?: "with" | "without" | "all"
  onOpenProduct: (variantId: number) => void
  onOpenReception: (receptionId: number, detailId: number) => void
}) {
  const [periodPreset, setPeriodPreset] = useState<ReceptionPeriodPreset>("current_month")
  const [draft, setDraft] = useState(() => {
    const range = initialReceptionDates()
    return {
      dateFrom: range.from,
      dateTo: range.to,
      productTypeId: "",
      officeId: "",
      direction: "all" as "all" | "up" | "down",
      minChange: "",
      q: "",
    }
  })
  const [applied, setApplied] = useState(draft)
  const [page, setPage] = useState(1)
  const [data, setData] = useState<ReceptionAnalysisResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [selected, setSelected] = useState<ReceptionAnalysisItem | null>(null)

  const applyPeriodPreset = (preset: ReceptionPeriodPreset) => {
    setPeriodPreset(preset)
    if (preset === "custom") return
    const range = rangeForReceptionPreset(preset)
    const next = { ...draft, dateFrom: range.from, dateTo: range.to }
    setDraft(next)
    setPage(1)
    setApplied(next)
  }

  useEffect(() => {
    const ac = new AbortController()
    setLoading(true)
    setError(null)
    const minChange = applied.minChange ? Number(applied.minChange) : null
    getReceptionAnalysis(
      {
        company_id: companyId,
        date_from: applied.dateFrom,
        date_to: applied.dateTo,
        product_type_id: applied.productTypeId ? Number(applied.productTypeId) : null,
        office_id: applied.officeId ? Number(applied.officeId) : null,
        direction: applied.direction,
        min_abs_change_pct: minChange !== null && Number.isFinite(minChange) ? minChange : null,
        q: applied.q.trim() || null,
        stock_filter: stockFilter,
        page,
        page_size: 50,
      },
      ac.signal,
    )
      .then(setData)
      .catch((e: unknown) => {
        if (!ac.signal.aborted) setError(e instanceof Error ? e.message : "Error al cargar el análisis")
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoading(false)
      })
    return () => ac.abort()
  }, [companyId, applied, page, stockFilter])

  return (
    <div className="space-y-3">
      <SectionCard
        title="Análisis de recepciones"
        description="Variación compra actual vs compra anterior (no vs promedio Bsale). El período filtra solo la recepción actual; la comparable anterior puede ser de un mes previo."
      >
        {null}
      </SectionCard>

      <form
        onSubmit={(e) => {
          e.preventDefault()
          setPage(1)
          setApplied({ ...draft })
        }}
      >
        <SmartFilterBar>
          <div className="flex flex-wrap items-end gap-3">
            <label className="flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground">
              Período
              <select
                className={selectClass}
                value={periodPreset}
                onChange={(e) => applyPeriodPreset(e.target.value as ReceptionPeriodPreset)}
              >
                {RECEPTION_PERIOD_OPTIONS.map((opt) => (
                  <option key={opt.value} value={opt.value}>
                    {opt.label}
                  </option>
                ))}
              </select>
            </label>
            <label className="flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground">
              Desde
              <Input
                type="date"
                min="2025-01-01"
                value={draft.dateFrom}
                onChange={(e) => {
                  setPeriodPreset("custom")
                  setDraft((d) => ({ ...d, dateFrom: e.target.value }))
                }}
              />
            </label>
            <label className="flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground">
              Hasta
              <Input
                type="date"
                value={draft.dateTo}
                onChange={(e) => {
                  setPeriodPreset("custom")
                  setDraft((d) => ({ ...d, dateTo: e.target.value }))
                }}
              />
            </label>
            <label className="flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground">
              Tipo
              <select
                className={selectClass}
                value={draft.productTypeId}
                onChange={(e) => setDraft((d) => ({ ...d, productTypeId: e.target.value }))}
              >
                <option value="">Todos</option>
                {(filters?.product_types ?? []).map((t) => (
                  <option key={t.id} value={t.id}>
                    {t.name ?? `Tipo ${t.id}`}
                  </option>
                ))}
              </select>
            </label>
            <label className="flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground">
              Sucursal
              <select
                className={selectClass}
                value={draft.officeId}
                onChange={(e) => setDraft((d) => ({ ...d, officeId: e.target.value }))}
              >
                <option value="">Todas</option>
                {(filters?.offices ?? []).map((o) => (
                  <option key={o.id} value={o.id}>
                    {formatOfficeLabel(o.name, o.id, companyId)}
                  </option>
                ))}
              </select>
            </label>
            <label className="flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground">
              Dirección
              <select
                className={selectClass}
                value={draft.direction}
                onChange={(e) => setDraft((d) => ({ ...d, direction: e.target.value as "all" | "up" | "down" }))}
              >
                <option value="all">Todas</option>
                <option value="up">Solo alzas</option>
                <option value="down">Solo bajas</option>
              </select>
            </label>
            <label className="flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground">
              Umbral |%|
              <Input
                type="number"
                min={0}
                step={0.1}
                placeholder="0"
                value={draft.minChange}
                onChange={(e) => setDraft((d) => ({ ...d, minChange: e.target.value }))}
                className="w-24"
              />
            </label>
            <label className="flex min-w-[12rem] flex-1 flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground">
              Buscar
              <Input
                value={draft.q}
                onChange={(e) => setDraft((d) => ({ ...d, q: e.target.value }))}
                placeholder="Producto, variante o barcode"
              />
            </label>
            <Button type="submit" disabled={loading || !draft.dateFrom || !draft.dateTo || draft.dateFrom > draft.dateTo}>
              {loading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
              Aplicar
            </Button>
          </div>
        </SmartFilterBar>
      </form>

      {error ? <p className="text-sm text-destructive">{error}</p> : null}

      <ProfessionalDataTable loading={loading}>
        <div className="overflow-x-auto">
          <table className="w-full min-w-[1100px] text-sm">
            <thead>
              <tr className="border-b text-left text-xs uppercase text-muted-foreground">
                <th className="px-3 py-2">Tipo</th>
                <th className="px-3 py-2">Producto</th>
                <th className="px-3 py-2">Barcode</th>
                <th className="px-3 py-2">Fecha</th>
                <th className="px-3 py-2 text-right">Costo actual</th>
                <th className="px-3 py-2 text-right">Costo anterior</th>
                <th className="px-3 py-2 text-right">Variación %</th>
                <th className="px-3 py-2">Estado</th>
              </tr>
            </thead>
            <tbody>
              {(data?.items ?? []).map((row) => (
                <tr
                  key={`${row.reception_id}-${row.detail_id}`}
                  className="cursor-pointer border-b border-border/50 hover:bg-muted/40"
                  onClick={() => setSelected(row)}
                >
                  <td className="px-3 py-2 text-muted-foreground">{row.product_type?.name ?? "—"}</td>
                  <td className="px-3 py-2">
                    {row.variant_id != null ? (
                      <button
                        type="button"
                        className="group text-left"
                        onClick={(e) => {
                          e.stopPropagation()
                          onOpenProduct(row.variant_id!)
                        }}
                      >
                        <ProductIdentityCell
                          compact
                          productTypeName={row.product_type?.name}
                          productName={row.product_name}
                          variantName={row.variant_name}
                          barcode={row.barcode}
                        />
                      </button>
                    ) : (
                      <ProductIdentityCell
                        compact
                        productTypeName={row.product_type?.name}
                        productName={row.product_name}
                        variantName={row.variant_name}
                        barcode={row.barcode}
                      />
                    )}
                  </td>
                  <td className="px-3 py-2 font-mono text-xs">{row.barcode || "—"}</td>
                  <td className="px-3 py-2 whitespace-nowrap">{formatDate(row.admission_date)}</td>
                  <td className="px-3 py-2 text-right tabular-nums">{formatMoney(row.cost)}</td>
                  <td className="px-3 py-2 text-right tabular-nums">
                    {row.previous_cost != null ? formatMoney(row.previous_cost) : "Sin referencia"}
                  </td>
                  <td className="px-3 py-2 text-right tabular-nums">
                    {row.change_pct != null ? formatPct(row.change_pct) : "—"}
                  </td>
                  <td className="px-3 py-2">
                    <StatusBadge tone={statusTone(row.status)}>{row.status_label}</StatusBadge>
                  </td>
                </tr>
              ))}
              {!loading && (data?.items.length ?? 0) === 0 ? (
                <tr>
                  <td colSpan={8} className="px-3 py-8 text-center text-muted-foreground">
                    Sin recepciones comparables en el período.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </ProfessionalDataTable>

      {data ? (
        <Pager
          page={data.page}
          totalPages={data.total_pages}
          total={data.total}
          loading={loading}
          onPage={setPage}
        />
      ) : null}

      <ReceptionDetailSheet
        companyId={companyId}
        item={selected}
        open={selected != null}
        onOpenChange={(open) => {
          if (!open) setSelected(null)
        }}
        onOpenProduct={onOpenProduct}
        onOpenReception={onOpenReception}
      />
    </div>
  )
}
