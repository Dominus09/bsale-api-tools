"use client"

import { useEffect, useState } from "react"
import { Loader2 } from "lucide-react"

import { ProfessionalDataTable, SectionCard, SmartFilterBar, StatusBadge } from "@/components/analytics-v2/shared/analytics-ui"
import { ProductIdentityCell } from "@/components/analytics-v2/shared/product-identity-cell"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { getReceptionAnalysis } from "@/lib/analytics-v2/api"
import { formatOfficeLabel } from "@/lib/analytics-v2/display-labels"
import {
  formatDate,
  formatMoney,
  formatPct,
} from "@/lib/analytics-v2/labels"
import type { CostFilters, ReceptionAnalysisItem, ReceptionAnalysisResponse } from "@/lib/analytics-v2/types"

import { Pager } from "./cost-v2-products"

function isoDaysAgo(days: number): string {
  const d = new Date()
  d.setDate(d.getDate() - days)
  return d.toISOString().slice(0, 10)
}

const selectClass = "h-9 rounded-md border border-input bg-background px-2 text-sm"

function statusTone(status: string): "neutral" | "success" | "warning" | "danger" | "info" {
  if (status === "alza") return "danger"
  if (status === "baja") return "success"
  if (status === "sin_referencia_anterior") return "warning"
  return "neutral"
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
  const [draft, setDraft] = useState({
    dateFrom: isoDaysAgo(90),
    dateTo: isoDaysAgo(0),
    productTypeId: "",
    officeId: "",
    direction: "all" as "all" | "up" | "down",
    minChange: "",
    q: "",
  })
  const [applied, setApplied] = useState(draft)
  const [page, setPage] = useState(1)
  const [data, setData] = useState<ReceptionAnalysisResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [expanded, setExpanded] = useState<ReceptionAnalysisItem | null>(null)

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
        description="Variación de costo entre la compra comparable actual y la anterior. No se compara con el promedio Bsale (dato adicional en el detalle)."
      />

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
              Desde
              <Input
                type="date"
                min="2025-01-01"
                value={draft.dateFrom}
                onChange={(e) => setDraft((d) => ({ ...d, dateFrom: e.target.value }))}
              />
            </label>
            <label className="flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground">
              Hasta
              <Input
                type="date"
                value={draft.dateTo}
                onChange={(e) => setDraft((d) => ({ ...d, dateTo: e.target.value }))}
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
                    {formatOfficeLabel(o.name, o.id)}
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
                  onClick={() => setExpanded(row)}
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

      {expanded ? (
        <SectionCard title="Detalle de la compra" description="Comparación compra vs compra; el promedio Bsale es solo referencia.">
          <dl className="mt-2 grid gap-2 text-sm sm:grid-cols-2">
            <div>
              <dt className="text-xs uppercase text-muted-foreground">Recepción</dt>
              <dd>
                <button
                  type="button"
                  className="text-primary underline-offset-2 hover:underline"
                  onClick={() => onOpenReception(expanded.reception_id, expanded.detail_id)}
                >
                  #{expanded.reception_id} · detalle {expanded.detail_id}
                </button>
              </dd>
            </div>
            <div>
              <dt className="text-xs uppercase text-muted-foreground">Documento</dt>
              <dd>
                {[expanded.document, expanded.document_number].filter(Boolean).join(" ") || "—"}
              </dd>
            </div>
            <div>
              <dt className="text-xs uppercase text-muted-foreground">Sucursal</dt>
              <dd>
                {expanded.office_name?.trim() ||
                  (expanded.office_id != null
                    ? `Sucursal sin nombre — requiere corrección (ref. ${expanded.office_id})`
                    : "—")}
              </dd>
            </div>
            <div>
              <dt className="text-xs uppercase text-muted-foreground">Cantidad</dt>
              <dd className="tabular-nums">{expanded.quantity ?? "—"}</dd>
            </div>
            <div>
              <dt className="text-xs uppercase text-muted-foreground">Costo neto recepción</dt>
              <dd className="tabular-nums">{formatMoney(expanded.cost)}</dd>
            </div>
            <div>
              <dt className="text-xs uppercase text-muted-foreground">Costo comparable anterior</dt>
              <dd className="tabular-nums">
                {expanded.previous_cost != null ? formatMoney(expanded.previous_cost) : "Sin referencia anterior"}
              </dd>
            </div>
            <div>
              <dt className="text-xs uppercase text-muted-foreground">Diferencia monetaria</dt>
              <dd className="tabular-nums">
                {expanded.change_amount != null ? formatMoney(expanded.change_amount) : "—"}
              </dd>
            </div>
            <div>
              <dt className="text-xs uppercase text-muted-foreground">Promedio Bsale (adicional)</dt>
              <dd className="tabular-nums">{formatMoney(expanded.bsale_average_cost)}</dd>
            </div>
            {expanded.unit_note ? (
              <div className="sm:col-span-2">
                <dt className="text-xs uppercase text-muted-foreground">Presentación / unidad</dt>
                <dd>{expanded.unit_note}</dd>
              </div>
            ) : null}
            {expanded.previous_reception_id != null ? (
              <div className="sm:col-span-2">
                <dt className="text-xs uppercase text-muted-foreground">Compra anterior</dt>
                <dd>
                  Recepción #{expanded.previous_reception_id}
                  {expanded.previous_admission_date ? ` · ${formatDate(expanded.previous_admission_date)}` : ""}
                </dd>
              </div>
            ) : null}
          </dl>
          <Button type="button" variant="outline" size="sm" className="mt-3" onClick={() => setExpanded(null)}>
            Cerrar detalle
          </Button>
        </SectionCard>
      ) : null}
    </div>
  )
}
