"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import { Loader2, Search } from "lucide-react"

import { SectionCard, StatusBadge } from "@/components/analytics-v2/shared/analytics-ui"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from "@/components/ui/sheet"
import { cn } from "@/lib/utils"
import {
  getOfficeCompare,
  getOfficeCompareProduct,
  getOfficeCompareTimeline,
} from "@/lib/analytics-v2/api"
import {
  OFFICE_INFO_TAG_LABELS,
  OFFICE_INFO_TAG_TONES,
  RELIABILITY_LABELS,
  formatDate,
  formatMoney,
  formatPct,
  formatQty,
} from "@/lib/analytics-v2/labels"
import {
  formatBarcode,
  formatProductPlusVariant,
} from "@/lib/analytics-v2/product-identity"
import type {
  CostFilters,
  CostReliability,
  OfficeCompareLine,
  OfficeCompareListResponse,
  OfficeCompareProductResponse,
  OfficeCompareRow,
  OfficeCompareTimelineResponse,
  OfficeInfoTag,
} from "@/lib/analytics-v2/types"

import { MovementBadge, ReliabilityBadge } from "./badges"
import { CostV2StartReviewButton } from "./cost-v2-start-review-button"
import { Pager } from "./cost-v2-products"

const PAGE_SIZE = 25

const INFO_TAGS = Object.keys(OFFICE_INFO_TAG_LABELS) as OfficeInfoTag[]

function InfoTagBadge({ tag }: { tag: OfficeInfoTag }) {
  return (
    <span
      className={cn(
        "inline-flex items-center whitespace-nowrap rounded-md border px-1.5 py-0.5 text-[11px] font-medium",
        OFFICE_INFO_TAG_TONES[tag],
      )}
    >
      {OFFICE_INFO_TAG_LABELS[tag]}
    </span>
  )
}

function OfficeCellSummary({
  cell,
  compact,
}: {
  cell: OfficeCompareRow["offices"][number]
  compact?: boolean
}) {
  const last = cell.last_reception_unit_cost
  return (
    <div className={cn("space-y-1", compact ? "min-w-[7.5rem]" : "")}>
      <div className="flex flex-wrap items-center gap-1">
        <InfoTagBadge tag={cell.info_tag} />
        {cell.alerts?.length ? (
          <span className="text-[10px] text-lq-warning">{cell.alerts.length} aviso(s)</span>
        ) : null}
      </div>
      <p className="text-sm font-semibold tabular-nums text-lq-ink">
        {formatMoney(last?.cost)}
        <span className="ml-1 text-[11px] font-normal text-lq-muted">unit.</span>
      </p>
      <p className="text-[11px] text-lq-muted">
        Stock {formatQty(cell.stock.quantity)}
        {last ? ` · ${formatDate(last.admission_date)}` : ""}
      </p>
      {last ? (
        <p className="truncate text-[11px] text-lq-body" title={last.document ?? undefined}>
          {last.movement_label}
          {last.document_number != null ? ` N° ${last.document_number}` : ""}
        </p>
      ) : (
        <p className="text-[11px] text-lq-faint">Sin recepción documentada</p>
      )}
      {cell.last_supplier_purchase ? (
        <p className="text-[10px] text-lq-muted">
          Compra (factura): {formatMoney(cell.last_supplier_purchase.cost)}
        </p>
      ) : (
        <p className="text-[10px] text-lq-faint">Sin factura a proveedor</p>
      )}
    </div>
  )
}

function OfficeCompareDrawer({
  companyId,
  variantId,
  open,
  onOpenChange,
  onOpenReception,
}: {
  companyId: number
  variantId: number | null
  open: boolean
  onOpenChange: (open: boolean) => void
  onOpenReception: (receptionId: number, detailId: number) => void
}) {
  const [detail, setDetail] = useState<OfficeCompareProductResponse | null>(null)
  const [timeline, setTimeline] = useState<OfficeCompareTimelineResponse | null>(null)
  const [officeFilter, setOfficeFilter] = useState<string>("")
  const [page, setPage] = useState(1)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!open || variantId == null) return
    const ac = new AbortController()
    setLoading(true)
    setError(null)
    setPage(1)
    getOfficeCompareProduct(companyId, variantId, ac.signal)
      .then(setDetail)
      .catch((e: unknown) => {
        if (!ac.signal.aborted) setError(e instanceof Error ? e.message : "Error al cargar")
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoading(false)
      })
    return () => ac.abort()
  }, [open, companyId, variantId])

  useEffect(() => {
    if (!open || variantId == null) return
    const ac = new AbortController()
    getOfficeCompareTimeline(
      {
        company_id: companyId,
        variant_id: variantId,
        office_id: officeFilter ? Number(officeFilter) : null,
        page,
        page_size: 20,
      },
      ac.signal,
    )
      .then(setTimeline)
      .catch(() => {
        if (!ac.signal.aborted) setTimeline(null)
      })
    return () => ac.abort()
  }, [open, companyId, variantId, officeFilter, page])

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="w-full overflow-y-auto sm:max-w-xl lg:max-w-2xl">
        <SheetHeader>
          <SheetTitle>
            {detail
              ? formatProductPlusVariant(detail.product_name, detail.variant_name)
              : "Comparativo por oficina"}
          </SheetTitle>
          <SheetDescription>
            Último costo unitario documentado por oficina · no es promedio de oficina · promedio
            Bsale aparte
          </SheetDescription>
        </SheetHeader>

        {loading ? (
          <div className="flex justify-center py-16">
            <Loader2 className="h-6 w-6 animate-spin text-lq-muted" />
          </div>
        ) : error ? (
          <p className="mt-6 text-sm text-lq-danger">{error}</p>
        ) : detail ? (
          <div className="mt-6 space-y-6">
            <div className="flex flex-wrap items-center gap-2">
              <ReliabilityBadge status={detail.reliability.status} title={detail.reliability.reason} />
              <span className="text-xs text-lq-muted">{formatBarcode(detail.barcode)}</span>
              <span className="text-xs text-lq-muted">Var. {detail.variant_id}</span>
            </div>

            <SectionCard title="Referencia global Bsale" description="No es costo por oficina">
              <p className="text-2xl font-semibold tabular-nums text-lq-ink">
                {formatMoney(detail.bsale_average_cost?.value)}
              </p>
              <p className="mt-1 text-xs text-lq-muted">{detail.labels?.bsale_average_cost}</p>
            </SectionCard>

            <div className="grid gap-3 sm:grid-cols-2">
              {detail.offices.map((cell) => (
                <div
                  key={cell.office_id}
                  className="rounded-xl border border-lq-line bg-lq-surface p-3 shadow-[0_1px_2px_rgba(15,23,42,0.03)]"
                >
                  <p className="text-sm font-medium text-lq-ink">{cell.office_name}</p>
                  <div className="mt-2">
                    <OfficeCellSummary cell={cell} />
                  </div>
                  {cell.last_reception_unit_cost ? (
                    <button
                      type="button"
                      className="mt-2 text-xs text-primary hover:underline"
                      onClick={() =>
                        onOpenReception(
                          cell.last_reception_unit_cost!.reception_id,
                          cell.last_reception_unit_cost!.detail_id,
                        )
                      }
                    >
                      Ver recepción #{cell.last_reception_unit_cost.reception_id}
                    </button>
                  ) : null}
                </div>
              ))}
            </div>

            {detail.office_spread ? (
              <SectionCard title="Spread entre facturas de oficina">
                <p className="text-sm text-lq-body">
                  {formatMoney(detail.office_spread.min_cost)} → {formatMoney(detail.office_spread.max_cost)}{" "}
                  ({formatPct(detail.office_spread.spread_pct)})
                  {detail.office_spread.alert ? (
                    <StatusBadge tone="warning" className="ml-2">
                      Diferencia relevante
                    </StatusBadge>
                  ) : null}
                </p>
                <p className="mt-1 text-xs text-lq-muted">
                  Solo compara costos unitarios de facturas (misma base RAW). No usa guías ni el
                  promedio Bsale.
                </p>
              </SectionCard>
            ) : null}

            <div className="flex flex-wrap items-center gap-2">
              <CostV2StartReviewButton
                companyId={companyId}
                variant="outline"
                payload={{
                  variant_id: detail.variant_id,
                  anomaly_code:
                    detail.anomalies?.[0]?.code ??
                    (detail.has_relevant_difference
                      ? "diferencia_promedio_bsale"
                      : "movimiento_no_determinado"),
                  reason_label: "Revisión desde comparativo por oficina",
                  priority: detail.has_relevant_difference ? "alta" : "media",
                  source: "office_compare",
                  source_ref: {
                    surface: "office_compare",
                    office_scope_supported: false,
                  },
                }}
              />
              <p className="text-[11px] text-lq-muted">{detail.review_hint.note}</p>
            </div>

            <SectionCard
              title="Timeline de recepciones"
              description="Factura = compra a proveedor · Guía = documento de traspaso"
              action={
                <Select
                  value={officeFilter || "all"}
                  onValueChange={(v) => {
                    setOfficeFilter(v === "all" ? "" : v)
                    setPage(1)
                  }}
                >
                  <SelectTrigger className="h-9 w-[200px]">
                    <SelectValue placeholder="Oficina" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">Todas (operativas)</SelectItem>
                    {(detail.offices || []).map((o) => (
                      <SelectItem key={o.office_id} value={String(o.office_id)}>
                        {o.office_name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              }
            >
              <ul className="divide-y divide-lq-line">
                {(timeline?.items ?? []).map((line: OfficeCompareLine) => (
                  <li key={`${line.reception_id}-${line.detail_id}`} className="flex flex-col gap-1 py-3">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="text-sm font-medium tabular-nums text-lq-ink">
                        {formatMoney(line.cost)}
                      </span>
                      <MovementBadge movement={line.movement_class} />
                      <span className="text-xs text-lq-muted">{formatDate(line.admission_date)}</span>
                    </div>
                    <p className="text-xs text-lq-body">
                      {line.office_name ?? `Oficina ${line.office_id}`} · {line.document ?? "—"}
                      {line.document_number != null ? ` N° ${line.document_number}` : ""} ·{" "}
                      {formatQty(line.quantity)} un.
                    </p>
                    <button
                      type="button"
                      className="w-fit text-xs text-primary hover:underline"
                      onClick={() => onOpenReception(line.reception_id, line.detail_id)}
                    >
                      Recepción #{line.reception_id}
                    </button>
                  </li>
                ))}
                {!timeline?.items?.length ? (
                  <li className="py-6 text-sm text-lq-muted">Sin movimientos en el filtro actual.</li>
                ) : null}
              </ul>
              {timeline ? (
                <Pager
                  page={timeline.page}
                  totalPages={timeline.total_pages}
                  total={timeline.total}
                  loading={false}
                  onPage={setPage}
                />
              ) : null}
            </SectionCard>
          </div>
        ) : null}
      </SheetContent>
    </Sheet>
  )
}

export function CostV2OfficeCompare({
  companyId,
  stockFilter,
  filters,
  onOpenReception,
}: {
  companyId: number
  stockFilter: "with" | "without" | "all"
  filters: CostFilters | null
  onOpenReception: (receptionId: number, detailId: number) => void
}) {
  const [q, setQ] = useState("")
  const [appliedQ, setAppliedQ] = useState("")
  const [reliability, setReliability] = useState<string>("")
  const [infoTag, setInfoTag] = useState<string>("")
  const [officeId, setOfficeId] = useState<string>("")
  const [sort, setSort] = useState("spread")
  const [minSpread, setMinSpread] = useState(false)
  const [page, setPage] = useState(1)
  const [data, setData] = useState<OfficeCompareListResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [variantId, setVariantId] = useState<number | null>(null)
  const abortRef = useRef<AbortController | null>(null)

  const load = useCallback(() => {
    abortRef.current?.abort()
    const ac = new AbortController()
    abortRef.current = ac
    setLoading(true)
    setError(null)
    getOfficeCompare(
      {
        company_id: companyId,
        q: appliedQ || null,
        reliability: (reliability || null) as CostReliability | null,
        info_tag: (infoTag || null) as OfficeInfoTag | null,
        office_id: officeId ? Number(officeId) : null,
        min_office_spread_pct: minSpread ? 5 : null,
        stock_filter: stockFilter,
        sort,
        page,
        page_size: PAGE_SIZE,
      },
      ac.signal,
    )
      .then(setData)
      .catch((e: unknown) => {
        if (!ac.signal.aborted) setError(e instanceof Error ? e.message : "Error al cargar")
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoading(false)
      })
  }, [companyId, appliedQ, reliability, infoTag, officeId, minSpread, stockFilter, sort, page])

  useEffect(() => {
    load()
    return () => abortRef.current?.abort()
  }, [load])

  const offices = data?.operational_offices ?? filters?.offices ?? []

  return (
    <div className="space-y-4">
      <SectionCard
        title="Comparativo por oficina"
        description="Matriz de último costo unitario documentado en oficinas operativas. El promedio Bsale es solo referencia global."
      >
        <div className="flex flex-col gap-3 lg:flex-row lg:flex-wrap lg:items-end">
          <div className="relative min-w-[220px] flex-1">
            <Search className="pointer-events-none absolute left-2.5 top-2.5 h-4 w-4 text-lq-muted" />
            <Input
              className="h-10 pl-9"
              placeholder="Nombre, SEC, barras o variante"
              value={q}
              onChange={(e) => setQ(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") {
                  setAppliedQ(q.trim())
                  setPage(1)
                }
              }}
            />
          </div>
          <Select
            value={reliability || "all"}
            onValueChange={(v) => {
              setReliability(v === "all" ? "" : v)
              setPage(1)
            }}
          >
            <SelectTrigger className="h-10 w-[180px]">
              <SelectValue placeholder="Confiabilidad" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">Toda confiabilidad</SelectItem>
              {Object.entries(RELIABILITY_LABELS).map(([k, label]) => (
                <SelectItem key={k} value={k}>
                  {label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select
            value={infoTag || "all"}
            onValueChange={(v) => {
              setInfoTag(v === "all" ? "" : v)
              setPage(1)
            }}
          >
            <SelectTrigger className="h-10 w-[170px]">
              <SelectValue placeholder="Etiqueta" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">Todas las etiquetas</SelectItem>
              {INFO_TAGS.map((t) => (
                <SelectItem key={t} value={t}>
                  {OFFICE_INFO_TAG_LABELS[t]}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select
            value={officeId || "all"}
            onValueChange={(v) => {
              setOfficeId(v === "all" ? "" : v)
              setPage(1)
            }}
          >
            <SelectTrigger className="h-10 w-[200px]">
              <SelectValue placeholder="Oficina" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">Todas operativas</SelectItem>
              {offices.map((o) => (
                <SelectItem key={o.id} value={String(o.id)}>
                  {o.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Select
            value={sort}
            onValueChange={(v) => {
              setSort(v)
              setPage(1)
            }}
          >
            <SelectTrigger className="h-10 w-[180px]">
              <SelectValue placeholder="Orden" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="spread">Mayor diferencia</SelectItem>
              <SelectItem value="reliability">Confiabilidad</SelectItem>
              <SelectItem value="product">Producto</SelectItem>
              <SelectItem value="stale">Más antiguo</SelectItem>
              <SelectItem value="stock">Mayor stock</SelectItem>
            </SelectContent>
          </Select>
          <Button
            type="button"
            variant={minSpread ? "default" : "outline"}
            className="h-10"
            onClick={() => {
              setMinSpread((v) => !v)
              setPage(1)
            }}
          >
            Solo con spread ≥ 5%
          </Button>
          <Button
            type="button"
            className="h-10"
            onClick={() => {
              setAppliedQ(q.trim())
              setPage(1)
            }}
          >
            Buscar
          </Button>
        </div>
        {data?.thresholds && !data.thresholds.net_cost_verified ? (
          <p className="mt-3 text-xs text-lq-muted">
            No se calculan deltas entre promedio Bsale y guías: el carácter neto/bruto de los costos
            de recepción aún no está verificado.
          </p>
        ) : null}
      </SectionCard>

      {error ? <p className="text-sm text-lq-danger">{error}</p> : null}

      <div className="overflow-x-auto rounded-xl border border-lq-line bg-lq-surface shadow-sm">
        <table className="min-w-full text-left text-sm">
          <thead className="border-b border-lq-line bg-lq-canvas/60 text-[11px] uppercase tracking-wide text-lq-muted">
            <tr>
              <th className="sticky left-0 z-10 bg-lq-canvas/95 px-3 py-2.5 font-medium">Producto</th>
              <th className="px-3 py-2.5 font-medium">Prom. Bsale</th>
              {(data?.operational_offices ?? offices).map((o) => (
                <th key={o.id} className="px-3 py-2.5 font-medium">
                  {o.name}
                </th>
              ))}
              <th className="px-3 py-2.5 font-medium">Spread facturas</th>
            </tr>
          </thead>
          <tbody>
            {loading && !data ? (
              <tr>
                <td colSpan={8} className="px-3 py-10 text-center text-lq-muted">
                  <Loader2 className="mx-auto h-5 w-5 animate-spin" />
                </td>
              </tr>
            ) : null}
            {(data?.items ?? []).map((row) => (
              <tr
                key={row.variant_id}
                className="cursor-pointer border-b border-lq-line/80 hover:bg-lq-canvas/40"
                onClick={() => setVariantId(row.variant_id)}
              >
                <td className="sticky left-0 z-10 bg-lq-surface px-3 py-3">
                  <p className="font-medium text-lq-ink">
                    {formatProductPlusVariant(row.product_name, row.variant_name)}
                  </p>
                  <div className="mt-1 flex flex-wrap items-center gap-1.5">
                    <ReliabilityBadge status={row.reliability.status} />
                    <span className="text-[11px] text-lq-muted">{formatBarcode(row.barcode)}</span>
                  </div>
                </td>
                <td className="px-3 py-3 tabular-nums text-lq-body">
                  {formatMoney(row.bsale_average_cost?.value)}
                </td>
                {row.offices.map((cell) => (
                  <td key={cell.office_id} className="px-3 py-3 align-top">
                    <OfficeCellSummary cell={cell} compact />
                  </td>
                ))}
                <td className="px-3 py-3 align-top">
                  {row.office_spread ? (
                    <div>
                      <p className="font-medium tabular-nums text-lq-ink">
                        {formatPct(row.office_spread.spread_pct)}
                      </p>
                      {row.has_relevant_difference ? (
                        <StatusBadge tone="warning">Relevante</StatusBadge>
                      ) : (
                        <span className="text-[11px] text-lq-muted">Dentro de umbral</span>
                      )}
                    </div>
                  ) : (
                    <span className="text-xs text-lq-faint">Insuficiente</span>
                  )}
                </td>
              </tr>
            ))}
            {!loading && data && data.items.length === 0 ? (
              <tr>
                <td colSpan={8} className="px-3 py-10 text-center text-lq-muted">
                  Sin productos para los filtros actuales.
                </td>
              </tr>
            ) : null}
          </tbody>
        </table>
      </div>

      {data ? (
        <Pager
          page={data.page}
          totalPages={data.total_pages}
          total={data.total}
          loading={loading}
          onPage={setPage}
        />
      ) : null}

      <OfficeCompareDrawer
        companyId={companyId}
        variantId={variantId}
        open={variantId != null}
        onOpenChange={(open) => {
          if (!open) setVariantId(null)
        }}
        onOpenReception={onOpenReception}
      />
    </div>
  )
}
