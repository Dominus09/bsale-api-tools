"use client"

import { useEffect, useMemo, useState } from "react"
import { Loader2 } from "lucide-react"
import { CartesianGrid, Line, LineChart, ReferenceLine, XAxis, YAxis } from "recharts"

import { ChartContainer, ChartTooltip, ChartTooltipContent, type ChartConfig } from "@/components/ui/chart"
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from "@/components/ui/sheet"
import { cn } from "@/lib/utils"
import { getCostProduct, getCostProductHistory } from "@/lib/analytics-v2/api"
import {
  GROSS_STATUS_LABELS,
  TAX_ISSUE_LABELS,
  TAX_LABELS,
  formatDate,
  formatMoney,
  formatPct,
  formatQty,
  toNumber,
} from "@/lib/analytics-v2/labels"
import type { CostProductResponse, HistoryResponse, ReceptionLineRef } from "@/lib/analytics-v2/types"

import { AnomalyChips, AnomalyList, MovementBadge, ReliabilityBadge } from "./badges"
import { Pager } from "./cost-v2-products"

const chartConfig = {
  cost: { label: "Costo compra", color: "hsl(215 70% 40%)" },
} satisfies ChartConfig

function CostCard({
  title,
  value,
  hint,
  line,
  onOpenReception,
}: {
  title: string
  value: string
  hint?: string
  line?: ReceptionLineRef | null
  onOpenReception?: (receptionId: number, detailId: number) => void
}) {
  return (
    <div className="rounded-md border p-3">
      <p className="text-[11px] font-medium uppercase tracking-wide text-muted-foreground">{title}</p>
      <p className="mt-1 text-xl font-semibold tabular-nums">{value}</p>
      {hint ? <p className="text-[11px] text-muted-foreground">{hint}</p> : null}
      {line ? (
        <div className="mt-1 space-y-0.5 text-[11px] text-muted-foreground">
          <p>
            {formatDate(line.admission_date)} · {line.office_name ?? `Oficina ${line.office_id ?? "—"}`} ·{" "}
            {formatQty(line.quantity)} un.
          </p>
          <p className="truncate">
            {line.document ?? "Sin etiqueta"} {line.document_number ? `N° ${line.document_number}` : ""}
          </p>
          {onOpenReception ? (
            <button
              type="button"
              className="text-primary hover:underline"
              onClick={() => onOpenReception(line.reception_id, line.detail_id)}
            >
              Ver recepción #{line.reception_id}
            </button>
          ) : null}
        </div>
      ) : null}
    </div>
  )
}

export function CostV2ProductSheet({
  companyId,
  variantId,
  offices,
  onOpenChange,
  onOpenReception,
}: {
  companyId: number
  variantId: number | null
  offices: { id: number; name: string | null }[]
  onOpenChange: (open: boolean) => void
  onOpenReception: (receptionId: number, detailId: number) => void
}) {
  const [product, setProduct] = useState<CostProductResponse | null>(null)
  const [history, setHistory] = useState<HistoryResponse | null>(null)
  const [page, setPage] = useState(1)
  const [officeId, setOfficeId] = useState<string>("")
  const [loading, setLoading] = useState(false)
  const [historyLoading, setHistoryLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setPage(1)
    setOfficeId("")
  }, [variantId])

  useEffect(() => {
    if (variantId == null) return
    const ac = new AbortController()
    setLoading(true)
    setError(null)
    setProduct(null)
    getCostProduct(companyId, variantId, ac.signal)
      .then(setProduct)
      .catch((e: unknown) => {
        if (!ac.signal.aborted) setError(e instanceof Error ? e.message : "Error al cargar el producto")
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoading(false)
      })
    return () => ac.abort()
  }, [companyId, variantId])

  useEffect(() => {
    if (variantId == null) return
    const ac = new AbortController()
    setHistoryLoading(true)
    getCostProductHistory(
      {
        company_id: companyId,
        variant_id: variantId,
        office_id: officeId ? Number(officeId) : null,
        page,
        page_size: 25,
      },
      ac.signal,
    )
      .then(setHistory)
      .catch((e: unknown) => {
        if (!ac.signal.aborted) setError(e instanceof Error ? e.message : "Error al cargar el historial")
      })
      .finally(() => {
        if (!ac.signal.aborted) setHistoryLoading(false)
      })
    return () => ac.abort()
  }, [companyId, variantId, page, officeId])

  const series = useMemo(
    () =>
      (history?.series ?? [])
        .map((p) => ({ date: formatDate(p.date), cost: toNumber(p.cost) }))
        .filter((p) => p.cost !== null),
    [history],
  )
  const average = toNumber(product?.bsale_average_cost.value ?? null)

  return (
    <Sheet open={variantId != null} onOpenChange={onOpenChange}>
      <SheetContent side="right" className="w-full overflow-y-auto sm:max-w-3xl">
        <SheetHeader>
          <SheetTitle>{product?.product_name ?? (loading ? "Cargando…" : "Producto")}</SheetTitle>
          <SheetDescription>
            {product
              ? `${product.code ?? "—"}${product.barcode && product.barcode !== product.code ? ` · ${product.barcode}` : ""} · ${product.product_type.name ?? "Sin tipo"} · SKU ${product.variant_state}`
              : "Control de costos V2"}
          </SheetDescription>
        </SheetHeader>

        <div className="space-y-4 px-4 pb-6">
          {error ? <p className="text-sm text-destructive">{error}</p> : null}
          {loading && !product ? (
            <div className="flex justify-center py-8">
              <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
            </div>
          ) : null}

          {product ? (
            <>
              <div className="flex flex-wrap items-center gap-2">
                <ReliabilityBadge status={product.reliability.status} />
                <span className="text-sm text-muted-foreground">{product.reliability.reason}</span>
              </div>

              <section className="grid gap-2 sm:grid-cols-3">
                <CostCard
                  title="Costo promedio Bsale"
                  value={formatMoney(product.bsale_average_cost.value)}
                  hint="Neto ponderado informado por Bsale (sin recalcular)"
                />
                <CostCard
                  title="Última compra comparable"
                  value={formatMoney(product.last_comparable?.cost ?? null)}
                  hint={product.last_comparable ? undefined : "Sin factura/guía comparable desde 2025"}
                  line={product.last_comparable}
                  onOpenReception={onOpenReception}
                />
                <CostCard
                  title="Compra comparable anterior"
                  value={formatMoney(product.previous_comparable?.cost ?? null)}
                  hint={
                    product.change
                      ? `Diferencia ${formatMoney(product.change.amount)} (${formatPct(product.change.pct)})`
                      : "Sin compra anterior para comparar"
                  }
                  line={product.previous_comparable}
                  onOpenReception={onOpenReception}
                />
              </section>

              {product.last_reception &&
              product.last_reception.reception_id !== product.last_comparable?.reception_id ? (
                <div className="flex flex-wrap items-center gap-2 rounded-md border border-dashed p-2 text-sm">
                  <span className="text-muted-foreground">Último movimiento (no comparable):</span>
                  <MovementBadge movement={product.last_reception.movement_class} />
                  <span>
                    {formatDate(product.last_reception.admission_date)} · {formatMoney(product.last_reception.cost)}
                  </span>
                  <button
                    type="button"
                    className="text-primary hover:underline"
                    onClick={() =>
                      onOpenReception(product.last_reception!.reception_id, product.last_reception!.detail_id)
                    }
                  >
                    Ver recepción
                  </button>
                </div>
              ) : null}

              <section className="grid gap-3 sm:grid-cols-2">
                <div className="rounded-md border p-3">
                  <h3 className="text-sm font-semibold">Alertas</h3>
                  <div className="mt-2">
                    <AnomalyList anomalies={product.anomalies} />
                  </div>
                </div>
                <div className="rounded-md border p-3 text-sm">
                  <h3 className="font-semibold">Impuestos y costo bruto</h3>
                  <p className="mt-2">
                    {TAX_LABELS[product.tax_profile.status]}
                    {product.tax_profile.total_rate_pct ? ` · ${product.tax_profile.total_rate_pct}%` : ""}
                  </p>
                  <p className="text-[11px] text-muted-foreground">
                    {product.tax_profile.taxes
                      .map((t) => `${t.name}${t.code ? ` (SII ${t.code})` : ""}`)
                      .join(", ") || product.tax_profile.reason}
                  </p>
                  {product.tax_profile.issues.length ? (
                    <p className="text-[11px] text-orange-700">
                      {product.tax_profile.issues.map((i) => TAX_ISSUE_LABELS[i] ?? i).join(" · ")}
                    </p>
                  ) : null}
                  <dl className="mt-2 grid grid-cols-2 gap-x-3 gap-y-0.5 text-[12px]">
                    <dt className="text-muted-foreground">Costo neto promedio</dt>
                    <dd className="text-right tabular-nums">{formatMoney(product.commercial_gross_cost.net)}</dd>
                    <dt className="text-muted-foreground">IVA aplicado</dt>
                    <dd className="text-right tabular-nums">{product.commercial_gross_cost.iva_rate_pct ?? "—"}%</dd>
                    <dt className="text-muted-foreground">ILA aplicado</dt>
                    <dd className="text-right tabular-nums">{product.commercial_gross_cost.ila_rate_pct ?? "—"}%</dd>
                    <dt className="text-muted-foreground">IDs Bsale aplicados</dt>
                    <dd className="text-right tabular-nums">
                      {product.commercial_gross_cost.taxes_applied?.length
                        ? product.commercial_gross_cost.taxes_applied
                            .map((t) => `#${t.tax_id} (${t.percentage ?? "—"}%)`)
                            .join(" + ")
                        : "—"}
                    </dd>
                    <dt className="text-muted-foreground">Factor</dt>
                    <dd className="text-right tabular-nums">{product.commercial_gross_cost.factor ?? "—"}</dd>
                    <dt className="font-medium">Costo bruto comercial</dt>
                    <dd className="text-right font-semibold tabular-nums">
                      {formatMoney(product.commercial_gross_cost.gross)}
                    </dd>
                  </dl>
                  <p className="text-[11px] text-amber-700">
                    {GROSS_STATUS_LABELS[product.commercial_gross_cost.status]}
                    {product.meta.tax_config_verified ? "" : " · configuración tributaria aún no contrastada"}
                  </p>
                  <p className="mt-2 text-[11px] text-muted-foreground">
                    Bruto sobre última recepción: {formatMoney(product.gross_cost_preliminary.value)}
                    {product.meta.net_cost_verified
                      ? ""
                      : " · el carácter neto del costo de recepción aún no se valida contra facturas"}
                  </p>
                </div>
              </section>

              <section className="rounded-md border p-3">
                <h3 className="text-sm font-semibold">Evolución del costo de compra comparable</h3>
                {series.length === 0 ? (
                  <p className="py-8 text-center text-sm text-muted-foreground">
                    Sin compras comparables para graficar.
                  </p>
                ) : (
                  <ChartContainer config={chartConfig} className="mt-2 h-[220px] w-full">
                    <LineChart data={series} margin={{ left: 4, right: 8, top: 8, bottom: 0 }}>
                      <CartesianGrid vertical={false} />
                      <XAxis dataKey="date" tickLine={false} axisLine={false} fontSize={11} minTickGap={24} />
                      <YAxis
                        tickLine={false}
                        axisLine={false}
                        fontSize={11}
                        width={56}
                        tickFormatter={(v) => formatMoney(Number(v))}
                      />
                      <ChartTooltip content={<ChartTooltipContent formatter={(v) => formatMoney(Number(v))} />} />
                      {average !== null && average > 0 ? (
                        <ReferenceLine y={average} stroke="hsl(160 45% 35%)" strokeDasharray="4 4" />
                      ) : null}
                      <Line type="stepAfter" dataKey="cost" stroke="var(--color-cost)" strokeWidth={2} dot={{ r: 2 }} />
                    </LineChart>
                  </ChartContainer>
                )}
                <p className="mt-1 text-[11px] text-muted-foreground">
                  Línea punteada: costo promedio Bsale actual.
                  {history?.series_truncated ? " Se muestran los últimos 1.000 puntos." : ""}
                </p>
              </section>

              <section className="space-y-2">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <h3 className="text-sm font-semibold">Recepciones desde 01-01-2025</h3>
                  <select
                    className="h-8 rounded-md border border-input bg-background px-2 text-sm"
                    value={officeId}
                    onChange={(e) => {
                      setOfficeId(e.target.value)
                      setPage(1)
                    }}
                  >
                    <option value="">Todas las sucursales</option>
                    {offices.map((o) => (
                      <option key={o.id} value={o.id}>
                        {o.name ?? `Oficina ${o.id}`}
                      </option>
                    ))}
                  </select>
                </div>
                <div className={cn("overflow-x-auto rounded-md border", historyLoading && "opacity-60")}>
                  <table className="w-full min-w-[720px] text-sm">
                    <thead className="bg-muted/40 text-left text-[11px] uppercase text-muted-foreground">
                      <tr>
                        <th className="px-2 py-1.5 font-medium">Fecha</th>
                        <th className="px-2 py-1.5 font-medium">Movimiento</th>
                        <th className="px-2 py-1.5 font-medium">Documento</th>
                        <th className="px-2 py-1.5 text-right font-medium">Cant.</th>
                        <th className="px-2 py-1.5 text-right font-medium">Costo</th>
                        <th className="px-2 py-1.5 text-right font-medium">Var.</th>
                        <th className="px-2 py-1.5 font-medium">Alertas</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-border/60">
                      {(history?.items ?? []).map((l) => (
                        <tr
                          key={`${l.reception_id}-${l.detail_id}`}
                          className={cn("cursor-pointer hover:bg-muted/40", !l.comparable && "text-muted-foreground")}
                          onClick={() => onOpenReception(l.reception_id, l.detail_id)}
                        >
                          <td className="px-2 py-1.5">
                            {formatDate(l.admission_date)}
                            <span className="block text-[11px]">{l.office_name ?? l.office_id ?? "—"}</span>
                          </td>
                          <td className="px-2 py-1.5">
                            <MovementBadge movement={l.movement_class} />
                          </td>
                          <td className="max-w-[180px] truncate px-2 py-1.5 text-xs">
                            {l.document ?? "—"} {l.document_number ? `N° ${l.document_number}` : ""}
                          </td>
                          <td className="px-2 py-1.5 text-right tabular-nums">{formatQty(l.quantity)}</td>
                          <td className="px-2 py-1.5 text-right tabular-nums">{formatMoney(l.cost)}</td>
                          <td className="px-2 py-1.5 text-right tabular-nums">{formatPct(l.change_pct)}</td>
                          <td className="px-2 py-1.5">
                            <AnomalyChips anomalies={l.anomalies} max={2} />
                          </td>
                        </tr>
                      ))}
                      {history && history.items.length === 0 ? (
                        <tr>
                          <td colSpan={7} className="px-2 py-6 text-center text-muted-foreground">
                            Sin recepciones en RAW desde 2025.
                          </td>
                        </tr>
                      ) : null}
                    </tbody>
                  </table>
                </div>
                {history ? (
                  <Pager
                    page={history.page}
                    totalPages={history.total_pages}
                    total={history.total}
                    loading={historyLoading}
                    onPage={setPage}
                  />
                ) : null}
              </section>
            </>
          ) : null}
        </div>
      </SheetContent>
    </Sheet>
  )
}
