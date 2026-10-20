"use client"

import { Fragment, useEffect, useState } from "react"
import { AlertTriangle, Calculator, CircleDollarSign, Layers3, Loader2, Percent } from "lucide-react"

import { MetricCard, SectionCard, StatusBadge } from "@/components/analytics-v2/shared/analytics-ui"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from "@/components/ui/sheet"
import { cn } from "@/lib/utils"
import { getPackDetail } from "@/lib/analytics-v2/api"
import { RELIABILITY_LABELS, formatDateTime, formatMoney, formatPct, formatQty } from "@/lib/analytics-v2/labels"
import { COST_BASIS_LABELS, MARGIN_STATUS_LABELS, MARGIN_STATUS_TONES } from "@/lib/analytics-v2/margins-labels"
import { PACK_COMPONENT_STATUS_LABELS, PACK_PROBLEM_LABELS } from "@/lib/analytics-v2/packs-labels"
import type { PackDetailResponse, PackLine } from "@/lib/analytics-v2/packs-types"

import { PackStatusBadge } from "./pack-v2-table"

function taxesText(line: PackLine): string {
  if (line.is_pack) return "Pack anidado: impuestos por componente"
  if (!line.taxes_applied.length) return line.tax_ids.length ? `IDs ${line.tax_ids.join(", ")} sin resolver` : "—"
  return line.taxes_applied
    .map((t) => `${t.kind === "ila" ? "ILA" : t.kind === "iva" ? "IVA" : "IVA anticipado"} ${formatPct(t.percentage)}`)
    .join(" + ")
}

function ComponentRows({ lines, depth = 0 }: { lines: PackLine[]; depth?: number }) {
  return (
    <>
      {lines.map((l) => (
        <Fragment key={`${depth}-${l.position}-${l.variant_id ?? "x"}`}>
          <tr className="align-top">
            <td className="px-2 py-1.5" style={{ paddingLeft: 8 + depth * 16 }}>
              <span className="block font-medium">{l.name ?? (l.variant_id ? `Variante ${l.variant_id}` : "Variante no determinada")}</span>
              <span className="block text-[11px] text-muted-foreground">
                {l.code ?? "—"} · #{l.variant_id ?? "—"}
                {l.active === false ? " · inactivo" : ""}
              </span>
            </td>
            <td className="px-2 py-1.5 text-right tabular-nums">{formatQty(l.quantity)}</td>
            <td className="px-2 py-1.5 text-right tabular-nums">{formatMoney(l.unit_net)}</td>
            <td className="px-2 py-1.5 text-[11px]">
              {taxesText(l)}
              {l.tax_factor ? <span className="block text-muted-foreground">factor {l.tax_factor}</span> : null}
            </td>
            <td className="px-2 py-1.5 text-right tabular-nums">{formatMoney(l.unit_gross)}</td>
            <td className="px-2 py-1.5 text-right tabular-nums">{formatMoney(l.subtotal_net)}</td>
            <td className="px-2 py-1.5 text-right tabular-nums">{formatMoney(l.subtotal_gross)}</td>
            <td className="px-2 py-1.5 text-[11px]">
              <span className={cn(l.status !== "calculado" && "font-medium text-red-700 dark:text-red-300")}>{PACK_COMPONENT_STATUS_LABELS[l.status]}</span>
              {l.reliability ? <span className="block text-muted-foreground">{RELIABILITY_LABELS[l.reliability]}</span> : null}
              {l.cost_change_pct ? <span className="block text-muted-foreground">Última variación {formatPct(l.cost_change_pct)}</span> : null}
            </td>
          </tr>
          {l.nested_pack ? <ComponentRows lines={l.nested_pack.lines} depth={depth + 1} /> : null}
        </Fragment>
      ))}
    </>
  )
}

function ComponentCards({ lines, depth = 0 }: { lines: PackLine[]; depth?: number }) {
  return (
    <div className="space-y-3">
      {lines.map((line) => (
        <div key={`${depth}-${line.position}-${line.variant_id ?? "x"}`} className="rounded-lg border border-lq-line bg-lq-surface p-4" style={{ marginLeft: depth * 12 }}>
          <div className="flex items-start justify-between gap-3">
            <div className="min-w-0">
              <p className="truncate text-sm font-semibold text-lq-ink">{line.name ?? (line.variant_id ? `Variante ${line.variant_id}` : "Variante no determinada")}</p>
              <p className="mt-1 text-xs text-lq-muted">{line.code ?? "Sin SKU"} · Cantidad {formatQty(line.quantity)}</p>
            </div>
            <StatusBadge tone={line.status === "calculado" ? "success" : "warning"}>{PACK_COMPONENT_STATUS_LABELS[line.status]}</StatusBadge>
          </div>
          <dl className="mt-4 grid grid-cols-2 gap-3 text-sm">
            <div><dt className="text-xs text-lq-muted">Costo neto unitario</dt><dd className="mt-0.5 font-medium tabular-nums">{formatMoney(line.unit_net)}</dd></div>
            <div><dt className="text-xs text-lq-muted">Costo bruto unitario</dt><dd className="mt-0.5 font-medium tabular-nums">{formatMoney(line.unit_gross)}</dd></div>
            <div><dt className="text-xs text-lq-muted">Impuestos</dt><dd className="mt-0.5 font-medium">{taxesText(line)}</dd></div>
            <div><dt className="text-xs text-lq-muted">Subtotal bruto</dt><dd className="mt-0.5 font-semibold tabular-nums">{formatMoney(line.subtotal_gross)}</dd></div>
          </dl>
          {line.nested_pack ? <div className="mt-3"><ComponentCards lines={line.nested_pack.lines} depth={depth + 1} /></div> : null}
        </div>
      ))}
    </div>
  )
}

export function PackV2DetailSheet({
  companyId,
  variantId,
  costBasis,
  onOpenChange,
}: {
  companyId: number
  variantId: number | null
  costBasis: string
  onOpenChange: (open: boolean) => void
}) {
  const [data, setData] = useState<PackDetailResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (variantId == null) {
      setData(null)
      return
    }
    const ac = new AbortController()
    setLoading(true)
    setError(null)
    getPackDetail(companyId, variantId, costBasis, ac.signal)
      .then(setData)
      .catch((e: unknown) => {
        if (!ac.signal.aborted) setError(e instanceof Error ? e.message : "Error al cargar el pack")
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoading(false)
      })
    return () => ac.abort()
  }, [companyId, variantId, costBasis])

  const pack = data?.pack
  return (
    <Sheet open={variantId != null} onOpenChange={onOpenChange}>
      <SheetContent side="right" className="w-full overflow-y-auto border-l-lq-line bg-lq-canvas p-0 sm:max-w-[min(1100px,92vw)]">
        <SheetHeader className="sticky top-0 z-20 border-b border-lq-line bg-lq-surface/95 px-5 py-4 backdrop-blur md:px-6">
          <SheetTitle className="pr-8 text-xl tracking-[-0.015em]">{data?.product_name ?? (loading ? "Cargando…" : "Pack")}</SheetTitle>
          <SheetDescription>
            {data ? `${data.code ?? "Sin SKU"} · variante #${data.variant_id} · ${data.pack_state} · ${pack?.components_count ?? 0} componentes` : null}
          </SheetDescription>
        </SheetHeader>

        <div className="space-y-5 px-4 pb-8 md:px-6">
          {loading && !data ? (
            <div className="flex justify-center py-10">
              <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
            </div>
          ) : null}
          {error ? (
            <Alert variant="destructive">
              <AlertTriangle className="h-4 w-4" />
              <AlertTitle>Error</AlertTitle>
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          ) : null}

          {data && pack ? (
            <>
              <div className="flex flex-wrap items-center justify-between gap-3 pt-1">
                <div>
                  <p className="text-xs font-medium uppercase tracking-[0.14em] text-lq-muted">Resultado del cálculo</p>
                  <p className="mt-1 text-sm text-lq-body">Base {COST_BASIS_LABELS[pack.basis]} · composición {formatDateTime(pack.composition_fetched_at)}</p>
                </div>
                <PackStatusBadge status={pack.status} />
              </div>

              <section className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                <MetricCard label="Costo neto total" value={formatMoney(pack.net_total)} icon={Layers3} tone="neutral" context="Suma neta de componentes" />
                <MetricCard label="Costo bruto comercial" value={formatMoney(pack.gross_total)} icon={CircleDollarSign} tone={pack.calculable ? "warning" : "neutral"} context="Incluye IVA e ILA por componente" />
                <MetricCard label="Recargo de referencia" value={formatPct(data.rows.find((r) => r.markup_pct != null)?.markup_pct ?? null)} icon={Percent} context="Primera lista con precio evaluable" />
                <MetricCard label="Margen de referencia" value={formatPct(data.rows.find((r) => r.margin_on_sale_pct != null)?.margin_on_sale_pct ?? null)} icon={Calculator} context="Precio menos costo sobre precio" />
              </section>

              {data.problems.length ? (
                <Alert>
                  <AlertTriangle className="h-4 w-4" />
                  <AlertTitle>Señales para revisión</AlertTitle>
                  <AlertDescription>
                    {data.problems.map((p) => PACK_PROBLEM_LABELS[p]).join(" · ")}. No se cambian precios ni costos automáticamente.
                  </AlertDescription>
                </Alert>
              ) : null}

              <SectionCard
                title={`Composición del pack (${pack.components_count})`}
                description="Cada componente aporta su cantidad, costo neto e impuestos propios al costo total."
                contentClassName="p-0"
              >
                <div className="p-4 md:hidden"><ComponentCards lines={pack.lines} /></div>
                <div className="hidden overflow-x-auto md:block">
                  <table className="w-full min-w-[820px] text-sm">
                    <thead className="border-b border-lq-line-soft bg-lq-canvas text-left text-xs text-lq-muted">
                      <tr>
                        <th className="px-4 py-3 font-medium">Componente</th>
                        <th className="px-3 py-3 text-right font-medium">Cantidad</th>
                        <th className="px-3 py-3 text-right font-medium">Neto unit.</th>
                        <th className="px-3 py-3 font-medium">Impuestos</th>
                        <th className="px-3 py-3 text-right font-medium">Bruto unit.</th>
                        <th className="px-3 py-3 text-right font-medium">Subtotal neto</th>
                        <th className="px-3 py-3 text-right font-medium">Subtotal bruto</th>
                        <th className="px-4 py-3 font-medium">Estado</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-lq-line-soft">
                      <ComponentRows lines={pack.lines} />
                      {pack.lines.length === 0 ? (
                        <tr>
                          <td colSpan={8} className="px-2 py-6 text-center text-muted-foreground">
                            Sin composición utilizable en BSALE RAW ({pack.composition_issues.join(", ") || "sin detalle"}).
                          </td>
                        </tr>
                      ) : null}
                    </tbody>
                  </table>
                </div>
                {pack.warnings.length ? (
                  <p className="text-[11px] text-amber-800">
                    {pack.warnings.map((w) => PACK_PROBLEM_LABELS[w as keyof typeof PACK_PROBLEM_LABELS] ?? w.replaceAll("_", " ")).join(" · ")}
                  </p>
                ) : null}
              </SectionCard>

              <SectionCard
                title="Precios y rentabilidad por lista"
                description="Compara el mismo costo del pack con cada precio bruto vigente."
                contentClassName="p-0"
              >
                <div className="overflow-x-auto">
                  <table className="w-full min-w-[680px] text-sm">
                    <thead className="border-b border-lq-line-soft bg-lq-canvas text-left text-xs text-lq-muted">
                      <tr>
                        <th className="px-5 py-3 font-medium">Lista</th>
                        <th className="px-4 py-3 text-right font-medium">Precio bruto</th>
                        <th className="px-4 py-3 text-right font-medium">Recargo</th>
                        <th className="px-4 py-3 text-right font-medium">Margen venta</th>
                        <th className="px-4 py-3 text-right font-medium">Política mín / máx</th>
                        <th className="px-5 py-3 font-medium">Estado</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-lq-line-soft">
                      {data.rows.map((r) => (
                        <tr key={r.price_list.id}>
                          <td className="px-5 py-3">
                            {r.price_list.label ?? r.price_list.name} <span className="text-muted-foreground">#{r.price_list.id}</span>
                          </td>
                          <td className="px-4 py-3 text-right font-medium tabular-nums">{formatMoney(r.price_gross)}</td>
                          <td className="px-4 py-3 text-right tabular-nums">{formatPct(r.markup_pct)}</td>
                          <td className="px-4 py-3 text-right tabular-nums">{formatPct(r.margin_on_sale_pct)}</td>
                          <td className="px-4 py-3 text-right tabular-nums">
                            {r.rule ? `${formatPct(r.rule.min_markup_pct)} / ${Number(r.rule.max_markup_pct) > 0 ? formatPct(r.rule.max_markup_pct) : "—"}` : "Sin regla"}
                          </td>
                          <td className="px-5 py-3">
                            <span className={cn("rounded-md border px-1.5 py-0.5 text-[11px]", MARGIN_STATUS_TONES[r.margin_status])}>
                              {MARGIN_STATUS_LABELS[r.margin_status]}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                {data.list_price_spread?.alert ? (
                  <p className="border-t border-red-100 bg-red-50 dark:border-red-400/20 dark:bg-red-400/10 px-5 py-3 text-xs text-red-800 dark:text-red-200">
                    Diferencia entre listas: máximo {formatMoney(data.list_price_spread.max)} vs mínimo {formatMoney(data.list_price_spread.min)}
                    (×{data.list_price_spread.ratio}). Confirmar en BSALE RAW antes de clasificarlo como anomalía.
                  </p>
                ) : null}
                <p className="border-t border-lq-line-soft px-5 py-3 text-xs text-lq-muted">
                  Rentabilidad estimada con precios vigentes y costos actuales; la rentabilidad histórica de ventas no está implementada.
                </p>
              </SectionCard>

              <SectionCard title="¿Cómo se calcula?" description="Explicación del cálculo comercial, sin modificar datos de Bsale.">
                <ol className="grid gap-3 text-sm leading-6 text-lq-body md:grid-cols-3">
                  <li className="rounded-lg bg-lq-canvas p-3"><strong className="block text-lq-ink">1. Costo neto</strong>Cantidad × costo neto de cada componente.</li>
                  <li className="rounded-lg bg-lq-canvas p-3"><strong className="block text-lq-ink">2. Costo bruto</strong>Al costo neto se suma IVA e ILA propios de cada componente.</li>
                  <li className="rounded-lg bg-lq-canvas p-3"><strong className="block text-lq-ink">3. Rentabilidad</strong>El precio de cada lista se compara con el costo bruto total.</li>
                </ol>
                <p className="mt-4 text-xs leading-5 text-lq-muted">
                  El costo promedio propio del SKU del pack ({formatMoney(data.pack_sku_own.bsale_average_cost.value)}) y sus impuestos propios
                  no participan del cálculo. El resultado continúa siendo provisional hasta la validación contable.
                </p>
              </SectionCard>
            </>
          ) : null}
        </div>
      </SheetContent>
    </Sheet>
  )
}
