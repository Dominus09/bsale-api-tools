"use client"

import { Fragment, useEffect, useState } from "react"
import { AlertTriangle, Loader2 } from "lucide-react"

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
  return line.taxes_applied.map((t) => `#${t.tax_id} ${formatPct(t.percentage)}`).join(" + ")
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
              <span className={cn(l.status !== "calculado" && "font-medium text-red-700")}>{PACK_COMPONENT_STATUS_LABELS[l.status]}</span>
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
      <SheetContent side="right" className="w-full overflow-y-auto sm:max-w-4xl">
        <SheetHeader>
          <SheetTitle>{data?.product_name ?? (loading ? "Cargando…" : "Pack")}</SheetTitle>
          <SheetDescription>
            {data ? `${data.code ?? "—"} · variante #${data.variant_id} · ${data.pack_state}` : null}
          </SheetDescription>
        </SheetHeader>

        <div className="space-y-4 px-4 pb-6">
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
              <section className="grid gap-2 rounded-md border p-3 text-sm sm:grid-cols-2">
                <div>
                  <span className="block text-[11px] uppercase text-muted-foreground">Costo del pack</span>
                  <PackStatusBadge status={pack.status} />
                  <span className="mt-1 block text-[11px] text-muted-foreground">
                    Base {COST_BASIS_LABELS[pack.basis]} de cada componente · composición {formatDateTime(pack.composition_fetched_at)}
                  </span>
                </div>
                <div className="grid grid-cols-2 gap-2 tabular-nums">
                  <div>
                    <span className="block text-[11px] uppercase text-muted-foreground">Costo neto total</span>
                    {formatMoney(pack.net_total)}
                  </div>
                  <div>
                    <span className="block text-[11px] uppercase text-muted-foreground">Costo bruto comercial</span>
                    {formatMoney(pack.gross_total)}
                  </div>
                </div>
                <p className="text-[11px] text-muted-foreground sm:col-span-2">
                  El costo promedio propio del SKU del pack ({formatMoney(data.pack_sku_own.bsale_average_cost.value)}) y sus impuestos propios
                  (IDs {data.pack_sku_own.tax_ids?.join(", ") || "—"}) no se usan: cada componente aporta su costo y sus impuestos.
                </p>
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

              <section className="space-y-2">
                <h3 className="text-sm font-semibold">Componentes ({pack.components_count})</h3>
                <div className="overflow-x-auto rounded-md border">
                  <table className="w-full min-w-[820px] text-xs">
                    <thead className="bg-muted/40 text-left text-[10px] uppercase tracking-wide text-muted-foreground">
                      <tr>
                        <th className="px-2 py-1.5 font-medium">Componente</th>
                        <th className="px-2 py-1.5 text-right font-medium">Cantidad</th>
                        <th className="px-2 py-1.5 text-right font-medium">Costo neto unit.</th>
                        <th className="px-2 py-1.5 font-medium">Impuestos (ID Bsale)</th>
                        <th className="px-2 py-1.5 text-right font-medium">Costo bruto unit.</th>
                        <th className="px-2 py-1.5 text-right font-medium">Subtotal neto</th>
                        <th className="px-2 py-1.5 text-right font-medium">Subtotal bruto</th>
                        <th className="px-2 py-1.5 font-medium">Estado</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-border/60">
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
              </section>

              <section className="space-y-2">
                <h3 className="text-sm font-semibold">Rentabilidad estimada actual por lista</h3>
                <div className="overflow-x-auto rounded-md border">
                  <table className="w-full text-xs">
                    <thead className="bg-muted/40 text-left text-[10px] uppercase tracking-wide text-muted-foreground">
                      <tr>
                        <th className="px-2 py-1.5 font-medium">Lista</th>
                        <th className="px-2 py-1.5 text-right font-medium">Precio bruto</th>
                        <th className="px-2 py-1.5 text-right font-medium">Recargo %</th>
                        <th className="px-2 py-1.5 text-right font-medium">Margen s/venta</th>
                        <th className="px-2 py-1.5 text-right font-medium">Política mín / máx</th>
                        <th className="px-2 py-1.5 font-medium">Estado</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-border/60">
                      {data.rows.map((r) => (
                        <tr key={r.price_list.id}>
                          <td className="px-2 py-1.5">
                            {r.price_list.label ?? r.price_list.name} <span className="text-muted-foreground">#{r.price_list.id}</span>
                          </td>
                          <td className="px-2 py-1.5 text-right tabular-nums">{formatMoney(r.price_gross)}</td>
                          <td className="px-2 py-1.5 text-right tabular-nums">{formatPct(r.markup_pct)}</td>
                          <td className="px-2 py-1.5 text-right tabular-nums">{formatPct(r.margin_on_sale_pct)}</td>
                          <td className="px-2 py-1.5 text-right tabular-nums">
                            {r.rule ? `${formatPct(r.rule.min_markup_pct)} / ${Number(r.rule.max_markup_pct) > 0 ? formatPct(r.rule.max_markup_pct) : "—"}` : "Sin regla"}
                          </td>
                          <td className="px-2 py-1.5">
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
                  <p className="text-[11px] text-red-800">
                    Diferencia entre listas: máximo {formatMoney(data.list_price_spread.max)} vs mínimo {formatMoney(data.list_price_spread.min)}
                    (×{data.list_price_spread.ratio}). Confirmar en BSALE RAW antes de clasificarlo como anomalía.
                  </p>
                ) : null}
                <p className="text-[11px] text-muted-foreground">
                  Rentabilidad estimada con precios vigentes y costos actuales; la rentabilidad histórica de ventas no está implementada.
                </p>
              </section>
            </>
          ) : null}
        </div>
      </SheetContent>
    </Sheet>
  )
}
