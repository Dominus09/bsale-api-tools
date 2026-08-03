"use client"

import { useEffect, useState, type ReactNode } from "react"

import { CostV2InfoHint } from "@/components/costos-v2/cost-v2-info-hint"
import { CostV2StatusBadge } from "@/components/costos-v2/cost-v2-status-badge"
import { ChangeCell } from "@/components/costos-v2/cost-v2-recent-changes"
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Separator } from "@/components/ui/separator"
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet"
import { Skeleton } from "@/components/ui/skeleton"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { CostV2ApiError, getCostV2ProductDetail } from "@/lib/costos-v2/api"
import {
  additionalTaxCategoryLabel,
  displayCorrectedGross,
  displayCorrectedGrossPrecise,
  explanationForStatus,
  formatDateCL,
  formatMoneyCLPPrecise,
  formatMoneyCLPTable,
  formatPercentCL,
  formatTaxRate,
} from "@/lib/costos-v2/format"
import {
  statusLabel,
  statusShortHelp,
  warningLabel,
} from "@/lib/costos-v2/labels"
import type { CostV2ProductItem } from "@/lib/costos-v2/types"

function Field({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="space-y-0.5">
      <dt className="text-[11px] uppercase tracking-wide text-muted-foreground">{label}</dt>
      <dd className="text-sm">{value ?? "—"}</dd>
    </div>
  )
}

export function CostV2ProductDetailDrawer({
  open,
  onOpenChange,
  variantId,
  companyId,
  officeId,
  dateFrom,
  dateTo,
  onOpenSymbology,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  variantId: number | null
  companyId: number
  officeId: number
  dateFrom: string
  dateTo: string
  onOpenSymbology?: () => void
}) {
  const [item, setItem] = useState<CostV2ProductItem | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!open || variantId == null || !companyId || !officeId) return
    const ac = new AbortController()
    setLoading(true)
    setError(null)
    setItem(null)
    ;(async () => {
      try {
        const res = await getCostV2ProductDetail({
          company_id: companyId,
          office_id: officeId,
          variant_id: variantId,
          date_from: dateFrom,
          date_to: dateTo,
          history_limit: 30,
          signal: ac.signal,
        })
        if (!ac.signal.aborted) setItem(res.item)
      } catch (e) {
        if (ac.signal.aborted) return
        if (e instanceof CostV2ApiError) setError(e.message)
        else setError("Error de red al cargar el producto.")
      } finally {
        if (!ac.signal.aborted) setLoading(false)
      }
    })()
    return () => ac.abort()
  }, [open, variantId, companyId, officeId, dateFrom, dateTo])

  const status = item?.current_quality_status
  const explain = explanationForStatus(status)
  const calc = item?.calculation

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="w-full overflow-y-auto sm:max-w-xl">
        <SheetHeader>
          <SheetTitle className="pr-6 text-left leading-snug">
            {item?.product_name || "Detalle de producto"}
          </SheetTitle>
        </SheetHeader>

        {loading ? (
          <div className="mt-4 space-y-2">
            <Skeleton className="h-4 w-2/3" />
            <Skeleton className="h-20 w-full" />
          </div>
        ) : null}

        {error ? (
          <Alert variant="destructive" className="mt-4">
            <AlertTitle>No se pudo cargar</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        ) : null}

        {item && !loading ? (
          <div className="mt-4 space-y-5">
            <div className="space-y-1">
              <p className="text-sm text-muted-foreground">
                {[item.variant_name, item.barcode].filter(Boolean).join(" · ")}
              </p>
              <div className="flex flex-wrap items-end gap-3">
                <div>
                  <p className="text-[11px] uppercase text-muted-foreground">Costo vigente</p>
                  <p className="text-xl font-semibold tabular-nums">
                    {displayCorrectedGross(item.current_corrected_gross_cost)}
                  </p>
                </div>
                <ChangeCell
                  amount={item.unit_change_amount}
                  percent={item.unit_change_percent}
                />
                <p className="text-xs text-muted-foreground">
                  Última recepción {formatDateCL(item.latest_admission_date)}
                  {item.latest_document_number != null
                    ? ` · Doc ${item.latest_document_number}`
                    : ""}
                </p>
              </div>
            </div>

            <Separator />

            <section className="space-y-2">
              <h3 className="text-sm font-semibold">Costo vigente</h3>
              {explain ? <p className="text-sm text-muted-foreground">{explain}</p> : null}
              <dl className="grid grid-cols-2 gap-2">
                <Field
                  label="Neto"
                  value={formatMoneyCLPPrecise(item.current_stored_cost_net)}
                />
                <Field
                  label="IVA"
                  value={formatMoneyCLPPrecise(
                    calc?.iva?.amount ?? item.current_calculated_iva_amount,
                  )}
                />
                <Field
                  label="Impuestos adicionales"
                  value={formatMoneyCLPPrecise(item.current_additional_tax_amount_total)}
                />
                <Field
                  label="Bruto corregido"
                  value={displayCorrectedGrossPrecise(item.current_corrected_gross_cost)}
                />
                <Field label="Tasa total" value={formatTaxRate(item.current_total_tax_rate)} />
              </dl>
              {(item.current_additional_taxes ?? []).length > 0 ? (
                <ul className="space-y-1.5">
                  {(item.current_additional_taxes ?? []).map((t) => (
                    <li
                      key={`${t.tax_id}-${t.category}`}
                      className="flex items-center justify-between rounded border border-border/60 px-2 py-1.5 text-xs"
                    >
                      <span>
                        {t.name || `Tax ${t.tax_id}`} ·{" "}
                        {additionalTaxCategoryLabel(t.category)}
                      </span>
                      <span className="tabular-nums">
                        {formatMoneyCLPPrecise(t.amount)}
                      </span>
                    </li>
                  ))}
                </ul>
              ) : null}
            </section>

            <section className="space-y-2">
              <h3 className="text-sm font-semibold">Comparación</h3>
              <dl className="grid grid-cols-2 gap-2">
                <Field
                  label="Costo anterior"
                  value={formatMoneyCLPPrecise(item.previous_corrected_gross_cost)}
                />
                <Field
                  label="Costo actual"
                  value={displayCorrectedGrossPrecise(item.current_corrected_gross_cost)}
                />
                <Field
                  label="Diferencia unitaria"
                  value={formatMoneyCLPPrecise(item.unit_change_amount)}
                />
                <Field
                  label="Variación %"
                  value={formatPercentCL(item.unit_change_percent)}
                />
              </dl>
            </section>

            <section className="space-y-2">
              <h3 className="text-sm font-semibold">Historial</h3>
              <div className="overflow-x-auto rounded border border-border/60">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Fecha</TableHead>
                      <TableHead>Doc</TableHead>
                      <TableHead className="text-right">Neto</TableHead>
                      <TableHead className="text-right">Bruto V2</TableHead>
                      <TableHead>Estado</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {(item.receptions ?? []).map((r) => (
                      <TableRow key={r.history_id}>
                        <TableCell className="whitespace-nowrap text-xs">
                          {formatDateCL(r.admission_date)}
                        </TableCell>
                        <TableCell className="text-xs">
                          {r.document_number ?? "—"}
                        </TableCell>
                        <TableCell className="text-right text-xs tabular-nums">
                          {formatMoneyCLPTable(r.stored_cost_net)}
                        </TableCell>
                        <TableCell className="text-right text-xs tabular-nums">
                          {displayCorrectedGross(r.corrected_gross_cost)}
                        </TableCell>
                        <TableCell>
                          <CostV2StatusBadge status={r.effective_quality_status} />
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            </section>

            <section className="space-y-2">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <h3 className="inline-flex items-center gap-1.5 text-sm font-semibold">
                  Estado del costo
                  <CostV2InfoHint
                    title={statusLabel(status)}
                    text={statusShortHelp(status)}
                  />
                </h3>
                {onOpenSymbology ? (
                  <Button
                    type="button"
                    variant="link"
                    size="sm"
                    className="h-auto px-0 text-xs"
                    onClick={onOpenSymbology}
                  >
                    Ver simbología
                  </Button>
                ) : null}
              </div>
              <div className="flex flex-wrap items-center gap-2">
                <CostV2StatusBadge status={status} showHelp={false} />
                {(item.current_warnings ?? []).map((w) => (
                  <Badge key={w} variant="outline" className="font-normal">
                    {warningLabel(w)}
                  </Badge>
                ))}
              </div>
              <p className="text-sm text-muted-foreground">{statusShortHelp(status)}</p>
              <dl className="grid grid-cols-2 gap-2">
                <Field label="Origen tax IDs" value={item.tax_ids_source ?? "—"} />
                <Field label="Origen tasas" value={item.tax_rates_source ?? "—"} />
              </dl>
            </section>

            <Accordion type="single" collapsible>
              <AccordionItem value="trace">
                <AccordionTrigger className="text-sm">
                  Detalle técnico
                </AccordionTrigger>
                <AccordionContent>
                  <dl className="space-y-2 text-xs">
                    <Field
                      label="effective_quality_status"
                      value={
                        <span className="font-mono">{status ?? "—"}</span>
                      }
                    />
                    <Field
                      label="warnings"
                      value={
                        <span className="font-mono">
                          {(item.current_warnings ?? []).join(", ") || "—"}
                        </span>
                      }
                    />
                    <Field label="calculation_version" value={item.calculation_version} />
                    <Field
                      label="source_history_fingerprint"
                      value={
                        <span className="break-all font-mono">
                          {item.source_history_fingerprint ?? "—"}
                        </span>
                      }
                    />
                    <Field
                      label="tax_context_fingerprint"
                      value={
                        <span className="break-all font-mono">
                          {item.tax_context_fingerprint ?? "—"}
                        </span>
                      }
                    />
                    <Field
                      label="calculation_result_fingerprint"
                      value={
                        <span className="break-all font-mono">
                          {item.calculation_result_fingerprint ?? "—"}
                        </span>
                      }
                    />
                  </dl>
                </AccordionContent>
              </AccordionItem>
            </Accordion>

            <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
              Cerrar
            </Button>
          </div>
        ) : null}
      </SheetContent>
    </Sheet>
  )
}
