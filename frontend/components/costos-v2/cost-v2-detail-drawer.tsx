"use client"

import { useEffect, useState, type ReactNode } from "react"

import { CostV2InfoHint } from "@/components/costos-v2/cost-v2-info-hint"
import { CostV2StatusBadge } from "@/components/costos-v2/cost-v2-status-badge"
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
import { CostV2ApiError, getCostV2ReceptionDetail } from "@/lib/costos-v2/api"
import {
  additionalTaxCategoryLabel,
  displayCorrectedGross,
  displayUnitDifference,
  explanationForStatus,
  formatDateCL,
  formatDateTimeCL,
  formatDecimalMoneyCLP,
  formatTaxRate,
} from "@/lib/costos-v2/format"
import {
  statusLabel,
  statusShortHelp,
  warningLabel,
} from "@/lib/costos-v2/labels"
import type { CostV2ReceptionDetail } from "@/lib/costos-v2/types"

function Field({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="space-y-0.5">
      <dt className="text-[11px] uppercase tracking-wide text-muted-foreground">{label}</dt>
      <dd className="text-sm text-foreground">{value ?? "—"}</dd>
    </div>
  )
}

export function CostV2DetailDrawer({
  open,
  onOpenChange,
  historyId,
  companyId,
  officeId,
  onOpenSymbology,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  historyId: number | null
  companyId: number
  officeId: number
  onOpenSymbology?: () => void
}) {
  const [item, setItem] = useState<CostV2ReceptionDetail | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!open || historyId == null || !companyId || !officeId) return
    const ac = new AbortController()
    setLoading(true)
    setError(null)
    setItem(null)
    ;(async () => {
      try {
        const res = await getCostV2ReceptionDetail({
          company_id: companyId,
          office_id: officeId,
          history_id: historyId,
          signal: ac.signal,
        })
        if (!ac.signal.aborted) setItem(res.item)
      } catch (e) {
        if (ac.signal.aborted) return
        if (e instanceof CostV2ApiError) setError(e.message)
        else if (e instanceof DOMException && e.name === "AbortError") return
        else setError("Error de red al cargar el detalle.")
      } finally {
        if (!ac.signal.aborted) setLoading(false)
      }
    })()
    return () => ac.abort()
  }, [open, historyId, companyId, officeId])

  const status = item?.effective_quality_status
  const explain = explanationForStatus(status)
  const calc = item?.calculation

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="w-full overflow-y-auto sm:max-w-lg">
        <SheetHeader>
          <SheetTitle>Detalle recepción V2</SheetTitle>
        </SheetHeader>

        {loading ? (
          <div className="mt-6 space-y-3">
            <Skeleton className="h-4 w-2/3" />
            <Skeleton className="h-4 w-1/2" />
            <Skeleton className="h-24 w-full" />
          </div>
        ) : null}

        {error ? (
          <Alert variant="destructive" className="mt-6">
            <AlertTitle>No se pudo cargar</AlertTitle>
            <AlertDescription className="flex flex-col gap-2">
              <span>{error}</span>
              <Button
                type="button"
                size="sm"
                variant="outline"
                onClick={() => onOpenChange(false)}
              >
                Cerrar
              </Button>
            </AlertDescription>
          </Alert>
        ) : null}

        {item && !loading ? (
          <div className="mt-6 space-y-6">
            <section className="space-y-3">
              <h3 className="text-sm font-semibold">Identificación</h3>
              <dl className="grid grid-cols-2 gap-3">
                <Field label="history_id" value={item.history_id} />
                <Field label="Fecha" value={formatDateCL(item.admission_date)} />
                <Field
                  label="Documento"
                  value={item.document_number ?? item.document ?? "—"}
                />
                <Field label="Oficina" value={item.office_id ?? "—"} />
                <Field label="Producto" value={item.product_name} />
                <Field label="Variante" value={item.variant_name} />
                <Field label="Barcode" value={item.barcode ?? "—"} />
              </dl>
            </section>

            <Separator />

            <section className="space-y-3">
              <h3 className="text-sm font-semibold">Costos originales</h3>
              <dl className="grid grid-cols-2 gap-3">
                <Field
                  label="Costo neto"
                  value={formatDecimalMoneyCLP(item.stored_cost_net)}
                />
                <Field
                  label="Bruto almacenado"
                  value={formatDecimalMoneyCLP(item.stored_cost_gross)}
                />
                <Field
                  label="IVA almacenado"
                  value={formatDecimalMoneyCLP(item.stored_iva_amount)}
                />
                <Field
                  label="Otros impuestos almacenados"
                  value={formatDecimalMoneyCLP(item.stored_other_taxes)}
                />
              </dl>
            </section>

            <Separator />

            <section className="space-y-3">
              <h3 className="text-sm font-semibold">Cálculo V2</h3>
              {explain ? (
                <p className="text-sm text-muted-foreground">{explain}</p>
              ) : null}
              <dl className="grid grid-cols-2 gap-3">
                <Field
                  label="Costo neto base"
                  value={formatDecimalMoneyCLP(calc?.stored_cost_net ?? item.stored_cost_net)}
                />
                <Field
                  label="IVA (tasa)"
                  value={formatTaxRate(calc?.iva?.rate ?? null)}
                />
                <Field
                  label="IVA (monto)"
                  value={formatDecimalMoneyCLP(calc?.iva?.amount ?? item.calculated_iva_amount)}
                />
                <Field
                  label="Tasa tributaria total"
                  value={formatTaxRate(item.total_tax_rate)}
                />
                <Field
                  label="Bruto corregido"
                  value={displayCorrectedGross(
                    calc?.corrected_gross_cost ?? item.corrected_gross_cost,
                  )}
                />
                <Field
                  label="Diferencia unitaria"
                  value={displayUnitDifference({
                    stored_cost_gross: item.stored_cost_gross,
                    unit_difference: item.unit_difference,
                  })}
                />
              </dl>
              <p className="text-xs text-muted-foreground">
                Fórmula: neto + IVA + impuestos adicionales
              </p>

              {(calc?.additional_taxes?.length || item.additional_taxes?.length) ? (
                <div className="space-y-2">
                  <p className="text-xs font-medium text-muted-foreground">
                    Impuestos adicionales / anticipos
                  </p>
                  <ul className="space-y-2">
                    {(calc?.additional_taxes ?? item.additional_taxes ?? []).map((t) => (
                      <li
                        key={`${t.tax_id}-${t.category}`}
                        className="rounded-md border border-border/60 px-3 py-2 text-sm"
                      >
                        <div className="flex items-center justify-between gap-2">
                          <span className="font-medium">
                            {t.name || `Tax ${t.tax_id}`}
                          </span>
                          <Badge variant="outline" className="font-normal">
                            {additionalTaxCategoryLabel(t.category)}
                          </Badge>
                        </div>
                        <div className="mt-1 flex justify-between text-xs text-muted-foreground">
                          <span>Tasa {formatTaxRate(t.rate)}</span>
                          <span className="tabular-nums">
                            {formatDecimalMoneyCLP(t.amount)}
                          </span>
                        </div>
                      </li>
                    ))}
                  </ul>
                </div>
              ) : null}
            </section>

            <Separator />

            <section className="space-y-3">
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
                {(item.warnings ?? []).map((w) => (
                  <Badge key={w} variant="outline" className="font-normal">
                    {warningLabel(w)}
                  </Badge>
                ))}
              </div>
              <p className="text-sm text-muted-foreground">{statusShortHelp(status)}</p>
              {item.suspicious_outlier ? (
                <Alert>
                  <AlertTitle>Costo atípico</AlertTitle>
                  <AlertDescription>
                    Advertencia adicional; no invalida automáticamente el cálculo V2.
                  </AlertDescription>
                </Alert>
              ) : null}
              <dl className="grid grid-cols-2 gap-3">
                <Field
                  label="Contexto tributario disponible"
                  value={
                    item.historical_tax_context_available == null
                      ? "—"
                      : item.historical_tax_context_available
                        ? "Sí"
                        : "No"
                  }
                />
                <Field label="Origen tax IDs" value={item.tax_ids_source ?? "—"} />
                <Field label="Origen tasas" value={item.tax_rates_source ?? "—"} />
                <Field
                  label="Fuente contexto"
                  value={item.tax_context_source ?? "—"}
                />
              </dl>
            </section>

            <Accordion type="single" collapsible className="w-full">
              <AccordionItem value="trace">
                <AccordionTrigger className="text-sm">
                  Detalle técnico
                </AccordionTrigger>
                <AccordionContent>
                  <dl className="grid grid-cols-1 gap-3">
                    <Field
                      label="effective_quality_status"
                      value={<span className="font-mono text-xs">{status ?? "—"}</span>}
                    />
                    <Field
                      label="warnings"
                      value={
                        <span className="font-mono text-xs">
                          {(item.warnings ?? []).join(", ") || "—"}
                        </span>
                      }
                    />
                    <Field label="calculation_version" value={item.calculation_version} />
                    <Field
                      label="calculation_batch_id"
                      value={
                        <span className="break-all font-mono text-xs">
                          {item.calculation_batch_id ?? "—"}
                        </span>
                      }
                    />
                    <Field
                      label="calculated_at"
                      value={formatDateTimeCL(item.calculated_at)}
                    />
                    <Field
                      label="source_history_fingerprint"
                      value={
                        <span className="break-all font-mono text-[10px]">
                          {item.source_history_fingerprint ?? "—"}
                        </span>
                      }
                    />
                    <Field
                      label="tax_context_fingerprint"
                      value={
                        <span className="break-all font-mono text-[10px]">
                          {item.tax_context_fingerprint ?? "—"}
                        </span>
                      }
                    />
                    <Field
                      label="calculation_result_fingerprint"
                      value={
                        <span className="break-all font-mono text-[10px]">
                          {item.calculation_result_fingerprint ?? "—"}
                        </span>
                      }
                    />
                  </dl>
                </AccordionContent>
              </AccordionItem>
            </Accordion>
          </div>
        ) : null}
      </SheetContent>
    </Sheet>
  )
}
