"use client"

import { ArrowDownRight, ArrowUpRight, Check, Copy } from "lucide-react"
import { useEffect, useMemo, useState, type ReactNode } from "react"

import { CostV2StatusBadge } from "@/components/costos-v2/cost-v2-status-badge"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Sheet,
  SheetContent,
  SheetDescription,
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
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { CostV2ApiError, getCostV2ProductDetail } from "@/lib/costos-v2/api"
import {
  changeDirection,
  displayAdditionalTaxTitle,
  displayCorrectedGross,
  formatDateCL,
  formatMoneyCLPPrecise,
  formatMoneyCLPTable,
  formatPercentCL,
  formatTaxRate,
} from "@/lib/costos-v2/format"
import {
  COST_V2_SCOPE_NOTE_DRAWER,
  statusDrawerDescription,
  statusSuggestedAction,
  warningLabel,
} from "@/lib/costos-v2/labels"
import type { CostV2ProductItem, CostV2ReceptionListItem } from "@/lib/costos-v2/types"
import { cn } from "@/lib/utils"

const HISTORY_PREVIEW = 5

function MetricCell({
  label,
  value,
  emphasize,
}: {
  label: string
  value: ReactNode
  emphasize?: boolean
}) {
  return (
    <div
      className={cn(
        "rounded-md border border-border/60 px-3 py-2.5",
        emphasize && "border-foreground/20 bg-muted/40",
      )}
    >
      <p className="text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
        {label}
      </p>
      <p
        className={cn(
          "mt-1 tabular-nums text-foreground",
          emphasize ? "text-lg font-semibold" : "text-sm font-medium",
        )}
      >
        {value}
      </p>
    </div>
  )
}

function CostVariationBlock({
  amount,
  percent,
}: {
  amount: string | null | undefined
  percent: string | null | undefined
}) {
  const dir = changeDirection(amount)
  if (dir === "flat" || dir === "none") {
    return <p className="text-sm text-muted-foreground">Sin variación</p>
  }
  const Icon = dir === "up" ? ArrowUpRight : ArrowDownRight
  return (
    <p
      className={cn(
        "inline-flex items-center gap-1.5 text-sm font-medium tabular-nums",
        dir === "up" && "text-emerald-700 dark:text-emerald-400",
        dir === "down" && "text-red-700 dark:text-red-400",
      )}
    >
      <Icon className="h-4 w-4 shrink-0" aria-hidden />
      <span>
        {formatMoneyCLPTable(amount)}
        <span className="mx-1 text-muted-foreground">·</span>
        {dir === "up" ? "+" : ""}
        {formatPercentCL(percent)}
      </span>
    </p>
  )
}

function HistoryTable({
  rows,
  highlightFirst,
}: {
  rows: CostV2ReceptionListItem[]
  highlightFirst?: boolean
}) {
  if (!rows.length) {
    return (
      <p className="py-6 text-center text-sm text-muted-foreground">
        Sin recepciones en el rango.
      </p>
    )
  }
  return (
    <div className="overflow-x-auto rounded-md border border-border/70">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="whitespace-nowrap">Fecha</TableHead>
            <TableHead className="whitespace-nowrap">Documento</TableHead>
            <TableHead className="whitespace-nowrap text-right">Neto</TableHead>
            <TableHead className="whitespace-nowrap text-right">Bruto V2</TableHead>
            <TableHead className="min-w-[9.5rem] whitespace-nowrap">Estado</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((r, idx) => (
            <TableRow
              key={r.history_id}
              className={cn(highlightFirst && idx === 0 && "bg-muted/40")}
            >
              <TableCell className="whitespace-nowrap text-sm">
                {formatDateCL(r.admission_date)}
              </TableCell>
              <TableCell className="whitespace-nowrap text-sm">
                {r.document_number ?? "—"}
              </TableCell>
              <TableCell className="whitespace-nowrap text-right text-sm tabular-nums">
                {formatMoneyCLPTable(r.stored_cost_net)}
              </TableCell>
              <TableCell className="whitespace-nowrap text-right text-sm tabular-nums font-medium">
                {displayCorrectedGross(r.corrected_gross_cost)}
              </TableCell>
              <TableCell className="min-w-[9.5rem]">
                <CostV2StatusBadge
                  status={r.effective_quality_status}
                  showHelp={false}
                />
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  )
}

function TechCopyRow({ label, value }: { label: string; value: string }) {
  const [copied, setCopied] = useState(false)
  const display = value || "—"
  return (
    <div className="flex items-start justify-between gap-2 rounded-md border border-border/60 px-2.5 py-2">
      <div className="min-w-0 space-y-0.5">
        <p className="text-[11px] uppercase tracking-wide text-muted-foreground">
          {label}
        </p>
        <p className="break-all font-mono text-xs text-foreground">{display}</p>
      </div>
      {value ? (
        <Button
          type="button"
          variant="ghost"
          size="icon"
          className="h-7 w-7 shrink-0"
          title="Copiar"
          onClick={async () => {
            try {
              await navigator.clipboard.writeText(value)
              setCopied(true)
              window.setTimeout(() => setCopied(false), 1500)
            } catch {
              /* ignore */
            }
          }}
        >
          {copied ? (
            <Check className="h-3.5 w-3.5 text-emerald-600" />
          ) : (
            <Copy className="h-3.5 w-3.5" />
          )}
        </Button>
      ) : null}
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
  /** Solo para preview/tests locales — omite el fetch. */
  previewItem,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  variantId: number | null
  companyId: number
  officeId: number
  dateFrom: string
  dateTo: string
  onOpenSymbology?: () => void
  previewItem?: CostV2ProductItem | null
}) {
  const [item, setItem] = useState<CostV2ProductItem | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [tab, setTab] = useState("resumen")

  useEffect(() => {
    if (!open) return
    setTab("resumen")
  }, [open, variantId])

  useEffect(() => {
    if (!open) return
    if (previewItem) {
      setItem(previewItem)
      setLoading(false)
      setError(null)
      return
    }
    if (variantId == null || !companyId || !officeId) return
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
  }, [open, variantId, companyId, officeId, dateFrom, dateTo, previewItem])

  const status = item?.current_quality_status
  const calc = item?.calculation
  const receptions = item?.receptions ?? []
  const previewRows = useMemo(
    () => receptions.slice(0, HISTORY_PREVIEW),
    [receptions],
  )
  const hasMoreHistory = receptions.length > HISTORY_PREVIEW
  const changeDir = changeDirection(item?.unit_change_amount)
  const additionalTaxes =
    item?.current_additional_taxes ?? calc?.additional_taxes ?? []

  const ivaAmount = calc?.iva?.amount ?? item?.current_calculated_iva_amount
  const storedGross =
    item?.current_stored_gross_cost ??
    (receptions[0]?.stored_cost_gross ?? null)

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent
        className={cn(
          "gap-0 overflow-hidden p-0",
          "w-full max-w-full",
          "sm:w-[min(100%,820px)] sm:max-w-[min(820px,55vw)]",
        )}
      >
        {loading ? (
          <div className="space-y-3 p-5">
            <SheetHeader className="sr-only">
              <SheetTitle>Cargando detalle de producto</SheetTitle>
            </SheetHeader>
            <Skeleton className="h-7 w-2/3" />
            <Skeleton className="h-4 w-1/2" />
            <Skeleton className="h-28 w-full" />
            <Skeleton className="h-40 w-full" />
          </div>
        ) : null}

        {error ? (
          <div className="p-5">
            <SheetHeader className="sr-only">
              <SheetTitle>Error al cargar producto</SheetTitle>
            </SheetHeader>
            <Alert variant="destructive">
              <AlertTitle>No se pudo cargar</AlertTitle>
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          </div>
        ) : null}

        {item && !loading ? (
          <div className="flex h-full min-h-0 flex-col">
            {/* Header sticky */}
            <SheetHeader className="sticky top-0 z-10 shrink-0 space-y-3 border-b border-border/70 bg-background px-5 py-4 pr-12 text-left">
              <div className="space-y-1">
                <SheetTitle className="text-xl font-semibold leading-tight tracking-tight">
                  {item.product_name || "Detalle de producto"}
                </SheetTitle>
                <SheetDescription className="text-sm text-muted-foreground">
                  {[item.variant_name, item.barcode].filter(Boolean).join(" · ") ||
                    "Sin variante / código"}
                </SheetDescription>
              </div>

              <div className="flex flex-wrap items-center gap-2">
                <CostV2StatusBadge status={status} showHelp={false} />
                {(item.current_warnings ?? []).map((w) => (
                  <Badge key={w} variant="outline" className="font-normal">
                    {warningLabel(w)}
                  </Badge>
                ))}
              </div>

              <div className="grid gap-1 text-sm text-muted-foreground sm:grid-cols-2">
                <p>
                  <span className="text-foreground/80">Última recepción:</span>{" "}
                  {formatDateCL(item.latest_admission_date)}
                </p>
                <p>
                  <span className="text-foreground/80">Documento:</span>{" "}
                  {item.latest_document_number ?? "—"}
                </p>
              </div>
            </SheetHeader>

            {/* Body scroll */}
            <div className="min-h-0 flex-1 overflow-y-auto px-5 py-4">
              <Tabs value={tab} onValueChange={setTab} className="gap-4">
                <TabsList className="w-full justify-start sm:w-auto">
                  <TabsTrigger value="resumen">Resumen</TabsTrigger>
                  <TabsTrigger value="historial">Historial</TabsTrigger>
                  <TabsTrigger value="tecnico">Detalle técnico</TabsTrigger>
                </TabsList>

                <TabsContent value="resumen" className="mt-0 space-y-4">
                  {/* Costo vigente — elemento principal */}
                  <section className="rounded-lg border border-border/80 bg-gradient-to-br from-muted/50 to-background px-4 py-4">
                    <p className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
                      Costo vigente V2
                    </p>
                    <p className="mt-1 text-3xl font-semibold tracking-tight tabular-nums sm:text-4xl">
                      {displayCorrectedGross(item.current_corrected_gross_cost)}
                    </p>
                    <div className="mt-3 flex flex-wrap items-baseline gap-x-4 gap-y-1">
                      <p className="text-sm text-muted-foreground">
                        Costo anterior:{" "}
                        <span className="tabular-nums text-foreground/90">
                          {formatMoneyCLPTable(item.previous_corrected_gross_cost)}
                        </span>
                      </p>
                      <CostVariationBlock
                        amount={item.unit_change_amount}
                        percent={item.unit_change_percent}
                      />
                    </div>
                  </section>

                  {/* Estado del costo */}
                  <section className="space-y-2 rounded-md border border-border/70 px-3 py-3">
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <h3 className="text-sm font-semibold">Estado del costo</h3>
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
                    <CostV2StatusBadge status={status} showHelp={false} />
                    <p className="text-sm leading-relaxed text-muted-foreground">
                      {statusDrawerDescription(status)}
                    </p>
                    <p className="text-xs text-muted-foreground">
                      <span className="font-medium text-foreground/80">
                        Acción sugerida:
                      </span>{" "}
                      {statusSuggestedAction(status)}
                    </p>
                  </section>

                  {/* Desglose */}
                  <section className="space-y-2">
                    <h3 className="text-sm font-semibold">Desglose del costo</h3>
                    <div className="grid grid-cols-2 gap-2">
                      <MetricCell
                        label="Costo neto"
                        value={formatMoneyCLPTable(item.current_stored_cost_net)}
                      />
                      <MetricCell
                        label="IVA"
                        value={formatMoneyCLPTable(ivaAmount)}
                      />
                      <MetricCell
                        label="Impuestos adicionales"
                        value={formatMoneyCLPTable(
                          item.current_additional_tax_amount_total,
                        )}
                      />
                      <MetricCell
                        label="Tasa total"
                        value={formatTaxRate(item.current_total_tax_rate)}
                      />
                      <MetricCell
                        label="Bruto almacenado"
                        value={formatMoneyCLPTable(storedGross)}
                      />
                      <MetricCell
                        label="Bruto corregido V2"
                        value={displayCorrectedGross(
                          item.current_corrected_gross_cost,
                        )}
                        emphasize
                      />
                    </div>
                  </section>

                  {/* Impuestos adicionales */}
                  {additionalTaxes.length > 0 ? (
                    <section className="space-y-2">
                      <h3 className="text-sm font-semibold">
                        Impuestos adicionales
                      </h3>
                      <ul className="space-y-1.5 rounded-md border border-border/70 p-2">
                        {additionalTaxes.map((t) => (
                          <li
                            key={`${t.tax_id}-${t.category}-${t.rate}`}
                            className="flex items-center justify-between gap-3 rounded-md bg-muted/30 px-3 py-2.5 text-sm"
                          >
                            <span className="min-w-0 leading-snug">
                              {displayAdditionalTaxTitle(t)}
                            </span>
                            <span className="shrink-0 font-medium tabular-nums">
                              {formatMoneyCLPTable(t.amount)}
                            </span>
                          </li>
                        ))}
                      </ul>
                    </section>
                  ) : null}

                  {/* Comparación */}
                  <section className="space-y-2">
                    <h3 className="text-sm font-semibold">Comparación</h3>
                    {changeDir === "flat" || changeDir === "none" ? (
                      <p className="rounded-md border border-border/60 px-3 py-3 text-sm text-muted-foreground">
                        Sin variación respecto al costo anterior.
                      </p>
                    ) : null}
                    <div className="grid grid-cols-2 gap-2">
                      <MetricCell
                        label="Costo anterior"
                        value={formatMoneyCLPTable(
                          item.previous_corrected_gross_cost,
                        )}
                      />
                      <MetricCell
                        label="Costo actual"
                        value={displayCorrectedGross(
                          item.current_corrected_gross_cost,
                        )}
                      />
                      <MetricCell
                        label="Diferencia unitaria"
                        value={
                          changeDir === "flat" || changeDir === "none"
                            ? "Sin variación"
                            : formatMoneyCLPTable(item.unit_change_amount)
                        }
                      />
                      <MetricCell
                        label="Variación porcentual"
                        value={
                          changeDir === "flat" || changeDir === "none"
                            ? "Sin variación"
                            : formatPercentCL(item.unit_change_percent)
                        }
                      />
                    </div>
                  </section>

                  {/* Inicio historial */}
                  <section className="space-y-2">
                    <div className="flex items-center justify-between gap-2">
                      <h3 className="text-sm font-semibold">Historial reciente</h3>
                      {hasMoreHistory ? (
                        <Button
                          type="button"
                          variant="link"
                          size="sm"
                          className="h-auto px-0 text-xs"
                          onClick={() => setTab("historial")}
                        >
                          Ver historial completo
                        </Button>
                      ) : null}
                    </div>
                    <HistoryTable rows={previewRows} highlightFirst />
                  </section>
                </TabsContent>

                <TabsContent value="historial" className="mt-0 space-y-2">
                  <h3 className="text-sm font-semibold">
                    Historial de recepciones
                  </h3>
                  <p className="text-xs text-muted-foreground">
                    {receptions.length} recepción
                    {receptions.length === 1 ? "" : "es"} en el rango.
                  </p>
                  <HistoryTable rows={receptions} highlightFirst />
                </TabsContent>

                <TabsContent value="tecnico" className="mt-0 space-y-3">
                  <h3 className="text-sm font-semibold">Detalle técnico</h3>
                  <p className="text-xs text-muted-foreground">
                    Valores originales del cálculo. Use copiar si necesita
                    trazabilidad.
                  </p>
                  <div className="space-y-2">
                    <TechCopyRow
                      label="current_quality_status"
                      value={status ?? ""}
                    />
                    <TechCopyRow
                      label="warnings"
                      value={(item.current_warnings ?? []).join(", ")}
                    />
                    <TechCopyRow
                      label="tax_ids_source"
                      value={item.tax_ids_source ?? ""}
                    />
                    <TechCopyRow
                      label="tax_rates_source"
                      value={item.tax_rates_source ?? ""}
                    />
                    <TechCopyRow
                      label="tax_context_source"
                      value={item.tax_context_source ?? ""}
                    />
                    <TechCopyRow
                      label="calculation_version"
                      value={item.calculation_version ?? ""}
                    />
                    <TechCopyRow
                      label="calculation_batch_id"
                      value={receptions[0]?.calculation_batch_id ?? ""}
                    />
                    <TechCopyRow
                      label="calculated_at"
                      value={
                        item.last_calculated_at ??
                        receptions[0]?.calculated_at ??
                        ""
                      }
                    />
                    <TechCopyRow
                      label="latest_history_id"
                      value={
                        item.latest_history_id != null
                          ? String(item.latest_history_id)
                          : ""
                      }
                    />
                    <TechCopyRow
                      label="source_history_fingerprint"
                      value={item.source_history_fingerprint ?? ""}
                    />
                    <TechCopyRow
                      label="tax_context_fingerprint"
                      value={item.tax_context_fingerprint ?? ""}
                    />
                    <TechCopyRow
                      label="calculation_result_fingerprint"
                      value={item.calculation_result_fingerprint ?? ""}
                    />
                    <TechCopyRow
                      label="current_corrected_gross_cost (preciso)"
                      value={
                        item.current_corrected_gross_cost
                          ? formatMoneyCLPPrecise(
                              item.current_corrected_gross_cost,
                            )
                          : ""
                      }
                    />
                    <TechCopyRow
                      label="formula"
                      value={calc?.formula ?? ""}
                    />
                  </div>
                </TabsContent>
              </Tabs>
            </div>

            {/* Footer nota alcance */}
            <div className="shrink-0 border-t border-border/60 bg-background px-5 py-3">
              <p className="text-[11px] leading-relaxed text-muted-foreground">
                {COST_V2_SCOPE_NOTE_DRAWER}
              </p>
            </div>
          </div>
        ) : null}
      </SheetContent>
    </Sheet>
  )
}
