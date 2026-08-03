"use client"

import { CostV2InfoHint } from "@/components/costos-v2/cost-v2-info-hint"
import { CostV2StatusBadge } from "@/components/costos-v2/cost-v2-status-badge"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import {
  displayCorrectedGross,
  displayUnitDifference,
  formatDateCL,
  formatMoneyCLPTable,
  formatTaxRate,
} from "@/lib/costos-v2/format"
import { warningLabel, warningShortHelp } from "@/lib/costos-v2/labels"
import type { CostV2ReceptionListItem } from "@/lib/costos-v2/types"
import { cn } from "@/lib/utils"

export function CostV2ReceptionsTable({
  items,
  loading,
  onOpenDetail,
}: {
  items: CostV2ReceptionListItem[]
  loading?: boolean
  onOpenDetail: (historyId: number) => void
}) {
  if (!loading && items.length === 0) {
    return (
      <p className="rounded-md border border-dashed border-border/70 px-4 py-12 text-center text-sm text-muted-foreground">
        No hay recepciones V2 para los filtros seleccionados.
      </p>
    )
  }

  return (
    <div className="overflow-x-auto rounded-md border border-border/60">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Fecha</TableHead>
            <TableHead>Documento</TableHead>
            <TableHead>Producto</TableHead>
            <TableHead className="hidden lg:table-cell">Variante</TableHead>
            <TableHead className="hidden md:table-cell">Código de barras</TableHead>
            <TableHead className="text-right">Costo neto</TableHead>
            <TableHead className="hidden xl:table-cell text-right">Bruto almacenado</TableHead>
            <TableHead className="text-right">Bruto corregido V2</TableHead>
            <TableHead className="text-right">Diferencia unitaria</TableHead>
            <TableHead className="hidden lg:table-cell text-right">Tasa total</TableHead>
            <TableHead>
              <span className="inline-flex items-center gap-1">
                Estado
                <CostV2InfoHint
                  title="Estado del costo"
                  text="Indica la calidad del costo y del cálculo tributario."
                />
              </span>
            </TableHead>
            <TableHead className="hidden md:table-cell">
              <span className="inline-flex items-center gap-1">
                Alertas
                <CostV2InfoHint
                  title="Alertas"
                  text="Advertencias adicionales; no reemplazan el estado principal."
                />
              </span>
            </TableHead>
            <TableHead className="text-right">Acción</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {items.map((row) => (
            <TableRow key={row.history_id} className={cn(loading && "opacity-60")}>
              <TableCell className="whitespace-nowrap tabular-nums">
                {formatDateCL(row.admission_date)}
              </TableCell>
              <TableCell className="max-w-[120px] truncate">
                {row.document_number ?? row.document ?? "—"}
              </TableCell>
              <TableCell className="max-w-[180px] truncate font-medium">
                {row.product_name ?? "—"}
              </TableCell>
              <TableCell className="hidden max-w-[140px] truncate lg:table-cell">
                {row.variant_name ?? "—"}
              </TableCell>
              <TableCell className="hidden font-mono text-xs md:table-cell">
                {row.barcode ?? "—"}
              </TableCell>
              <TableCell className="text-right tabular-nums">
                {formatMoneyCLPTable(row.stored_cost_net)}
              </TableCell>
              <TableCell className="hidden text-right tabular-nums xl:table-cell">
                {formatMoneyCLPTable(row.stored_cost_gross)}
              </TableCell>
              <TableCell className="text-right tabular-nums">
                {displayCorrectedGross(row.corrected_gross_cost)}
              </TableCell>
              <TableCell className="text-right tabular-nums">
                {displayUnitDifference({
                  stored_cost_gross: row.stored_cost_gross,
                  unit_difference: row.unit_difference,
                })}
              </TableCell>
              <TableCell className="hidden text-right tabular-nums lg:table-cell">
                {formatTaxRate(row.total_tax_rate)}
              </TableCell>
              <TableCell>
                <CostV2StatusBadge status={row.effective_quality_status} />
              </TableCell>
              <TableCell className="hidden md:table-cell">
                <div className="flex flex-wrap gap-1">
                  {(row.warnings ?? []).map((w) => (
                    <TooltipProvider key={`${row.history_id}-${w}`} delayDuration={150}>
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Badge variant="outline" className="font-normal text-[10px]">
                            {warningLabel(w)}
                          </Badge>
                        </TooltipTrigger>
                        <TooltipContent className="max-w-xs text-xs">
                          <p className="font-medium">{warningLabel(w)}</p>
                          <p className="mt-0.5">{warningShortHelp(w)}</p>
                        </TooltipContent>
                      </Tooltip>
                    </TooltipProvider>
                  ))}
                  {row.suspicious_outlier &&
                  !(row.warnings ?? []).includes("suspicious_outlier") ? (
                    <Badge variant="outline" className="font-normal text-[10px]">
                      {warningLabel("suspicious_outlier")}
                    </Badge>
                  ) : null}
                </div>
              </TableCell>
              <TableCell className="text-right">
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  onClick={() => onOpenDetail(row.history_id)}
                >
                  Ver detalle
                </Button>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  )
}
