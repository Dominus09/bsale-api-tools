"use client"

import { Fragment } from "react"
import { ChevronDown } from "lucide-react"

import type {
  DistribuidoraDispatchPrepPlanningRow,
  DistribuidoraTruck,
} from "@/lib/api"
import { distribuidoraTruckCapacityLabel } from "@/lib/api"
import {
  computeAmountThresholds,
  priorityBadgeClass,
  priorityShortLabel,
  resolveRowPriority,
  type PreDespachoAmountThresholds,
} from "@/lib/pre-despacho-priority"
import {
  PurchaseAssociatedDocumentCell,
  PurchaseInvoiceScoreCell,
  PurchaseInvoiceStatusCell,
} from "@/components/distribuidora/orders/PurchaseInvoiceTableCells"
import { PreDespachoEmptyState } from "@/components/distribuidora/orders/PreDespachoEmptyState"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { cn } from "@/lib/utils"

const TRUCK_UNSET = "__unset__"

const TH =
  "h-8 px-1.5 text-left align-middle text-[10px] font-semibold uppercase tracking-wide text-muted-foreground"
const TD = "px-1.5 py-1.5 align-middle text-xs"

const clp = new Intl.NumberFormat("es-CL", {
  style: "currency",
  currency: "CLP",
  maximumFractionDigits: 0,
})

function formatClp(n: number): string {
  return clp.format(Number.isFinite(n) ? n : 0)
}

function rowHasGeo(r: DistribuidoraDispatchPrepPlanningRow): boolean {
  return Boolean(r.has_georef && r.lat != null && r.lng != null)
}

function isValidTruckId(
  tid: number | null | undefined,
  trucks: DistribuidoraTruck[],
): tid is number {
  return (
    tid != null &&
    Number.isFinite(tid) &&
    tid > 0 &&
    trucks.some((t) => t.id === tid)
  )
}

function TruncateTooltip({
  text,
  className,
}: {
  text: string
  className?: string
}) {
  const display = text?.trim() || "—"
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className={cn("block min-w-0 truncate", className)}>{display}</span>
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-sm text-xs">
        {display}
      </TooltipContent>
    </Tooltip>
  )
}

function GroupTruckAssignMenu({
  municipalityLabel,
  groupRows,
  trucksOrdered,
  lastSuggestedTruckId,
  disabled,
  onPickTruck,
}: {
  municipalityLabel: string
  groupRows: DistribuidoraDispatchPrepPlanningRow[]
  trucksOrdered: DistribuidoraTruck[]
  lastSuggestedTruckId: number | null
  disabled: boolean
  onPickTruck: (truckId: number) => void
}) {
  const noGeoCount = groupRows.filter((r) => !rowHasGeo(r)).length
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button
          type="button"
          size="sm"
          variant="outline"
          disabled={disabled}
          className="h-7 gap-0.5 px-2 text-[10px]"
          aria-label={`Asignar camión en ${municipalityLabel}`}
        >
          Asignar
          <ChevronDown className="size-3 opacity-70" aria-hidden />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent
        align="end"
        className="w-[min(20rem,calc(100vw-2rem))] p-2"
      >
        {noGeoCount > 0 ? (
          <Alert className="mb-2 border-amber-500/40 bg-amber-50/90 dark:bg-amber-950/40">
            <AlertTitle className="text-xs">Georreferencia</AlertTitle>
            <AlertDescription className="text-xs">
              {noGeoCount} sin coordenadas (omitidos)
            </AlertDescription>
          </Alert>
        ) : null}
        <div className="flex flex-col gap-0.5">
          {trucksOrdered.map((t) => (
            <DropdownMenuItem
              key={t.id}
              onSelect={() => onPickTruck(t.id)}
              className={cn(
                "cursor-pointer text-xs",
                lastSuggestedTruckId === t.id &&
                  "bg-muted/70 ring-1 ring-inset ring-primary/30",
              )}
            >
              {t.name} ({t.plate})
            </DropdownMenuItem>
          ))}
        </div>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

function PriorityCell({
  row,
  thresholds,
}: {
  row: DistribuidoraDispatchPrepPlanningRow
  thresholds: PreDespachoAmountThresholds
}) {
  const { primary } = resolveRowPriority(row, thresholds)
  if (!primary) {
    return <span className="text-[10px] text-muted-foreground/40">—</span>
  }
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Badge
          variant="outline"
          className={cn(
            "max-w-full truncate px-1 py-0 text-[9px] font-medium leading-4",
            priorityBadgeClass(primary),
          )}
        >
          {priorityShortLabel(primary)}
        </Badge>
      </TooltipTrigger>
      <TooltipContent side="top" className="text-xs">
        {priorityShortLabel(primary)}
      </TooltipContent>
    </Tooltip>
  )
}

function PlanningTableRow({
  r,
  trucks,
  truckIdByDoc,
  clusterLabel,
  thresholds,
  onTruckChange,
}: {
  r: DistribuidoraDispatchPrepPlanningRow
  trucks: DistribuidoraTruck[]
  truckIdByDoc: Record<number, number | null>
  clusterLabel: string
  thresholds: PreDespachoAmountThresholds
  onTruckChange: (row: DistribuidoraDispatchPrepPlanningRow, raw: string) => void
}) {
  const geo = rowHasGeo(r)
  const docId = r.document_id
  const tid = truckIdByDoc[docId]
  const truck = tid != null ? trucks.find((t) => t.id === tid) : undefined
  const capLabel = truck ? distribuidoraTruckCapacityLabel(truck) : null
  const inPlan = geo && isValidTruckId(tid, trucks)
  const priority = resolveRowPriority(r, thresholds)
  const dir = r.direccion?.trim() || "—"
  const seller = r.seller_name?.trim() || "—"
  const clusterShort =
    clusterLabel.length > 12 ? `${clusterLabel.slice(0, 11)}…` : clusterLabel

  return (
    <tr
      className={cn(
        "border-b border-border/50 transition-colors duration-75",
        geo ? "hover:bg-muted/40" : "bg-destructive/5 hover:bg-destructive/10",
        inPlan && "border-l-2 border-l-primary bg-primary/[0.04]",
        priority.primary === "high_amount" &&
          !inPlan &&
          "bg-amber-50/25 dark:bg-amber-950/10",
        priority.primary === "stale_pending" &&
          !inPlan &&
          "bg-slate-50/40 dark:bg-slate-900/25",
      )}
    >
      <td className={TD}>
        <PriorityCell row={r} thresholds={thresholds} />
      </td>
      <td className={cn(TD, "font-mono text-[11px] font-semibold tabular-nums")}>
        {r.oc ?? "—"}
      </td>
      <td className={cn(TD, "max-w-0")}>
        <TruncateTooltip
          text={r.nombre_fantasia?.trim() || "—"}
          className="font-medium text-foreground"
        />
      </td>
      <td className={cn(TD, "max-w-0 text-muted-foreground")}>
        <TruncateTooltip text={r.municipality?.trim() || "—"} />
      </td>
      <td className={cn(TD, "max-w-0 text-muted-foreground")}>
        <TruncateTooltip text={dir} />
      </td>
      <td className={cn(TD, "max-w-0")}>
        <TruncateTooltip text={seller} />
      </td>
      <td className={cn(TD, "whitespace-nowrap")}>
        <PurchaseInvoiceStatusCell row={r} compact />
      </td>
      <td className={cn(TD, "max-w-0 whitespace-nowrap")}>
        <PurchaseAssociatedDocumentCell row={r} compact />
      </td>
      <td className={cn(TD, "text-right")}>
        <PurchaseInvoiceScoreCell row={r} compact />
      </td>
      <td className={cn(TD, "text-right text-[11px] font-medium tabular-nums")}>
        {formatClp(Number(r.total_amount ?? 0))}
      </td>
      <td className={cn(TD, "text-center")}>
        {geo ? (
          <Badge
            variant="outline"
            className="border-emerald-200 bg-emerald-50 px-1 py-0 text-[9px] text-emerald-800 dark:border-emerald-900 dark:bg-emerald-950/40 dark:text-emerald-200"
          >
            OK
          </Badge>
        ) : (
          <Tooltip>
            <TooltipTrigger asChild>
              <Badge
                variant="destructive"
                className="cursor-help px-1 py-0 text-[9px]"
              >
                No
              </Badge>
            </TooltipTrigger>
            <TooltipContent side="top" className="text-xs">
              Sin georreferencia
            </TooltipContent>
          </Tooltip>
        )}
      </td>
      <td className={cn(TD, "max-w-0 text-[10px] text-muted-foreground")}>
        <Tooltip>
          <TooltipTrigger asChild>
            <span className="block truncate">{clusterShort}</span>
          </TooltipTrigger>
          <TooltipContent side="top" className="text-xs">
            {clusterLabel}
          </TooltipContent>
        </Tooltip>
      </td>
      <td className={cn(TD, "max-w-0")}>
        <div className="flex min-w-0 flex-col gap-0.5">
          {trucks.length === 0 ? (
            <span className="text-[10px] text-muted-foreground">—</span>
          ) : (
            <select
              className="h-7 w-full min-w-0 max-w-full truncate rounded border border-input bg-background px-1 text-[10px] shadow-sm focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:opacity-50"
              value={
                tid != null && trucks.some((x) => x.id === tid)
                  ? String(tid)
                  : TRUCK_UNSET
              }
              onChange={(e) => onTruckChange(r, e.target.value)}
              disabled={!geo}
              aria-label={`Camión OC ${r.oc ?? docId}`}
              title={capLabel ?? undefined}
            >
              <option value={TRUCK_UNSET}>—</option>
              {trucks.map((t) => (
                <option key={t.id} value={t.id}>
                  {t.name}
                </option>
              ))}
            </select>
          )}
        </div>
      </td>
    </tr>
  )
}

export type PlanningTableBlock = {
  key: string
  rows: DistribuidoraDispatchPrepPlanningRow[]
  total: number
}

type PreDespachoPlanningTableProps = {
  blocks: PlanningTableBlock[]
  groupByMunicipality: boolean
  trucks: DistribuidoraTruck[]
  trucksOrderedForGroupMenu: DistribuidoraTruck[]
  lastSuggestedGroupTruckId: number | null
  truckIdByDoc: Record<number, number | null>
  clusterByDoc: Map<number, string>
  allRowsForThresholds: DistribuidoraDispatchPrepPlanningRow[]
  loading?: boolean
  statusFilterActive?: boolean
  onGroupTruckPick: (
    municipalityLabel: string,
    truckId: number,
    groupRows: DistribuidoraDispatchPrepPlanningRow[],
  ) => void
  onTruckChange: (row: DistribuidoraDispatchPrepPlanningRow, raw: string) => void
}

export function PreDespachoPlanningTable({
  blocks,
  groupByMunicipality,
  trucks,
  trucksOrderedForGroupMenu,
  lastSuggestedGroupTruckId,
  truckIdByDoc,
  clusterByDoc,
  allRowsForThresholds,
  loading,
  statusFilterActive,
  onGroupTruckPick,
  onTruckChange,
}: PreDespachoPlanningTableProps) {
  const thresholds = computeAmountThresholds(allRowsForThresholds)
  const rowCount = blocks.reduce((n, b) => n + b.rows.length, 0)

  if (!loading && rowCount === 0) {
    return (
      <PreDespachoEmptyState
        variant={statusFilterActive ? "filtered-out" : "no-data"}
      />
    )
  }

  return (
    <div className="relative w-full max-h-[min(72vh,56rem)] overflow-y-auto overflow-x-auto rounded-md border border-border/70 bg-card shadow-sm sm:overflow-x-hidden">
      <table className="w-full min-w-[960px] table-fixed border-collapse sm:min-w-0">
        <colgroup>
          <col className="w-[4%]" />
          <col className="w-[5%]" />
          <col className="w-[11%]" />
          <col className="w-[7%]" />
          <col className="w-[13%]" />
          <col className="w-[8%]" />
          <col className="w-[7%]" />
          <col className="w-[8%]" />
          <col className="w-[4%]" />
          <col className="w-[8%]" />
          <col className="w-[4%]" />
          <col className="w-[6%]" />
          <col className="w-[15%]" />
        </colgroup>
        <thead className="sticky top-0 z-10 bg-card/98 shadow-[0_1px_0_0_hsl(var(--border))] backdrop-blur-sm">
          <tr className="border-b border-border/80 hover:bg-transparent">
            <th className={TH}>Prio</th>
            <th className={TH}>OC</th>
            <th className={TH}>Cliente</th>
            <th className={TH}>Comuna</th>
            <th className={TH}>Dirección</th>
            <th className={TH}>Vendedor</th>
            <th className={TH}>Estado</th>
            <th className={TH}>Doc.</th>
            <th className={cn(TH, "text-right")}>Sc</th>
            <th className={cn(TH, "text-right")}>Monto</th>
            <th className={cn(TH, "text-center")}>Geo</th>
            <th className={TH}>Clust.</th>
            <th className={TH}>Camión</th>
          </tr>
        </thead>
        <tbody>
          {loading ? (
            <tr>
              <td
                colSpan={13}
                className="px-2 py-12 text-center text-xs text-muted-foreground"
              >
                Cargando órdenes…
              </td>
            </tr>
          ) : (
            blocks.map((block) => (
              <Fragment key={block.key}>
                {groupByMunicipality && block.key !== "_all" ? (
                  <tr className="bg-muted/50 hover:bg-muted/50">
                    <td colSpan={13} className="px-2 py-1.5">
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <span className="text-xs font-semibold">
                          {block.key}
                          <span className="ml-2 font-normal text-muted-foreground">
                            {block.rows.length} ped. · {formatClp(block.total)}
                          </span>
                        </span>
                        <GroupTruckAssignMenu
                          municipalityLabel={block.key}
                          groupRows={block.rows}
                          trucksOrdered={trucksOrderedForGroupMenu}
                          lastSuggestedTruckId={lastSuggestedGroupTruckId}
                          disabled={trucks.length === 0}
                          onPickTruck={(truckId) =>
                            onGroupTruckPick(block.key, truckId, block.rows)
                          }
                        />
                      </div>
                    </td>
                  </tr>
                ) : null}
                {block.rows.map((r) => (
                  <PlanningTableRow
                    key={r.document_id}
                    r={r}
                    trucks={trucks}
                    truckIdByDoc={truckIdByDoc}
                    clusterLabel={clusterByDoc.get(r.document_id) ?? "—"}
                    thresholds={thresholds}
                    onTruckChange={onTruckChange}
                  />
                ))}
              </Fragment>
            ))
          )}
        </tbody>
      </table>
    </div>
  )
}
