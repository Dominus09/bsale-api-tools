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
  priorityLabel,
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
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { cn } from "@/lib/utils"

const TRUCK_UNSET = "__unset__"

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
          className="h-8 gap-1 text-xs"
          aria-label={`Asignar camión a pedidos con georef en ${municipalityLabel}`}
        >
          Asignar camión
          <ChevronDown className="size-3.5 opacity-70" aria-hidden />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent
        align="end"
        className="w-[min(22rem,calc(100vw-2rem))] p-2"
      >
        {noGeoCount > 0 ? (
          <Alert className="mb-2 border-amber-500/40 bg-amber-50/90 dark:bg-amber-950/40">
            <AlertTitle className="text-xs">Georreferencia</AlertTitle>
            <AlertDescription className="text-xs">
              {noGeoCount} cliente{noGeoCount !== 1 ? "s" : ""} no tienen
              coordenadas y no serán asignados
            </AlertDescription>
          </Alert>
        ) : null}
        <div className="flex flex-col gap-0.5">
          {trucksOrdered.map((t) => (
            <DropdownMenuItem
              key={t.id}
              onSelect={() => onPickTruck(t.id)}
              className={cn(
                "cursor-pointer",
                lastSuggestedTruckId === t.id &&
                  "bg-muted/70 ring-1 ring-inset ring-primary/30",
              )}
            >
              <span className="flex w-full items-center justify-between gap-2 pr-1">
                <span>
                  {t.name} ({t.plate})
                </span>
                {lastSuggestedTruckId === t.id ? (
                  <Badge variant="outline" className="text-[10px] font-normal">
                    Reciente
                  </Badge>
                ) : null}
              </span>
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
  const { flags, primary } = resolveRowPriority(row, thresholds)
  if (!primary) {
    return <span className="text-xs text-muted-foreground/50">—</span>
  }
  return (
    <div className="flex flex-col gap-0.5">
      <Badge
        variant="outline"
        className={cn("w-fit text-[10px] font-medium", priorityBadgeClass(primary))}
      >
        {priorityLabel(primary)}
      </Badge>
      {flags.length > 1 ? (
        <span className="text-[10px] text-muted-foreground">
          +{flags.length - 1}
        </span>
      ) : null}
    </div>
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

  return (
    <TableRow
      className={cn(
        "text-sm transition-colors duration-100",
        geo ? "hover:bg-muted/50" : "bg-destructive/5 hover:bg-destructive/10",
        inPlan && "border-l-2 border-l-primary bg-primary/[0.04]",
        priority.primary === "high_amount" && !inPlan && "bg-amber-50/30 dark:bg-amber-950/15",
        priority.primary === "stale_pending" && !inPlan && "bg-slate-50/50 dark:bg-slate-900/30",
      )}
    >
      <TableCell className="py-3">
        <PriorityCell row={r} thresholds={thresholds} />
      </TableCell>
      <TableCell className="py-3 font-mono text-sm font-medium tabular-nums">
        {r.oc ?? "—"}
      </TableCell>
      <TableCell className="max-w-[10rem] truncate py-3">
        {r.nombre_fantasia?.trim() || "—"}
      </TableCell>
      <TableCell className="max-w-[8rem] truncate py-3 text-muted-foreground">
        {r.municipality?.trim() || "—"}
      </TableCell>
      <TableCell className="max-w-[12rem] truncate py-3 text-muted-foreground">
        {r.direccion?.trim() || "—"}
      </TableCell>
      <TableCell className="max-w-[8rem] truncate py-3">
        {r.seller_name?.trim() || "—"}
      </TableCell>
      <TableCell className="whitespace-nowrap py-3">
        <PurchaseInvoiceStatusCell row={r} />
      </TableCell>
      <TableCell className="py-3">
        <PurchaseAssociatedDocumentCell row={r} />
      </TableCell>
      <TableCell className="py-3 text-right">
        <PurchaseInvoiceScoreCell row={r} />
      </TableCell>
      <TableCell className="py-3 text-right font-medium tabular-nums">
        {formatClp(Number(r.total_amount ?? 0))}
      </TableCell>
      <TableCell className="py-3">
        {geo ? (
          <Badge
            variant="outline"
            className="border-emerald-200 bg-emerald-50 text-emerald-800 text-[10px] dark:border-emerald-900 dark:bg-emerald-950/40 dark:text-emerald-200"
          >
            OK
          </Badge>
        ) : (
          <Tooltip>
            <TooltipTrigger asChild>
              <span className="inline-flex cursor-help">
                <Badge variant="destructive" className="text-[10px]">
                  Sin geo
                </Badge>
              </span>
            </TooltipTrigger>
            <TooltipContent side="top" className="max-w-xs">
              Este cliente no puede ser planificado
            </TooltipContent>
          </Tooltip>
        )}
      </TableCell>
      <TableCell className="max-w-[9rem] truncate py-3 text-xs text-muted-foreground">
        {clusterLabel}
      </TableCell>
      <TableCell className="py-3">
        <div className="flex min-w-[11rem] flex-col gap-1.5">
          {trucks.length === 0 ? (
            <span className="text-xs text-muted-foreground">Sin camiones</span>
          ) : (
            <select
              className="h-8 max-w-[16rem] rounded-md border border-input bg-background px-2 text-xs shadow-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50"
              value={
                tid != null && trucks.some((x) => x.id === tid)
                  ? String(tid)
                  : TRUCK_UNSET
              }
              onChange={(e) => onTruckChange(r, e.target.value)}
              disabled={!geo}
              aria-label={`Camión OC ${r.oc ?? docId}`}
            >
              <option value={TRUCK_UNSET}>Asignar</option>
              {trucks.map((t) => (
                <option key={t.id} value={t.id}>
                  {t.name} ({t.plate})
                </option>
              ))}
            </select>
          )}
          {capLabel && geo ? (
            <Badge
              variant="secondary"
              className="w-fit max-w-[16rem] truncate text-[10px] font-normal"
              title={capLabel}
            >
              {capLabel}
            </Badge>
          ) : null}
        </div>
      </TableCell>
    </TableRow>
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
    <div className="relative max-h-[min(70vh,52rem)] overflow-auto rounded-lg border border-border/70 bg-card shadow-sm">
      <Table>
        <TableHeader className="sticky top-0 z-10 bg-card/95 shadow-[0_1px_0_0_hsl(var(--border))] backdrop-blur-sm">
          <TableRow className="hover:bg-transparent">
            <TableHead className="h-10 whitespace-nowrap text-xs font-semibold uppercase tracking-wide text-muted-foreground">
              Prioridad
            </TableHead>
            <TableHead className="h-10 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
              OC
            </TableHead>
            <TableHead className="h-10 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
              Cliente
            </TableHead>
            <TableHead className="h-10 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
              Comuna
            </TableHead>
            <TableHead className="h-10 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
              Dirección
            </TableHead>
            <TableHead className="h-10 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
              Vendedor
            </TableHead>
            <TableHead className="h-10 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
              Estado
            </TableHead>
            <TableHead className="h-10 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
              Doc. asoc.
            </TableHead>
            <TableHead className="h-10 text-right text-xs font-semibold uppercase tracking-wide text-muted-foreground">
              Score
            </TableHead>
            <TableHead className="h-10 text-right text-xs font-semibold uppercase tracking-wide text-muted-foreground">
              Monto
            </TableHead>
            <TableHead className="h-10 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
              Georef
            </TableHead>
            <TableHead className="h-10 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
              Cluster
            </TableHead>
            <TableHead className="h-10 min-w-[12rem] text-xs font-semibold uppercase tracking-wide text-muted-foreground">
              Camión
            </TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {loading ? (
            <TableRow>
              <TableCell
                colSpan={13}
                className="py-16 text-center text-sm text-muted-foreground"
              >
                Cargando órdenes…
              </TableCell>
            </TableRow>
          ) : (
            blocks.map((block) => (
              <Fragment key={block.key}>
                {groupByMunicipality && block.key !== "_all" ? (
                  <TableRow className="bg-muted/60 hover:bg-muted/60">
                    <TableCell colSpan={13} className="py-2.5">
                      <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                        <div>
                          <span className="text-sm font-semibold">
                            {block.key}
                          </span>
                          <span className="ml-2 text-xs text-muted-foreground">
                            {block.rows.length} pedidos · {formatClp(block.total)}
                          </span>
                        </div>
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
                    </TableCell>
                  </TableRow>
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
        </TableBody>
      </Table>
    </div>
  )
}
