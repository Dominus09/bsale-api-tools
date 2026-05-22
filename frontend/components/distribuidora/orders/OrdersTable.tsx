"use client"

import { Checkbox } from "@/components/ui/checkbox"
import { Button } from "@/components/ui/button"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import type { DistribuidoraPurchaseOrder } from "@/lib/api"
import {
  PurchaseAssociatedDocumentCell,
  PurchaseInvoiceScoreCell,
  PurchaseInvoiceStatusCell,
} from "@/components/distribuidora/orders/PurchaseInvoiceTableCells"

function formatClp(value: number | null | undefined): string {
  const n = Number(value)
  if (!Number.isFinite(n)) return "$0"
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  }).format(n)
}

type OrdersTableProps = {
  items: DistribuidoraPurchaseOrder[]
  pageSelectedIds: ReadonlySet<number>
  onToggle: (documentId: number, checked: boolean) => void
  onToggleAll: (checked: boolean) => void
  onAddToPlanning: () => void
  planningBasketCount: number
  loading?: boolean
}

export function OrdersTable({
  items,
  pageSelectedIds,
  onToggle,
  onToggleAll,
  onAddToPlanning,
  planningBasketCount,
  loading,
}: OrdersTableProps) {
  const allIds = items.map((r) => r.document_id)
  const allSelected =
    allIds.length > 0 && allIds.every((id) => pageSelectedIds.has(id))
  const someSelected = allIds.some((id) => pageSelectedIds.has(id))

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="text-sm text-muted-foreground">
          {pageSelectedIds.size > 0 ? (
            <span>
              {pageSelectedIds.size} OC seleccionada
              {pageSelectedIds.size === 1 ? "" : "s"} en esta vista
            </span>
          ) : (
            <span>Seleccione órdenes para enviarlas a planificación</span>
          )}
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Button
            type="button"
            variant="secondary"
            size="sm"
            disabled={loading || allIds.length === 0}
            onClick={() => onToggleAll(!allSelected)}
          >
            {allSelected ? "Desmarcar todas" : "Marcar todas"}
          </Button>
          <Button
            type="button"
            size="sm"
            disabled={loading || pageSelectedIds.size === 0}
            onClick={onAddToPlanning}
          >
            Agregar a planificación
          </Button>
          <span className="text-xs text-muted-foreground">
            En cola: {planningBasketCount}
          </span>
        </div>
      </div>
      <div className="relative max-h-[min(65vh,40rem)] overflow-auto rounded-lg border border-border/70 bg-card shadow-sm">
        <Table>
          <TableHeader className="sticky top-0 z-10 bg-card/95 shadow-[0_1px_0_0_hsl(var(--border))] backdrop-blur-sm">
            <TableRow className="hover:bg-transparent">
              <TableHead className="h-10 w-10 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                <Checkbox
                  checked={allSelected ? true : someSelected ? "indeterminate" : false}
                  onCheckedChange={(c) => onToggleAll(c === true)}
                  disabled={loading || allIds.length === 0}
                  aria-label="Seleccionar todas"
                />
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
              <TableHead className="h-10 text-right text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                Total
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
            </TableRow>
          </TableHeader>
          <TableBody>
            {items.length === 0 ? (
              <TableRow>
                <TableCell
                  colSpan={9}
                  className="h-24 text-center text-muted-foreground"
                >
                  No hay órdenes para mostrar.
                </TableCell>
              </TableRow>
            ) : (
              items.map((row) => {
                const seller =
                  row.seller_name?.trim() ||
                  row.seller?.trim() ||
                  (row.user_id != null ? `Usuario ${row.user_id}` : "—")
                const clientLabel =
                  row.oc_client_name?.trim() ||
                  row.nombre_fantasia?.trim() ||
                  "—"
                return (
                  <TableRow
                    key={row.document_id}
                    className="transition-colors duration-100 hover:bg-muted/50"
                  >
                    <TableCell className="py-3">
                      <Checkbox
                        checked={pageSelectedIds.has(row.document_id)}
                        onCheckedChange={(c) =>
                          onToggle(row.document_id, c === true)
                        }
                        disabled={loading}
                        aria-label={`Seleccionar OC ${row.number ?? row.document_id}`}
                      />
                    </TableCell>
                    <TableCell className="py-3 font-mono text-sm font-medium tabular-nums">
                      {row.number ?? row.oc_number ?? "—"}
                    </TableCell>
                    <TableCell className="max-w-[220px] truncate py-3">
                      {clientLabel}
                    </TableCell>
                    <TableCell className="max-w-[160px] truncate py-3 text-muted-foreground">
                      {row.municipality?.trim() || row.city?.trim() || "—"}
                    </TableCell>
                    <TableCell className="py-3 text-right font-medium tabular-nums">
                      {formatClp(row.total_amount)}
                    </TableCell>
                    <TableCell className="max-w-[180px] truncate py-3">
                      {seller}
                    </TableCell>
                    <TableCell className="py-3">
                      <PurchaseInvoiceStatusCell row={row} />
                    </TableCell>
                    <TableCell className="py-3">
                      <PurchaseAssociatedDocumentCell row={row} />
                    </TableCell>
                    <TableCell className="py-3 text-right">
                      <PurchaseInvoiceScoreCell row={row} />
                    </TableCell>
                  </TableRow>
                )
              })
            )}
          </TableBody>
        </Table>
      </div>
    </div>
  )
}
