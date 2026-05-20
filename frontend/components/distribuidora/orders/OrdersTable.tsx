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
      <div className="rounded-md border">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-10">
                <Checkbox
                  checked={allSelected ? true : someSelected ? "indeterminate" : false}
                  onCheckedChange={(c) => onToggleAll(c === true)}
                  disabled={loading || allIds.length === 0}
                  aria-label="Seleccionar todas"
                />
              </TableHead>
              <TableHead>OC</TableHead>
              <TableHead>Cliente</TableHead>
              <TableHead>Comuna</TableHead>
              <TableHead className="text-right">Total</TableHead>
              <TableHead>Vendedor</TableHead>
              <TableHead>Estado</TableHead>
              <TableHead>Documento asociado</TableHead>
              <TableHead className="text-right">Score</TableHead>
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
                  <TableRow key={row.document_id}>
                    <TableCell>
                      <Checkbox
                        checked={pageSelectedIds.has(row.document_id)}
                        onCheckedChange={(c) =>
                          onToggle(row.document_id, c === true)
                        }
                        disabled={loading}
                        aria-label={`Seleccionar OC ${row.number ?? row.document_id}`}
                      />
                    </TableCell>
                    <TableCell className="tabular-nums font-medium">
                      {row.number ?? row.oc_number ?? "—"}
                    </TableCell>
                    <TableCell className="max-w-[220px] truncate">
                      {clientLabel}
                    </TableCell>
                    <TableCell className="max-w-[160px] truncate">
                      {row.municipality?.trim() || row.city?.trim() || "—"}
                    </TableCell>
                    <TableCell className="text-right tabular-nums">
                      {formatClp(row.total_amount)}
                    </TableCell>
                    <TableCell className="max-w-[180px] truncate">{seller}</TableCell>
                    <TableCell>
                      <PurchaseInvoiceStatusCell row={row} />
                    </TableCell>
                    <TableCell>
                      <PurchaseAssociatedDocumentCell row={row} />
                    </TableCell>
                    <TableCell className="text-right">
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
