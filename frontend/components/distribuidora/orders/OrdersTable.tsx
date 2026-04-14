"use client"

import { Checkbox } from "@/components/ui/checkbox"
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
import type { DistribuidoraPurchaseOrder } from "@/lib/api"

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
              <TableHead>Nombre fantasía</TableHead>
              <TableHead>Comuna</TableHead>
              <TableHead className="text-right">Total</TableHead>
              <TableHead>Vendedor</TableHead>
              <TableHead>Estado</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {items.length === 0 ? (
              <TableRow>
                <TableCell colSpan={7} className="h-24 text-center text-muted-foreground">
                  No hay órdenes para mostrar.
                </TableCell>
              </TableRow>
            ) : (
              items.map((row) => {
                const invoiced = row.is_invoiced === true
                const seller =
                  row.seller_name?.trim() ||
                  row.seller?.trim() ||
                  (row.user_id != null ? `Usuario ${row.user_id}` : "—")
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
                      {row.number ?? row.document_id}
                    </TableCell>
                    <TableCell className="max-w-[220px] truncate">
                      {row.nombre_fantasia?.trim() || "—"}
                    </TableCell>
                    <TableCell className="max-w-[160px] truncate">
                      {row.municipality?.trim() || row.city?.trim() || "—"}
                    </TableCell>
                    <TableCell className="text-right tabular-nums">
                      {formatClp(row.total_amount)}
                    </TableCell>
                    <TableCell className="max-w-[180px] truncate">{seller}</TableCell>
                    <TableCell>
                      {invoiced ? (
                        <Badge className="bg-emerald-600 hover:bg-emerald-600">
                          Facturada
                        </Badge>
                      ) : (
                        <Badge variant="destructive">No facturada</Badge>
                      )}
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
