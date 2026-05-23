"use client"

import { useEffect, useState } from "react"
import Link from "next/link"
import { History, Loader2 } from "lucide-react"

import { listDispatchPlansRecent, type DispatchPlanSummary } from "@/lib/api"
import { formatClp } from "@/lib/ors-map-ui"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"

const STATUS_LABEL: Record<string, string> = {
  planned: "Planificado",
  invoicing: "Facturando",
  ready_for_picking: "Listo picking",
  picking_generated: "Picking OK",
  dispatched: "Despachado",
  delivered: "Entregado",
  draft: "Borrador",
}

function invoicingLabel(p: DispatchPlanSummary): string {
  const c = p.invoiced_confirmed ?? 0
  const pr = p.invoiced_probable ?? 0
  const pe = p.invoiced_pending ?? 0
  const total = p.order_count ?? c + pr + pe
  if (total === 0) return "—"
  if (pe > 0) return `${c}✓ · ${pr}? · ${pe} pend.`
  if (pr > 0) return `${c}✓ · ${pr} probable`
  return `${c}/${total} facturadas`
}

export default function PlanificacionesHistorialPage() {
  const [items, setItems] = useState<DispatchPlanSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    ;(async () => {
      setLoading(true)
      try {
        const res = await listDispatchPlansRecent({ limit: 80 })
        if (!cancelled) setItems(res.items)
      } catch (e: unknown) {
        if (!cancelled) setError(e instanceof Error ? e.message : "Error")
      } finally {
        if (!cancelled) setLoading(false)
      }
    })()
    return () => {
      cancelled = true
    }
  }, [])

  return (
    <div className="space-y-6 p-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <p className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
            Logística 1.2
          </p>
          <h1 className="text-xl font-semibold tracking-tight">Planificaciones recientes</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Historial persistente por camión — reabrir, auditar facturación y costos.
          </p>
        </div>
        <Button asChild variant="outline" size="sm">
          <Link href="/distribuidora/planificacion">Nueva planificación ORS</Link>
        </Button>
      </div>

      {error ? (
        <p className="text-sm text-destructive">{error}</p>
      ) : loading ? (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Loader2 className="size-4 animate-spin" aria-hidden />
          Cargando…
        </div>
      ) : items.length === 0 ? (
        <p className="text-sm text-muted-foreground">No hay planificaciones confirmadas aún.</p>
      ) : (
        <div className="overflow-hidden rounded-lg border border-border/80">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Código</TableHead>
                <TableHead>Nombre</TableHead>
                <TableHead>Camión</TableHead>
                <TableHead>Fecha</TableHead>
                <TableHead>Estado</TableHead>
                <TableHead className="text-right">OCs</TableHead>
                <TableHead className="text-right">Monto OCs</TableHead>
                <TableHead>Facturación</TableHead>
                <TableHead />
              </TableRow>
            </TableHeader>
            <TableBody>
              {items.map((p) => (
                <TableRow key={p.id}>
                  <TableCell className="font-mono text-xs">{p.planning_code ?? `PLAN-${p.id}`}</TableCell>
                  <TableCell className="font-medium">{p.planning_name ?? p.route_name}</TableCell>
                  <TableCell>{p.truck_name ?? "—"}</TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {p.planning_date?.slice(0, 10) ?? p.created_at?.slice(0, 10) ?? "—"}
                  </TableCell>
                  <TableCell>
                    <Badge variant="secondary" className="text-[10px]">
                      {STATUS_LABEL[p.status] ?? p.status}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-right tabular-nums">{p.order_count ?? 0}</TableCell>
                  <TableCell className="text-right tabular-nums">
                    {formatClp(Number(p.total_oc_amount) || 0)}
                  </TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {invoicingLabel(p)}
                  </TableCell>
                  <TableCell className="text-right">
                    <Button asChild variant="ghost" size="sm" className="h-8 gap-1 text-xs">
                      <Link href={`/distribuidora/planificaciones/${p.id}`}>
                        <History className="size-3.5" aria-hidden />
                        Abrir
                      </Link>
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  )
}
