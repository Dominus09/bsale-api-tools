"use client"

import { useEffect, useState } from "react"
import { AlertTriangle, Loader2, Package } from "lucide-react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { getOrders, type OrderRow } from "@/services/orders"

function formatOrderId(id: number) {
  return `#${String(id).padStart(4, "0")}`
}

function formatDate(iso: string) {
  if (!iso) return "—"
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return "—"
  return d.toLocaleDateString("es-CL")
}

function formatTotal(total: number) {
  return `$${total.toLocaleString("es-CL")}`
}

const statusStyles: Record<string, string> = {
  pendiente: "bg-yellow-500/15 text-yellow-800 dark:text-yellow-200 border-yellow-500/40",
  generado: "bg-green-500/15 text-green-800 dark:text-green-200 border-green-500/40",
  anulado: "bg-red-500/15 text-red-800 dark:text-red-200 border-red-500/40",
  revisar: "bg-blue-500/15 text-blue-800 dark:text-blue-200 border-blue-500/40",
}

function StatusBadge({ status }: { status: string | null }) {
  const key = (status ?? "").toLowerCase().trim()
  const cls = statusStyles[key] ?? "bg-muted text-muted-foreground border-border"
  return (
    <Badge variant="outline" className={cls}>
      {status?.trim() || "—"}
    </Badge>
  )
}

export default function OrdersPage() {
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [orders, setOrders] = useState<OrderRow[]>([])

  useEffect(() => {
    let cancelled = false
    async function load() {
      setLoading(true)
      setError(null)
      try {
        const data = await getOrders()
        if (!cancelled) setOrders(Array.isArray(data) ? data : [])
      } catch {
        if (!cancelled) setError("Error cargando pedidos")
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    load()
    return () => {
      cancelled = true
    }
  }, [])

  if (loading) {
    return (
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex h-full items-center justify-center">
        <Card className="w-full max-w-md">
          <CardContent className="flex flex-col items-center py-8">
            <AlertTriangle className="mb-4 h-12 w-12 text-destructive" />
            <p className="text-center text-muted-foreground">{error}</p>
          </CardContent>
        </Card>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-foreground">Pedidos</h1>
        <p className="text-muted-foreground">Pedidos registrados en el sistema</p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Package className="h-5 w-5 text-primary" />
            Lista de pedidos
          </CardTitle>
          <CardDescription>Pedidos ordenados por fecha (más recientes primero)</CardDescription>
        </CardHeader>
        <CardContent>
          {orders.length === 0 ? (
            <p className="py-8 text-center text-muted-foreground">No hay pedidos</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-border">
                    <th className="pb-3 text-left text-sm font-medium text-muted-foreground">ID</th>
                    <th className="pb-3 text-left text-sm font-medium text-muted-foreground">Cliente</th>
                    <th className="pb-3 text-left text-sm font-medium text-muted-foreground">RUT</th>
                    <th className="pb-3 text-left text-sm font-medium text-muted-foreground">Fecha</th>
                    <th className="pb-3 text-right text-sm font-medium text-muted-foreground">Total</th>
                    <th className="pb-3 text-center text-sm font-medium text-muted-foreground">Estado</th>
                  </tr>
                </thead>
                <tbody>
                  {orders.map((order) => (
                    <tr
                      key={order.id}
                      className="border-b border-border last:border-0 hover:bg-muted/50"
                    >
                      <td className="py-4 font-mono text-sm font-medium">{formatOrderId(order.id)}</td>
                      <td className="py-4">{order.client_name ?? "—"}</td>
                      <td className="py-4 text-muted-foreground">{order.rut ?? "—"}</td>
                      <td className="py-4 text-muted-foreground">{formatDate(order.created_at)}</td>
                      <td className="py-4 text-right font-medium">{formatTotal(Number(order.total))}</td>
                      <td className="py-4 text-center">
                        <StatusBadge status={order.status} />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
