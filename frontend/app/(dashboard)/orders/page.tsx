"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { AlertTriangle, Loader2, Package } from "lucide-react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { getOrders, ORDERS_PAGE_SIZE, type OrderRow } from "@/services/orders"

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

const STATUS_OPTIONS = [
  { value: "all", label: "Todos" },
  { value: "pendiente", label: "Pendiente" },
  { value: "generado", label: "Generado" },
  { value: "anulado", label: "Anulado" },
  { value: "revisar", label: "Revisar" },
] as const

export default function OrdersPage() {
  const router = useRouter()
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [orders, setOrders] = useState<OrderRow[]>([])
  const [selectedStatus, setSelectedStatus] = useState<string>("all")
  const [page, setPage] = useState(1)

  useEffect(() => {
    let cancelled = false
    async function load() {
      setLoading(true)
      setError(null)
      try {
        const data = await getOrders({
          page,
          limit: ORDERS_PAGE_SIZE,
          ...(selectedStatus !== "all" ? { status: selectedStatus } : {}),
        })
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
  }, [page, selectedStatus])

  const canGoPrev = page > 1
  const canGoNext = orders.length >= ORDERS_PAGE_SIZE

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
          <div className="mb-6 flex flex-col gap-3 sm:flex-row sm:flex-wrap sm:items-center sm:justify-between">
            <div className="flex flex-col gap-1.5 sm:min-w-[200px]">
              <label htmlFor="orders-status" className="text-xs font-medium text-muted-foreground">
                Estado
              </label>
              <select
                id="orders-status"
                value={selectedStatus}
                onChange={(e) => {
                  setSelectedStatus(e.target.value)
                  setPage(1)
                }}
                className="h-10 w-full max-w-xs rounded-md border border-input bg-background px-3 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring sm:w-auto"
              >
                {STATUS_OPTIONS.map((opt) => (
                  <option key={opt.value} value={opt.value}>
                    {opt.label}
                  </option>
                ))}
              </select>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <Button
                type="button"
                variant="outline"
                size="sm"
                disabled={!canGoPrev || loading}
                onClick={() => setPage((p) => Math.max(1, p - 1))}
              >
                Anterior
              </Button>
              <span className="min-w-[5.5rem] text-center text-sm text-muted-foreground">
                Página {page}
              </span>
              <Button
                type="button"
                variant="outline"
                size="sm"
                disabled={!canGoNext || loading}
                onClick={() => setPage((p) => p + 1)}
              >
                Siguiente
              </Button>
            </div>
          </div>

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
                      role="link"
                      tabIndex={0}
                      className="cursor-pointer border-b border-border last:border-0 hover:bg-muted/50"
                      onClick={() => router.push(`/orders/${order.id}`)}
                      onKeyDown={(e) => {
                        if (e.key === "Enter" || e.key === " ") {
                          e.preventDefault()
                          router.push(`/orders/${order.id}`)
                        }
                      }}
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
