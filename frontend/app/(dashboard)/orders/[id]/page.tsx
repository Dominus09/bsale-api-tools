"use client"

import { useEffect, useState } from "react"
import Link from "next/link"
import { useParams } from "next/navigation"
import { AlertTriangle, ArrowLeft, Loader2, Package } from "lucide-react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { getOrderById, type OrderDetail } from "@/services/orders"

function formatOrderId(id: number) {
  return `#${String(id).padStart(4, "0")}`
}

function formatDate(iso: string) {
  if (!iso) return "—"
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return "—"
  return d.toLocaleDateString("es-CL")
}

function formatMoney(n: number) {
  return `$${n.toLocaleString("es-CL")}`
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

export default function OrderDetailPage() {
  const params = useParams()
  const rawId = params?.id
  const orderId = typeof rawId === "string" ? Number.parseInt(rawId, 10) : Number.NaN

  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [order, setOrder] = useState<OrderDetail | null>(null)

  useEffect(() => {
    if (!Number.isFinite(orderId) || orderId < 1) {
      setLoading(false)
      setError("Pedido no válido")
      return
    }

    let cancelled = false
    async function load() {
      setLoading(true)
      setError(null)
      try {
        const data = await getOrderById(orderId)
        if (!cancelled) setOrder(data)
      } catch {
        if (!cancelled) setError("Error cargando pedido")
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    load()
    return () => {
      cancelled = true
    }
  }, [orderId])

  if (loading) {
    return (
      <div className="flex h-full items-center justify-center">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    )
  }

  if (error || !order) {
    return (
      <div className="space-y-4">
        <Button variant="ghost" size="sm" asChild>
          <Link href="/orders" className="gap-2">
            <ArrowLeft className="h-4 w-4" />
            Volver a pedidos
          </Link>
        </Button>
        <div className="flex h-[40vh] items-center justify-center">
          <Card className="w-full max-w-md">
            <CardContent className="flex flex-col items-center py-8">
              <AlertTriangle className="mb-4 h-12 w-12 text-destructive" />
              <p className="text-center text-muted-foreground">{error ?? "Pedido no encontrado"}</p>
            </CardContent>
          </Card>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center gap-4">
        <Button variant="ghost" size="sm" asChild>
          <Link href="/orders" className="gap-2">
            <ArrowLeft className="h-4 w-4" />
            Volver
          </Link>
        </Button>
        <div>
          <h1 className="text-2xl font-bold text-foreground">
            Pedido {formatOrderId(order.id)}
          </h1>
          <p className="text-muted-foreground">Detalle del pedido</p>
        </div>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-lg">
            <Package className="h-5 w-5 text-primary" />
            Datos del pedido
          </CardTitle>
          <CardDescription>Cliente y totales</CardDescription>
        </CardHeader>
        <CardContent>
          <dl className="grid gap-3 sm:grid-cols-2">
            <div>
              <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">ID</dt>
              <dd className="font-mono text-sm font-medium">{formatOrderId(order.id)}</dd>
            </div>
            <div>
              <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">Estado</dt>
              <dd className="pt-0.5">
                <StatusBadge status={order.status} />
              </dd>
            </div>
            <div>
              <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">Cliente</dt>
              <dd className="text-sm">{order.client_name ?? "—"}</dd>
            </div>
            <div>
              <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">RUT</dt>
              <dd className="text-sm text-muted-foreground">{order.rut ?? "—"}</dd>
            </div>
            <div>
              <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">Fecha</dt>
              <dd className="text-sm text-muted-foreground">{formatDate(order.created_at)}</dd>
            </div>
            <div>
              <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">Total</dt>
              <dd className="text-sm font-semibold">{formatMoney(Number(order.total))}</dd>
            </div>
          </dl>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-lg">Productos</CardTitle>
          <CardDescription>Líneas del pedido</CardDescription>
        </CardHeader>
        <CardContent>
          {!order.items?.length ? (
            <p className="py-6 text-center text-muted-foreground">Sin ítems registrados</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-border">
                    <th className="pb-3 text-left text-sm font-medium text-muted-foreground">Producto</th>
                    <th className="pb-3 text-left text-sm font-medium text-muted-foreground">Código barra</th>
                    <th className="pb-3 text-right text-sm font-medium text-muted-foreground">Cantidad</th>
                    <th className="pb-3 text-right text-sm font-medium text-muted-foreground">Precio</th>
                    <th className="pb-3 text-right text-sm font-medium text-muted-foreground">Subtotal</th>
                  </tr>
                </thead>
                <tbody>
                  {order.items.map((line, idx) => {
                    const qty = Number(line.quantity)
                    const price = Number(line.price)
                    const sub = qty * price
                    return (
                      <tr key={`${line.barcode ?? "x"}-${idx}`} className="border-b border-border last:border-0">
                        <td className="py-3 font-medium">{line.product_name ?? "—"}</td>
                        <td className="py-3 text-muted-foreground">{line.barcode ?? "—"}</td>
                        <td className="py-3 text-right">{qty}</td>
                        <td className="py-3 text-right">{formatMoney(price)}</td>
                        <td className="py-3 text-right font-medium">{formatMoney(sub)}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
