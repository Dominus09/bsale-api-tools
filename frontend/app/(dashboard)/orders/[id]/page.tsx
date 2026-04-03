"use client"

import type { ReactNode } from "react"
import { useEffect, useState } from "react"
import Link from "next/link"
import { useParams } from "next/navigation"
import {
  AlertTriangle,
  ArrowLeft,
  ClipboardList,
  Loader2,
  Package,
  StickyNote,
  User,
} from "lucide-react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { getOrderById, type OrderDetail } from "@/services/orders"

function formatOrderId(id: number) {
  return `#${String(id).padStart(4, "0")}`
}

function formatDate(iso: string | null | undefined) {
  if (!iso) return "—"
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return "—"
  return d.toLocaleDateString("es-CL")
}

function formatMoney(n: number) {
  return `$${n.toLocaleString("es-CL")}`
}

function formatPriceList(value: string | null | undefined) {
  if (!value?.trim()) return "—"
  return value.trim().charAt(0).toUpperCase() + value.trim().slice(1).toLowerCase()
}

const statusStyles: Record<string, string> = {
  pendiente: "bg-yellow-500/15 text-yellow-800 dark:text-yellow-200 border-yellow-500/40",
  generado: "bg-green-500/15 text-green-800 dark:text-green-200 border-green-500/40",
  anulado: "bg-red-500/15 text-red-800 dark:text-red-200 border-red-500/40",
  revisar: "bg-blue-500/15 text-blue-800 dark:text-blue-200 border-blue-500/40",
}

function StatusBadge({ status, className }: { status: string | null; className?: string }) {
  const key = (status ?? "").toLowerCase().trim()
  const cls = statusStyles[key] ?? "bg-muted text-muted-foreground border-border"
  return (
    <Badge variant="outline" className={`${cls} ${className ?? ""}`}>
      {status?.trim() || "—"}
    </Badge>
  )
}

function DefList({
  items,
  columnsClass = "sm:grid-cols-2",
}: {
  items: { label: string; value: ReactNode }[]
  columnsClass?: string
}) {
  return (
    <dl className={`grid gap-x-6 gap-y-5 ${columnsClass}`}>
      {items.map(({ label, value }) => (
        <div key={label} className="min-w-0">
          <dt className="text-xs font-medium uppercase tracking-wide text-muted-foreground">{label}</dt>
          <dd className="mt-1.5 break-words text-sm text-foreground">{value}</dd>
        </div>
      ))}
    </dl>
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

  const rut = order.client_rut ?? order.rut
  const notesText = (order.notes ?? "").trim()

  return (
    <div className="mx-auto max-w-5xl space-y-6 px-1 pb-8 sm:px-0">
      <div className="flex flex-col gap-4 sm:flex-row sm:flex-wrap sm:items-center sm:justify-between">
        <div className="flex flex-wrap items-center gap-3">
          <Button variant="ghost" size="sm" className="-ml-2 shrink-0" asChild>
            <Link href="/orders" className="gap-2">
              <ArrowLeft className="h-4 w-4" />
              Volver
            </Link>
          </Button>
          <div>
            <h1 className="text-2xl font-bold tracking-tight text-foreground">
              Pedido {formatOrderId(order.id)}
            </h1>
            <p className="text-sm text-muted-foreground">Detalle del pedido</p>
          </div>
        </div>
      </div>

      <Card className="border-l-4 border-l-primary shadow-sm">
        <CardHeader className="space-y-1 pb-4">
          <CardTitle className="flex items-center gap-2 text-lg font-semibold">
            <ClipboardList className="h-5 w-5 shrink-0 text-primary" />
            Información del pedido
          </CardTitle>
          <CardDescription className="text-sm">Identificación, fechas y condiciones comerciales</CardDescription>
        </CardHeader>
        <CardContent className="pt-0">
          <DefList
            columnsClass="sm:grid-cols-2 lg:grid-cols-3"
            items={[
              { label: "ID", value: <span className="font-mono font-semibold">{formatOrderId(order.id)}</span> },
              {
                label: "Estado",
                value: <StatusBadge status={order.status} className="px-2.5 py-0.5 text-xs font-medium" />,
              },
              { label: "Fecha creación", value: formatDate(order.created_at) },
              { label: "Fecha entrega", value: formatDate(order.delivery_date) },
              { label: "Lista de precios", value: formatPriceList(order.price_list) },
              { label: "Forma de pago", value: order.payment_method?.trim() || "—" },
            ]}
          />
        </CardContent>
      </Card>

      <Card className="shadow-sm">
        <CardHeader className="space-y-1 pb-4">
          <CardTitle className="flex items-center gap-2 text-lg font-semibold">
            <User className="h-5 w-5 shrink-0 text-primary" />
            Cliente
          </CardTitle>
          <CardDescription className="text-sm">Datos del cliente y contacto del pedido</CardDescription>
        </CardHeader>
        <CardContent className="pt-0">
          <DefList
            items={[
              { label: "Nombre cliente", value: order.client_name ?? "—" },
              { label: "RUT", value: <span className="text-muted-foreground">{rut ?? "—"}</span> },
              { label: "Nombre contacto", value: order.contact_name?.trim() || "—" },
              { label: "Teléfono contacto", value: order.contact_phone?.trim() || "—" },
            ]}
          />
        </CardContent>
      </Card>

      {notesText ? (
        <Card className="shadow-sm">
          <CardHeader className="space-y-1 pb-4">
            <CardTitle className="flex items-center gap-2 text-lg font-semibold">
              <StickyNote className="h-5 w-5 shrink-0 text-primary" />
              Observaciones
            </CardTitle>
            <CardDescription className="text-sm">Notas asociadas al pedido</CardDescription>
          </CardHeader>
          <CardContent className="pt-0">
            <p className="rounded-md border border-border bg-muted/30 px-4 py-3 text-sm leading-relaxed text-foreground whitespace-pre-wrap">
              {notesText}
            </p>
          </CardContent>
        </Card>
      ) : null}

      <Card className="shadow-sm">
        <CardHeader className="space-y-1 pb-4">
          <CardTitle className="flex items-center gap-2 text-lg font-semibold">
            <Package className="h-5 w-5 shrink-0 text-primary" />
            Productos
          </CardTitle>
          <CardDescription className="text-sm">Líneas del pedido</CardDescription>
        </CardHeader>
        <CardContent className="pt-0">
          {!order.items?.length ? (
            <p className="py-8 text-center text-sm text-muted-foreground">Sin ítems registrados</p>
          ) : (
            <div className="-mx-1 overflow-x-auto sm:mx-0">
              <table className="w-full min-w-[640px] text-sm">
                <thead>
                  <tr className="border-b border-border">
                    <th className="pb-3 pr-4 text-left font-medium text-muted-foreground">Nombre producto</th>
                    <th className="pb-3 pr-4 text-left font-medium text-muted-foreground">Código barra</th>
                    <th className="pb-3 pr-4 text-right font-medium text-muted-foreground">Cantidad</th>
                    <th className="pb-3 pr-4 text-right font-medium text-muted-foreground">Precio</th>
                    <th className="pb-3 text-right font-medium text-muted-foreground">Subtotal</th>
                  </tr>
                </thead>
                <tbody>
                  {order.items.map((line, idx) => {
                    const qty = Number(line.quantity)
                    const price = Number(line.price)
                    const sub = qty * price
                    return (
                      <tr
                        key={`${line.barcode ?? "x"}-${idx}`}
                        className="border-b border-border last:border-0 hover:bg-muted/40"
                      >
                        <td className="py-3 pr-4 font-medium">{line.product_name ?? "—"}</td>
                        <td className="py-3 pr-4 font-mono text-muted-foreground">{line.barcode ?? "—"}</td>
                        <td className="py-3 pr-4 text-right tabular-nums">{qty}</td>
                        <td className="py-3 pr-4 text-right tabular-nums">{formatMoney(price)}</td>
                        <td className="py-3 text-right font-medium tabular-nums">{formatMoney(sub)}</td>
                      </tr>
                    )
                  })}
                </tbody>
                <tfoot>
                  <tr className="border-t-2 border-border bg-muted/20">
                    <td colSpan={4} className="py-3 pr-4 text-right text-sm font-semibold text-foreground">
                      Total pedido
                    </td>
                    <td className="py-3 text-right text-base font-bold tabular-nums text-foreground">
                      {formatMoney(Number(order.total))}
                    </td>
                  </tr>
                </tfoot>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}
