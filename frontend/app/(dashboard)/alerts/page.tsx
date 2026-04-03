"use client"

import { useEffect, useState } from "react"
import { AlertTriangle, Loader2, Bell, TrendingDown, TrendingUp } from "lucide-react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { getMarginAlerts, type MarginAlert } from "@/lib/api"

const alertTypeConfig: Record<string, { color: string; icon: typeof TrendingDown; label: string }> = {
  LOW_MARGIN: { color: "bg-red-500 text-white", icon: TrendingDown, label: "Margen Bajo" },
  HIGH_MARGIN: { color: "bg-yellow-500 text-white", icon: TrendingUp, label: "Margen Alto" },
  ULTRA_HIGH_MARGIN: { color: "bg-purple-500 text-white", icon: TrendingUp, label: "Ultra Alto" },
}

export default function AlertsPage() {
  const [alerts, setAlerts] = useState<MarginAlert[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    async function loadData() {
      try {
        const data = await getMarginAlerts()
        setAlerts(data)
      } catch (err) {
        setError(err instanceof Error ? err.message : "Error al cargar alertas")
      } finally {
        setIsLoading(false)
      }
    }

    loadData()
  }, [])

  if (isLoading) {
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

  const lowMarginAlerts = alerts.filter((a) => a.alert_type === "LOW_MARGIN")
  const highMarginAlerts = alerts.filter((a) => a.alert_type === "HIGH_MARGIN" || a.alert_type === "ULTRA_HIGH_MARGIN")

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-foreground">Alertas de Márgenes</h1>
        <p className="text-muted-foreground">
          Productos que requieren revisión de precios
        </p>
      </div>

      {/* Summary Cards */}
      <div className="grid gap-4 sm:grid-cols-3">
        <Card>
          <CardContent className="flex items-center gap-4 pt-6">
            <div className="flex h-12 w-12 items-center justify-center rounded-lg bg-red-100">
              <TrendingDown className="h-6 w-6 text-red-500" />
            </div>
            <div>
              <div className="text-2xl font-bold">{lowMarginAlerts.length}</div>
              <p className="text-sm text-muted-foreground">Margen bajo</p>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="flex items-center gap-4 pt-6">
            <div className="flex h-12 w-12 items-center justify-center rounded-lg bg-yellow-100">
              <TrendingUp className="h-6 w-6 text-yellow-500" />
            </div>
            <div>
              <div className="text-2xl font-bold">{highMarginAlerts.length}</div>
              <p className="text-sm text-muted-foreground">Margen alto</p>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="flex items-center gap-4 pt-6">
            <div className="flex h-12 w-12 items-center justify-center rounded-lg bg-primary/10">
              <Bell className="h-6 w-6 text-primary" />
            </div>
            <div>
              <div className="text-2xl font-bold">{alerts.length}</div>
              <p className="text-sm text-muted-foreground">Total alertas</p>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Alerts Table */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <AlertTriangle className="h-5 w-5 text-yellow-500" />
            Todas las Alertas
          </CardTitle>
          <CardDescription>
            Lista de productos con problemas de margen
          </CardDescription>
        </CardHeader>
        <CardContent>
          {alerts.length === 0 ? (
            <div className="flex flex-col items-center py-12">
              <div className="mb-4 flex h-16 w-16 items-center justify-center rounded-full bg-green-100">
                <Bell className="h-8 w-8 text-green-500" />
              </div>
              <p className="text-lg font-medium text-foreground">Sin alertas</p>
              <p className="text-muted-foreground">
                Todos los productos tienen márgenes saludables
              </p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-border">
                    <th className="pb-3 text-left text-sm font-medium text-muted-foreground">
                      Producto
                    </th>
                    <th className="pb-3 text-right text-sm font-medium text-muted-foreground">
                      Margen Actual
                    </th>
                    <th className="pb-3 text-right text-sm font-medium text-muted-foreground">
                      Margen Esperado
                    </th>
                    <th className="pb-3 text-right text-sm font-medium text-muted-foreground">
                      Diferencia
                    </th>
                    <th className="pb-3 text-center text-sm font-medium text-muted-foreground">
                      Tipo de Alerta
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {alerts.map((alert, index) => {
                    const config = alertTypeConfig[alert.alert_type] || {
                      color: "bg-gray-500 text-white",
                      icon: AlertTriangle,
                      label: alert.alert_type ?? "—",
                    }
                    const currentMargin = alert.current_margin ?? 0
                    const expectedMargin = alert.expected_margin ?? 0
                    const difference = currentMargin - expectedMargin

                    return (
                      <tr
                        key={alert.id ?? `alert-${index}`}
                        className="border-b border-border last:border-0 hover:bg-muted/50"
                      >
                        <td className="py-4">
                          <div className="font-medium">{alert.product_name ?? "—"}</div>
                        </td>
                        <td className="py-4 text-right">
                          <span
                            className={
                              alert.alert_type === "LOW_MARGIN"
                                ? "font-medium text-red-500"
                                : "font-medium text-yellow-600"
                            }
                          >
                            {alert.current_margin != null ? `${currentMargin.toFixed(1)}%` : "—"}
                          </span>
                        </td>
                        <td className="py-4 text-right text-muted-foreground">
                          {alert.expected_margin != null ? `${expectedMargin.toFixed(1)}%` : "—"}
                        </td>
                        <td className="py-4 text-right">
                          <span
                            className={
                              difference < 0 ? "text-red-500" : "text-green-500"
                            }
                          >
                            {alert.current_margin != null && alert.expected_margin != null
                              ? `${difference > 0 ? "+" : ""}${difference.toFixed(1)}%`
                              : "—"}
                          </span>
                        </td>
                        <td className="py-4 text-center">
                          <Badge className={config.color}>{config.label}</Badge>
                        </td>
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
