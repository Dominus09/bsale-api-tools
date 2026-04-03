"use client"

import { useEffect, useState } from "react"
import {
  Package,
  AlertTriangle,
  TrendingUp,
  TrendingDown,
  Sparkles,
  Loader2,
} from "lucide-react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import {
  getMarginSummary,
  getMarginAlerts,
  type MarginSummary,
  type MarginAlert,
} from "@/lib/api"
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
} from "recharts"

export default function DashboardPage() {
  const [summary, setSummary] = useState<MarginSummary | null>(null)
  const [alerts, setAlerts] = useState<MarginAlert[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    async function loadData() {
      try {
        const [summaryData, alertsData] = await Promise.all([
          getMarginSummary(),
          getMarginAlerts(),
        ])
        setSummary(summaryData)
        setAlerts(alertsData)
      } catch (err) {
        setError(err instanceof Error ? err.message : "Error al cargar datos")
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

  const pieData = summary
    ? [
        { name: "Bajo margen", value: summary.low_margin_count, color: "#ef4444" },
        { name: "OK", value: summary.ok_count, color: "#22c55e" },
        { name: "Alto margen", value: summary.high_margin_count, color: "#eab308" },
        { name: "Ultra alto", value: summary.ultra_high_margin_count, color: "#a855f7" },
      ]
    : []

  const barData = summary
    ? [
        { name: "Bajo", count: summary.low_margin_count },
        { name: "OK", count: summary.ok_count },
        { name: "Alto", count: summary.high_margin_count },
        { name: "Ultra", count: summary.ultra_high_margin_count },
      ]
    : []

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-foreground">Dashboard</h1>
        <p className="text-muted-foreground">Resumen general del análisis de márgenes</p>
      </div>

      {/* Stats Cards */}
      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              Total Productos
            </CardTitle>
            <Package className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{summary?.total_products || 0}</div>
            <p className="text-xs text-muted-foreground">productos analizados</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              Bajo Margen
            </CardTitle>
            <TrendingDown className="h-4 w-4 text-red-500" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-red-500">
              {summary?.low_margin_count || 0}
            </div>
            <p className="text-xs text-muted-foreground">requieren atención</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              Alto Margen
            </CardTitle>
            <TrendingUp className="h-4 w-4 text-yellow-500" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold text-yellow-500">
              {summary?.high_margin_count || 0}
            </div>
            <p className="text-xs text-muted-foreground">sobre el promedio</p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">
              Margen Promedio
            </CardTitle>
            <Sparkles className="h-4 w-4 text-primary" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {summary?.average_margin?.toFixed(1) || 0}%
            </div>
            <p className="text-xs text-muted-foreground">margen general</p>
          </CardContent>
        </Card>
      </div>

      {/* Charts */}
      <div className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Distribución de Márgenes</CardTitle>
            <CardDescription>Productos por categoría de margen</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={pieData}
                    cx="50%"
                    cy="50%"
                    innerRadius={60}
                    outerRadius={80}
                    paddingAngle={5}
                    dataKey="value"
                  >
                    {pieData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip />
                </PieChart>
              </ResponsiveContainer>
            </div>
            <div className="mt-4 flex flex-wrap justify-center gap-4">
              {pieData.map((item) => (
                <div key={item.name} className="flex items-center gap-2">
                  <div
                    className="h-3 w-3 rounded-full"
                    style={{ backgroundColor: item.color }}
                  />
                  <span className="text-sm text-muted-foreground">
                    {item.name}: {item.value}
                  </span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Análisis por Categoría</CardTitle>
            <CardDescription>Cantidad de productos por estado</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={barData}>
                  <CartesianGrid strokeDasharray="3 3" className="stroke-border" />
                  <XAxis dataKey="name" className="text-xs" />
                  <YAxis className="text-xs" />
                  <Tooltip />
                  <Bar dataKey="count" fill="hsl(var(--primary))" radius={[4, 4, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Alerts Table */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <AlertTriangle className="h-5 w-5 text-yellow-500" />
            Alertas Recientes
          </CardTitle>
          <CardDescription>
            Productos que requieren atención inmediata
          </CardDescription>
        </CardHeader>
        <CardContent>
          {alerts.length === 0 ? (
            <p className="py-8 text-center text-muted-foreground">
              No hay alertas activas
            </p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-border">
                    <th className="pb-3 text-left text-sm font-medium text-muted-foreground">
                      Producto
                    </th>
                    <th className="pb-3 text-left text-sm font-medium text-muted-foreground">
                      Margen Actual
                    </th>
                    <th className="pb-3 text-left text-sm font-medium text-muted-foreground">
                      Margen Esperado
                    </th>
                    <th className="pb-3 text-left text-sm font-medium text-muted-foreground">
                      Tipo
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {alerts.slice(0, 5).map((alert, index) => (
                    <tr key={alert.id ?? `alert-${index}`} className="border-b border-border last:border-0">
                      <td className="py-3 text-sm font-medium">{alert.product_name ?? "—"}</td>
                      <td className="py-3 text-sm">{alert.current_margin != null ? `${alert.current_margin.toFixed(1)}%` : "—"}</td>
                      <td className="py-3 text-sm">{alert.expected_margin != null ? `${alert.expected_margin.toFixed(1)}%` : "—"}</td>
                      <td className="py-3">
                        <Badge
                          variant={
                            alert.alert_type === "LOW_MARGIN" ? "destructive" : "secondary"
                          }
                        >
                          {alert.alert_type === "LOW_MARGIN" ? "Bajo" : alert.alert_type ?? "—"}
                        </Badge>
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
