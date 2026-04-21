"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import Link from "next/link"
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"
import { AlertCircle, Loader2, Users, Wallet } from "lucide-react"

import {
  getDistribuidoraClientsDashboard,
  postDistribuidoraSyncSales,
  type DistribuidoraClientsDashboardResponse,
} from "@/lib/api"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { cn } from "@/lib/utils"

function formatCLP(n: number): string {
  return n.toLocaleString("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  })
}

function formatDay(iso: string | null | undefined): string {
  if (!iso) return "—"
  const d = new Date(iso + "T12:00:00")
  if (Number.isNaN(d.getTime())) return iso
  return d.toLocaleDateString("es-CL", { day: "2-digit", month: "short" })
}

function formatDateTime(iso: string | null | undefined): string {
  if (!iso) return "—"
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  return d.toLocaleString("es-CL", { dateStyle: "short", timeStyle: "short" })
}

export default function DistribuidoraCommercialDashboardPage() {
  const [loading, setLoading] = useState(true)
  const [syncingSales, setSyncingSales] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [data, setData] = useState<DistribuidoraClientsDashboardResponse | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await getDistribuidoraClientsDashboard({
        chart_days: 30,
        recover_min_days: 7,
      })
      setData(res)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al cargar")
      setData(null)
    } finally {
      setLoading(false)
    }
  }, [])

  const onSyncSalesFromBsale = useCallback(async () => {
    setSyncingSales(true)
    setError(null)
    try {
      const r = await postDistribuidoraSyncSales()
      if (!r.ok) {
        setError(r.error ?? "No se pudo sincronizar ventas desde Bsale.")
        return
      }
      await load()
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al sincronizar ventas.")
    } finally {
      setSyncingSales(false)
    }
  }, [load])

  useEffect(() => {
    void load()
  }, [load])

  const lineData = useMemo(() => {
    if (!data?.daily_sales?.length) return []
    return data.daily_sales.map((r) => ({
      label: formatDay(r.day ?? ""),
      total: Number(r.total_net ?? 0),
    }))
  }, [data])

  const barData = useMemo(() => {
    if (!data?.sales_by_seller?.length) return []
    return [...data.sales_by_seller]
      .filter((s) => (s.seller_name ?? "").trim())
      .slice(0, 12)
      .map((s) => ({
        name: (s.seller_name ?? "—").slice(0, 18),
        ventas: Number(s.ventas ?? 0),
      }))
  }, [data])

  const kpis = data?.kpis

  return (
    <div className="mx-auto flex max-w-[1400px] flex-col gap-8 pb-12">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            Distribuidora
          </p>
          <h1 className="mt-1 text-2xl font-semibold tracking-tight">Dashboard comercial</h1>
          <p className="mt-2 max-w-2xl text-sm text-muted-foreground">
            Ventas diarias, vendedores, KPI del mes y oportunidades de recuperación.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button variant="outline" size="sm" asChild>
            <Link href="/distribuidora/clientes">Clientes</Link>
          </Button>
          <Button variant="outline" size="sm" asChild>
            <Link href="/distribuidora/clientes/inactivos">Inactivos</Link>
          </Button>
          <Button variant="outline" size="sm" asChild>
            <Link href="/distribuidora/vendedores">Vendedores</Link>
          </Button>
          <Button type="button" variant="secondary" size="sm" onClick={() => void load()} disabled={loading}>
            Actualizar
          </Button>
          <Button
            type="button"
            variant="default"
            size="sm"
            onClick={() => void onSyncSalesFromBsale()}
            disabled={loading || syncingSales}
            className="gap-1.5"
          >
            {syncingSales ? (
              <Loader2 className="h-4 w-4 animate-spin" aria-hidden />
            ) : null}
            Actualizar ventas
          </Button>
        </div>
      </div>

      {error ? (
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {loading ? (
        <div className="flex min-h-[240px] items-center justify-center gap-2 text-muted-foreground">
          <Loader2 className="h-8 w-8 animate-spin" />
          <span>Cargando dashboard…</span>
        </div>
      ) : data ? (
        <>
          <section className="grid gap-4 sm:grid-cols-3">
            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Ventas mes ({data.kpi_month.label})</CardTitle>
                <Wallet className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <p className="text-2xl font-bold tabular-nums">{formatCLP(Number(kpis?.ventas_mes ?? 0))}</p>
                <p className="text-xs text-muted-foreground">Neto (incluye NC)</p>
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Ticket promedio (mes)</CardTitle>
                <Wallet className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <p className="text-2xl font-bold tabular-nums">{formatCLP(Number(kpis?.ticket_mes ?? 0))}</p>
                <p className="text-xs text-muted-foreground">Solo facturas y boletas</p>
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                <CardTitle className="text-sm font-medium">Clientes activos (mes)</CardTitle>
                <Users className="h-4 w-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <p className="text-2xl font-bold tabular-nums">{Number(kpis?.clientes_activos ?? 0)}</p>
                <p className="text-xs text-muted-foreground">Con al menos una compra en el mes</p>
              </CardContent>
            </Card>
          </section>

          <div className="grid gap-6 lg:grid-cols-2">
            <Card className="min-h-[320px]">
              <CardHeader>
                <CardTitle className="text-base">Ventas por día</CardTitle>
                <p className="text-sm text-muted-foreground">
                  Últimos {data.chart_range.days} días · neto diario
                </p>
              </CardHeader>
              <CardContent className="h-[260px]">
                {lineData.length === 0 ? (
                  <p className="text-sm text-muted-foreground">Sin datos en el rango.</p>
                ) : (
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={lineData} margin={{ left: 4, right: 8, top: 8, bottom: 0 }}>
                      <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                      <XAxis dataKey="label" tick={{ fontSize: 11 }} interval="preserveStartEnd" />
                      <YAxis
                        tick={{ fontSize: 11 }}
                        tickFormatter={(v) =>
                          Number(v).toLocaleString("es-CL", { notation: "compact", maximumFractionDigits: 0 })
                        }
                      />
                      <Tooltip
                        formatter={(v: number) => [formatCLP(Number(v)), "Neto"]}
                        labelClassName="text-xs"
                      />
                      <Line type="monotone" dataKey="total" stroke="hsl(var(--primary))" strokeWidth={2} dot={false} />
                    </LineChart>
                  </ResponsiveContainer>
                )}
              </CardContent>
            </Card>

            <Card className="min-h-[320px]">
              <CardHeader>
                <CardTitle className="text-base">Ventas por vendedor</CardTitle>
                <p className="text-sm text-muted-foreground">Top 12 en el mismo período del gráfico</p>
              </CardHeader>
              <CardContent className="h-[260px]">
                {barData.length === 0 ? (
                  <p className="text-sm text-muted-foreground">Sin datos.</p>
                ) : (
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={barData} layout="vertical" margin={{ left: 8, right: 16, top: 8 }}>
                      <CartesianGrid strokeDasharray="3 3" className="stroke-muted" horizontal={false} />
                      <XAxis
                        type="number"
                        tick={{ fontSize: 11 }}
                        tickFormatter={(v) =>
                          Number(v).toLocaleString("es-CL", { notation: "compact", maximumFractionDigits: 0 })
                        }
                      />
                      <YAxis type="category" dataKey="name" width={100} tick={{ fontSize: 11 }} />
                      <Tooltip formatter={(v: number) => [formatCLP(Number(v)), "Ventas netas"]} />
                      <Bar dataKey="ventas" fill="hsl(var(--chart-2))" radius={[0, 4, 4, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                )}
              </CardContent>
            </Card>
          </div>

          <Card className="border-red-200/80 dark:border-red-900/40">
            <CardHeader>
              <CardTitle className="text-base text-red-700 dark:text-red-300">Clientes a recuperar</CardTitle>
              <p className="text-sm text-muted-foreground">
                Top 10 por días sin comprar (riesgo). Valor histórico neto como referencia de oportunidad.
              </p>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto rounded-md border">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Cliente</TableHead>
                      <TableHead>Vendedor</TableHead>
                      <TableHead>Última compra</TableHead>
                      <TableHead className="text-right">Días sin comprar</TableHead>
                      <TableHead className="text-right">Valor hist. neto</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {(data.recover_clients ?? []).map((r) => {
                      const d = Number(r.dias_sin_comprar ?? 0)
                      return (
                        <TableRow
                          key={r.client_id}
                          className={cn(d >= 30 && "bg-red-50/90 dark:bg-red-950/30")}
                        >
                          <TableCell className="font-medium">
                            {(r.client_name ?? "").trim() || `Cliente ${r.client_id}`}
                          </TableCell>
                          <TableCell className="text-muted-foreground">
                            {(r.vendedor ?? "").trim() || "—"}
                          </TableCell>
                          <TableCell className="tabular-nums text-muted-foreground">
                            {formatDateTime(r.ultima_compra)}
                          </TableCell>
                          <TableCell className="text-right font-semibold tabular-nums text-red-700 dark:text-red-300">
                            {d}
                          </TableCell>
                          <TableCell className="text-right tabular-nums">
                            {formatCLP(Number(r.valor_historico_neto ?? 0))}
                          </TableCell>
                        </TableRow>
                      )
                    })}
                  </TableBody>
                </Table>
              </div>
            </CardContent>
          </Card>
        </>
      ) : null}
    </div>
  )
}
