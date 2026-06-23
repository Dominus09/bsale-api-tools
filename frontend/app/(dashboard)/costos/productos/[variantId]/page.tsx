"use client"

import { useEffect, useMemo, useState } from "react"
import Link from "next/link"
import { useParams, useSearchParams } from "next/navigation"
import { ArrowLeft, CircleDollarSign, Loader2 } from "lucide-react"
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts"

import {
  compareCostOffices,
  getCostVariantHistory,
  getStoredCompanyId,
  type CostHistoryRow,
} from "@/lib/api"
import { WatchlistButton } from "../../components/watchlist-button"
import { Badge } from "@/components/ui/badge"
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

const RECEPTION_LABELS: Record<string, string> = {
  recepcion_normal: "Recepción normal",
  recepcion_ajuste: "Recepción ajuste",
  recepcion_devolucion: "Recepción devolución",
  recepcion_nc: "Recepción NC",
}

function formatMoney(value: number | null | undefined) {
  if (value == null || !Number.isFinite(Number(value))) return "—"
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(Number(value))
}

function formatDate(value: string | null | undefined) {
  if (!value) return "—"
  const d = new Date(value)
  if (Number.isNaN(d.getTime())) return value
  return d.toLocaleString("es-CL", { dateStyle: "short", timeStyle: "short" })
}

function formatPct(value: number | null | undefined) {
  if (value == null || !Number.isFinite(Number(value))) return "—"
  const n = Number(value)
  return `${n > 0 ? "+" : ""}${n.toFixed(2)}%`
}

export default function CostoProductoPage() {
  const params = useParams()
  const searchParams = useSearchParams()
  const variantId = Number(params.variantId)
  const companyId = Number(searchParams.get("company_id") ?? getStoredCompanyId() ?? 0)

  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [data, setData] = useState<Awaited<ReturnType<typeof getCostVariantHistory>> | null>(null)
  const [offices, setOffices] = useState<
    Awaited<ReturnType<typeof compareCostOffices>> | null
  >(null)

  useEffect(() => {
    if (!companyId || !variantId) return
    setLoading(true)
    setError(null)
    Promise.all([
      getCostVariantHistory(variantId, companyId),
      compareCostOffices({ company_id: companyId, variant_id: variantId }),
    ])
      .then(([hist, cmp]) => {
        setData(hist)
        setOffices(cmp)
      })
      .catch((e: unknown) =>
        setError(e instanceof Error ? e.message : "Error al cargar producto"),
      )
      .finally(() => setLoading(false))
  }, [companyId, variantId])

  const chartData = useMemo(() => {
    const series = data?.chart_series ?? data?.items ?? []
    return [...series]
      .sort(
        (a, b) =>
          new Date(String(a.date ?? 0)).getTime() -
          new Date(String(b.date ?? 0)).getTime(),
      )
      .map((p) => ({
        date: formatDate(p.date as string),
        neto: Number(p.cost_net ?? 0),
        bruto: Number(p.cost_bruto_erp ?? 0),
        promedio: Number(p.average_cost ?? 0),
      }))
  }, [data])

  const comparison =
    offices && "comparison" in offices ? offices.comparison : null

  if (!companyId || !variantId) {
    return (
      <div className="p-6 text-sm text-muted-foreground">
        Parámetros inválidos.{" "}
        <Link href="/costos" className="underline">
          Volver a Costos
        </Link>
      </div>
    )
  }

  return (
    <div className="space-y-6 p-6">
      <div className="flex flex-wrap items-center gap-4">
        <Button variant="ghost" size="sm" asChild>
          <Link href="/costos">
            <ArrowLeft className="mr-2 h-4 w-4" />
            Costos
          </Link>
        </Button>
        <div className="flex-1">
          <h1 className="flex items-center gap-2 text-2xl font-semibold">
            <CircleDollarSign className="h-7 w-7" />
            {data?.product_name ?? "Producto"}
          </h1>
          <p className="text-sm text-muted-foreground">
            {data?.variant_name} · Código {variantId}
            {data?.barcode ? ` · ${data.barcode}` : ""}
          </p>
        </div>
        <WatchlistButton companyId={companyId} variantId={variantId} />
      </div>

      {loading ? (
        <div className="flex justify-center py-16">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : error ? (
        <p className="text-sm text-destructive">{error}</p>
      ) : (
        <>
          <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm text-muted-foreground">Costo promedio</CardTitle>
              </CardHeader>
              <CardContent className="text-2xl font-semibold">
                {formatMoney(data?.average_cost)}
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm text-muted-foreground">Último costo neto</CardTitle>
              </CardHeader>
              <CardContent className="text-2xl font-semibold">
                {formatMoney(data?.items?.[0]?.cost_net)}
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm text-muted-foreground">Recepciones</CardTitle>
              </CardHeader>
              <CardContent className="text-2xl font-semibold">
                {data?.items?.length ?? 0}
              </CardContent>
            </Card>
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm text-muted-foreground">Variación sucursales</CardTitle>
              </CardHeader>
              <CardContent className="text-2xl font-semibold">
                {formatPct(comparison?.max_spread_pct)}
              </CardContent>
            </Card>
          </div>

          {chartData.length > 0 ? (
            <Card>
              <CardHeader>
                <CardTitle>Evolución de costo</CardTitle>
              </CardHeader>
              <CardContent className="h-72">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={chartData}>
                    <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
                    <XAxis dataKey="date" tick={{ fontSize: 11 }} />
                    <YAxis tick={{ fontSize: 11 }} tickFormatter={(v) => `$${v}`} />
                    <Tooltip formatter={(v: number) => formatMoney(v)} />
                    <Legend />
                    <Line type="monotone" dataKey="neto" name="Costo neto" stroke="#2563eb" dot={false} />
                    <Line type="monotone" dataKey="bruto" name="Costo bruto ERP" stroke="#7c3aed" dot={false} />
                    <Line type="monotone" dataKey="promedio" name="Promedio" stroke="#16a34a" dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              </CardContent>
            </Card>
          ) : null}

          {comparison?.offices && comparison.offices.length > 0 ? (
            <Card>
              <CardHeader>
                <CardTitle>Costo por sucursal</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
                  {comparison.offices.map((o) => (
                    <div key={o.office_id} className="rounded-lg border p-3">
                      <p className="font-medium">{o.office_name}</p>
                      <p className="text-lg font-semibold">{formatMoney(o.cost_net)}</p>
                      <p className="text-xs text-muted-foreground">
                        {formatDate(o.admission_date)}
                      </p>
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>
          ) : null}

          <Card>
            <CardHeader>
              <CardTitle>Historial de recepciones</CardTitle>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Fecha</TableHead>
                    <TableHead>Sucursal</TableHead>
                    <TableHead>Documento</TableHead>
                    <TableHead>Nota</TableHead>
                    <TableHead>Tipo</TableHead>
                    <TableHead className="text-right">Cant.</TableHead>
                    <TableHead className="text-right">Costo</TableHead>
                    <TableHead className="text-right">Var. %</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {(data?.items ?? []).map((r: CostHistoryRow) => (
                    <TableRow key={r.reception_detail_id}>
                      <TableCell>{formatDate(r.admission_date)}</TableCell>
                      <TableCell>{r.office_name ?? "—"}</TableCell>
                      <TableCell>
                        {r.document ?? ""} {r.document_number ?? ""}
                      </TableCell>
                      <TableCell className="max-w-[200px] truncate text-sm">
                        {r.reception_note ?? "—"}
                      </TableCell>
                      <TableCell>
                        {r.reception_type ? (
                          <Badge variant="outline">
                            {RECEPTION_LABELS[r.reception_type] ?? r.reception_type}
                          </Badge>
                        ) : (
                          "—"
                        )}
                      </TableCell>
                      <TableCell className="text-right">{r.quantity}</TableCell>
                      <TableCell className="text-right">{formatMoney(r.cost_net)}</TableCell>
                      <TableCell
                        className={cn(
                          "text-right",
                          r.variation_pct != null && Math.abs(r.variation_pct) >= 10
                            ? "text-amber-600"
                            : "",
                        )}
                      >
                        {formatPct(r.variation_pct)}
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </>
      )}
    </div>
  )
}
