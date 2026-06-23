"use client"

import Link from "next/link"
import { useCallback, useEffect, useState } from "react"
import {
  AlertTriangle,
  Loader2,
  Star,
  TrendingDown,
  TrendingUp,
} from "lucide-react"

import {
  getCostIntelligence,
  removeCostWatchlistItem,
  type CostIntelligencePayload,
  type CostOpportunityRow,
  type CostWatchlistRow,
} from "@/lib/api"
import { Alert, AlertDescription } from "@/components/ui/alert"
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
import { WatchlistButton } from "./watchlist-button"

function formatMoney(value: number | null | undefined) {
  if (value == null || !Number.isFinite(Number(value))) return "—"
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(Number(value))
}

function formatPct(value: number | null | undefined) {
  if (value == null || !Number.isFinite(Number(value))) return "—"
  const n = Number(value)
  return `${n > 0 ? "+" : ""}${n.toFixed(1)}%`
}

function formatDate(value: string | null | undefined) {
  if (!value) return "—"
  const d = new Date(value)
  if (Number.isNaN(d.getTime())) return value
  return d.toLocaleString("es-CL", { dateStyle: "short", timeStyle: "short" })
}

function SemaphoreDot({ level }: { level: string }) {
  const cls =
    level === "red"
      ? "bg-red-500"
      : level === "yellow"
        ? "bg-amber-400"
        : "bg-green-500"
  return <span className={cn("inline-block h-3 w-3 shrink-0 rounded-full", cls)} />
}

function ProductMiniCard({
  row,
  companyId,
  kind,
}: {
  row: CostOpportunityRow
  companyId: number
  kind: "buy" | "risk"
}) {
  const title = [row.product_name, row.variant_name].filter(Boolean).join(" — ")
  return (
    <Card className={kind === "buy" ? "border-green-200 dark:border-green-900" : "border-red-200 dark:border-red-900"}>
      <CardContent className="space-y-2 py-4">
        <div className="flex items-start justify-between gap-2">
          <Link
            href={`/costos/productos/${row.variant_id}?company_id=${companyId}`}
            className="font-medium hover:underline"
          >
            {title || `Variante ${row.variant_id}`}
          </Link>
          <WatchlistButton companyId={companyId} variantId={row.variant_id} size="sm" />
        </div>
        <div className="grid grid-cols-2 gap-2 text-sm">
          <div>
            <p className="text-muted-foreground">Costo actual</p>
            <p className="font-semibold">{formatMoney(row.current_cost)}</p>
          </div>
          <div>
            <p className="text-muted-foreground">Promedio 90d</p>
            <p className="font-semibold">{formatMoney(row.avg_90d)}</p>
          </div>
        </div>
        <div className="flex items-center justify-between">
          <span className={cn("text-sm font-medium", kind === "buy" ? "text-green-700 dark:text-green-400" : "text-red-700 dark:text-red-400")}>
            {formatPct(row.variation_pct_90d)}
          </span>
          <Badge className={kind === "buy" ? "bg-green-100 text-green-800 dark:bg-green-950 dark:text-green-300" : "bg-red-100 text-red-800 dark:bg-red-950 dark:text-red-300"}>
            {kind === "buy" ? "Comprar" : "Revisar precio"}
          </Badge>
        </div>
        {row.commercial_score_label ? (
          <p className="flex items-center gap-2 text-xs text-muted-foreground">
            <SemaphoreDot level={row.commercial_score_semaphore ?? "yellow"} />
            Score: {row.commercial_score_label}
          </p>
        ) : null}
      </CardContent>
    </Card>
  )
}

export function CostIntelligencePanel({
  companyId,
  compact = false,
}: {
  companyId: number
  compact?: boolean
}) {
  const [data, setData] = useState<CostIntelligencePayload | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const payload = await getCostIntelligence(companyId)
      setData(payload)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al cargar resumen")
    } finally {
      setLoading(false)
    }
  }, [companyId])

  useEffect(() => {
    void load()
  }, [load])

  if (loading) {
    return (
      <div className="flex justify-center py-8">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error) {
    return (
      <Alert variant="destructive">
        <AlertTriangle className="h-4 w-4" />
        <AlertDescription>{error}</AlertDescription>
      </Alert>
    )
  }

  if (!data) return null

  const featured = data.auto_summary?.featured_product

  return (
    <div className="space-y-6">
      <Card className="border-primary/20 bg-gradient-to-br from-primary/5 to-transparent">
        <CardHeader className="pb-2">
          <CardTitle className="text-lg">Resumen Costos</CardTitle>
          <p className="text-sm text-muted-foreground">
            Últimas 24 horas · empresa consolidada
          </p>
        </CardHeader>
        <CardContent className="space-y-4">
          <ul className="space-y-1 text-sm">
            {(data.auto_summary?.bullets ?? []).map((b) => (
              <li key={b} className="flex items-start gap-2">
                <span className="mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full bg-primary" />
                {b}
              </li>
            ))}
          </ul>
          {featured ? (
            <div className="rounded-lg border bg-card p-4">
              <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                Producto destacado
              </p>
              <p className="mt-1 text-lg font-semibold">
                {[featured.product_name, featured.variant_name].filter(Boolean).join(" — ")}
              </p>
              <div className="mt-3 grid gap-3 sm:grid-cols-3 text-sm">
                <div>
                  <p className="text-muted-foreground">Costo actual</p>
                  <p className="font-semibold">{formatMoney(featured.current_cost)}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Promedio 90 días</p>
                  <p className="font-semibold">{formatMoney(featured.avg_90d)}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Variación</p>
                  <p className="font-semibold text-green-700 dark:text-green-400">
                    {formatPct(featured.variation_pct_90d)}
                  </p>
                </div>
              </div>
              {featured.status === "oportunidad_compra" ? (
                <Badge className="mt-3 bg-green-100 text-green-800 dark:bg-green-950 dark:text-green-300">
                  Oportunidad de compra
                </Badge>
              ) : null}
            </div>
          ) : null}
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="flex flex-row items-center justify-between pb-2">
          <CardTitle className="flex items-center gap-2 text-base">
            <Star className="h-4 w-4 text-amber-500" />
            Watchlist
          </CardTitle>
          <span className="text-xs text-muted-foreground">Productos que sigues</span>
        </CardHeader>
        <CardContent>
          {(data.watchlist ?? []).length === 0 ? (
            <p className="text-sm text-muted-foreground">
              Agrega productos desde fichas, alertas u oportunidades con ⭐ Agregar a Watchlist.
            </p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead />
                  <TableHead>Producto</TableHead>
                  <TableHead className="text-right">Último costo</TableHead>
                  <TableHead className="text-right">Prom. 90d</TableHead>
                  <TableHead className="text-right">Variación</TableHead>
                  <TableHead>Estado</TableHead>
                  <TableHead>Última recepción</TableHead>
                  <TableHead />
                </TableRow>
              </TableHeader>
              <TableBody>
                {(data.watchlist ?? []).map((w: CostWatchlistRow) => (
                  <TableRow key={w.variant_id}>
                    <TableCell>
                      <SemaphoreDot level={w.watchlist_semaphore ?? "yellow"} />
                    </TableCell>
                    <TableCell>
                      <Link
                        href={`/costos/productos/${w.variant_id}?company_id=${companyId}`}
                        className="hover:underline"
                      >
                        {w.product_name ?? w.variant_name ?? w.variant_id}
                      </Link>
                    </TableCell>
                    <TableCell className="text-right">{formatMoney(w.current_cost)}</TableCell>
                    <TableCell className="text-right">{formatMoney(w.avg_90d)}</TableCell>
                    <TableCell className="text-right">{formatPct(w.variation_pct_90d)}</TableCell>
                    <TableCell>
                      <Badge variant="outline">{w.watchlist_status_label ?? "—"}</Badge>
                    </TableCell>
                    <TableCell className="text-sm">{formatDate(w.last_reception_date)}</TableCell>
                    <TableCell>
                      <Button
                        size="sm"
                        variant="ghost"
                        onClick={() =>
                          void removeCostWatchlistItem(companyId, w.variant_id).then(() => load())
                        }
                      >
                        Quitar
                      </Button>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      {!compact ? (
        <>
          <div className="grid gap-4 lg:grid-cols-2">
            <div className="space-y-3">
              <h3 className="flex items-center gap-2 font-semibold">
                <TrendingDown className="h-5 w-5 text-green-600" />
                Oportunidades destacadas · Top 10
              </h3>
              <div className="grid gap-3 sm:grid-cols-2">
                {(data.top_opportunities ?? []).length === 0 ? (
                  <p className="text-sm text-muted-foreground">Sin oportunidades detectadas.</p>
                ) : (
                  (data.top_opportunities ?? []).map((r) => (
                    <ProductMiniCard key={r.variant_id} row={r} companyId={companyId} kind="buy" />
                  ))
                )}
              </div>
            </div>
            <div className="space-y-3">
              <h3 className="flex items-center gap-2 font-semibold">
                <TrendingUp className="h-5 w-5 text-red-600" />
                Riesgos destacados · Top 10
              </h3>
              <div className="grid gap-3 sm:grid-cols-2">
                {(data.top_risks ?? []).length === 0 ? (
                  <p className="text-sm text-muted-foreground">Sin riesgos comerciales destacados.</p>
                ) : (
                  (data.top_risks ?? []).map((r) => (
                    <ProductMiniCard key={r.variant_id} row={r} companyId={companyId} kind="risk" />
                  ))
                )}
              </div>
            </div>
          </div>

          <Card>
            <CardHeader>
              <CardTitle className="text-base">Alertas accionables</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2">
              {(data.actionable_alerts ?? []).length === 0 ? (
                <p className="text-sm text-muted-foreground">Sin alertas que requieran acción.</p>
              ) : (
                (data.actionable_alerts ?? []).map((a, i) => (
                  <div
                    key={`${a.variant_id}-${i}`}
                    className="flex flex-wrap items-center justify-between gap-2 rounded-lg border p-3"
                  >
                    <div className="flex items-center gap-3">
                      <SemaphoreDot level={a.action_semaphore ?? a.semaphore} />
                      <div>
                        <p className="font-medium">
                          {a.product_name} — {a.variant_name}
                        </p>
                        <p className="text-sm text-muted-foreground">{a.action}</p>
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="text-sm tabular-nums">{formatMoney(a.cost_net)}</span>
                      <WatchlistButton companyId={companyId} variantId={a.variant_id} size="sm" />
                      <Button size="sm" variant="outline" asChild>
                        <Link href={`/costos/productos/${a.variant_id}?company_id=${companyId}`}>
                          Ver
                        </Link>
                      </Button>
                    </div>
                  </div>
                ))
              )}
            </CardContent>
          </Card>
        </>
      ) : null}
    </div>
  )
}
