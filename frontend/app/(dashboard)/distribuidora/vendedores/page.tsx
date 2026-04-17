"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import Link from "next/link"
import { Loader2, UserCircle2 } from "lucide-react"

import {
  getDistribuidoraClientsSummarySellers,
  type DistribuidoraClientSellerSummary,
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

/** Filtro rápido por IDs de vendedor Bsale (``seller_id`` en documentos). */
const SELLER_ID_PRESETS = [80, 85, 59, 89] as const

function localIsoDate(d = new Date()): string {
  const y = d.getFullYear()
  const m = String(d.getMonth() + 1).padStart(2, "0")
  const day = String(d.getDate()).padStart(2, "0")
  return `${y}-${m}-${day}`
}

export default function DistribuidoraVendedoresPage() {
  const defaultTo = localIsoDate()
  const defaultFrom = (() => {
    const x = new Date()
    x.setDate(x.getDate() - 30)
    return localIsoDate(x)
  })()

  const [dateFrom, setDateFrom] = useState(defaultFrom)
  const [dateTo, setDateTo] = useState(defaultTo)
  const [selectedIds, setSelectedIds] = useState<number[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [rows, setRows] = useState<DistribuidoraClientSellerSummary[]>([])
  const [totals, setTotals] = useState<{ sellers: number; ventas_total: number } | null>(null)

  const sellerIdsParam = useMemo(
    () => (selectedIds.length ? selectedIds.join(",") : undefined),
    [selectedIds],
  )

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await getDistribuidoraClientsSummarySellers({
        limit: 500,
        start_date: dateFrom,
        end_date: dateTo,
        seller_ids: sellerIdsParam,
      })
      setRows(res.items)
      setTotals(res.totals)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error al cargar")
      setRows([])
      setTotals(null)
    } finally {
      setLoading(false)
    }
  }, [dateFrom, dateTo, sellerIdsParam])

  useEffect(() => {
    void load()
  }, [load])

  const toggleId = (id: number) => {
    setSelectedIds((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]))
  }

  return (
    <div className="mx-auto flex max-w-[1200px] flex-col gap-8 pb-12">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
            Distribuidora
          </p>
          <h1 className="mt-1 text-2xl font-semibold tracking-tight">Control de vendedores</h1>
          <p className="mt-2 max-w-2xl text-sm text-muted-foreground">
            Resumen por vendedor en el período. Filtro opcional por IDs Bsale (documentos).
          </p>
        </div>
        <Button variant="outline" size="sm" asChild>
          <Link href="/distribuidora/dashboard">Dashboard</Link>
        </Button>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-base">Filtros</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <div className="flex flex-wrap gap-4">
            <div className="space-y-1">
              <label className="text-xs font-medium text-muted-foreground">Desde</label>
              <input
                type="date"
                className="rounded-md border border-input bg-background px-3 py-2 text-sm"
                value={dateFrom}
                onChange={(e) => setDateFrom(e.target.value)}
              />
            </div>
            <div className="space-y-1">
              <label className="text-xs font-medium text-muted-foreground">Hasta</label>
              <input
                type="date"
                className="rounded-md border border-input bg-background px-3 py-2 text-sm"
                value={dateTo}
                onChange={(e) => setDateTo(e.target.value)}
              />
            </div>
            <div className="flex items-end">
              <Button type="button" onClick={() => void load()} disabled={loading}>
                Aplicar
              </Button>
            </div>
          </div>
          <div>
            <p className="mb-2 text-xs font-medium text-muted-foreground">Vendedores (ID Bsale)</p>
            <div className="flex flex-wrap gap-2">
              {SELLER_ID_PRESETS.map((id) => (
                <Button
                  key={id}
                  type="button"
                  size="sm"
                  variant={selectedIds.includes(id) ? "default" : "outline"}
                  onClick={() => toggleId(id)}
                >
                  {id}
                </Button>
              ))}
              {selectedIds.length > 0 ? (
                <Button type="button" size="sm" variant="ghost" onClick={() => setSelectedIds([])}>
                  Quitar filtro ID
                </Button>
              ) : null}
            </div>
          </div>
        </CardContent>
      </Card>

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>Error</AlertTitle>
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {loading ? (
        <div className="flex min-h-[200px] items-center justify-center gap-2 text-muted-foreground">
          <Loader2 className="h-8 w-8 animate-spin" />
          <span>Cargando…</span>
        </div>
      ) : (
        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <UserCircle2 className="h-4 w-4" />
              Resumen
            </CardTitle>
            {totals ? (
              <p className="text-sm text-muted-foreground">
                {totals.sellers} vendedores · Total neto período: {formatCLP(totals.ventas_total)}
              </p>
            ) : null}
          </CardHeader>
          <CardContent>
            <div className="max-h-[560px] overflow-auto rounded-md border">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Vendedor</TableHead>
                    <TableHead className="text-right">Ventas (neto)</TableHead>
                    <TableHead className="text-right">Clientes activos</TableHead>
                    <TableHead className="text-right">Ticket prom.</TableHead>
                    <TableHead className="text-right">Clientes inactivos</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {rows.map((r, i) => {
                    const inact = Number(r.clientes_inactivos ?? 0)
                    return (
                      <TableRow
                        key={`${r.seller_name ?? "—"}-${i}`}
                        className={cn(inact >= 5 && "bg-red-50/60 dark:bg-red-950/20")}
                      >
                        <TableCell className="font-medium">
                          {(r.seller_name ?? "").trim() || "—"}
                        </TableCell>
                        <TableCell className="text-right tabular-nums">
                          {formatCLP(Number(r.ventas ?? 0))}
                        </TableCell>
                        <TableCell className="text-right tabular-nums">{r.clientes ?? 0}</TableCell>
                        <TableCell className="text-right tabular-nums">
                          {formatCLP(Number(r.ticket_promedio ?? 0))}
                        </TableCell>
                        <TableCell className="text-right font-medium tabular-nums text-red-700 dark:text-red-300">
                          {inact}
                        </TableCell>
                      </TableRow>
                    )
                  })}
                </TableBody>
              </Table>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
