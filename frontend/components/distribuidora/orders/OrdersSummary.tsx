"use client"

import { useMemo } from "react"
import type { DistribuidoraPurchaseOrder } from "@/lib/api"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"

function formatClp(value: number | null | undefined): string {
  const n = Number(value)
  if (!Number.isFinite(n)) return "$0"
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  }).format(n)
}

type AggRow = { key: string; clientCount: number; totalAmount: number }

function aggregateByKey(
  items: DistribuidoraPurchaseOrder[],
  keyFn: (row: DistribuidoraPurchaseOrder) => string,
): AggRow[] {
  const map = new Map<string, { clients: Set<number>; sum: number }>()
  for (const row of items) {
    const key = keyFn(row)
    let g = map.get(key)
    if (!g) {
      g = { clients: new Set(), sum: 0 }
      map.set(key, g)
    }
    const cid = row.client_id
    if (cid != null && Number.isFinite(Number(cid))) {
      g.clients.add(Number(cid))
    }
    const amt = Number(row.total_amount)
    if (Number.isFinite(amt)) g.sum += amt
  }
  return Array.from(map.entries())
    .map(([key, v]) => ({
      key,
      clientCount: v.clients.size,
      totalAmount: v.sum,
    }))
    .sort((a, b) => b.totalAmount - a.totalAmount)
}

type OrdersSummaryProps = {
  items: DistribuidoraPurchaseOrder[]
}

export function OrdersSummary({ items }: OrdersSummaryProps) {
  const bySeller = useMemo(
    () =>
      aggregateByKey(items, (row) => {
        const s = row.seller?.trim()
        if (s) return s
        if (row.user_id != null) return `Usuario ${row.user_id}`
        return "Sin vendedor"
      }),
    [items],
  )

  const byMunicipality = useMemo(
    () =>
      aggregateByKey(items, (row) => {
        const m = row.municipality?.trim()
        if (m) return m
        const c = row.city?.trim()
        if (c) return c
        return "Sin comuna / ciudad"
      }),
    [items],
  )

  if (items.length === 0) {
    return (
      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Resumen por vendedor</CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-muted-foreground">
            Sin datos para los filtros actuales.
          </CardContent>
        </Card>
        <Card>
          <CardHeader className="pb-2">
            <CardTitle className="text-base">Resumen por ciudad / comuna</CardTitle>
          </CardHeader>
          <CardContent className="text-sm text-muted-foreground">
            Sin datos para los filtros actuales.
          </CardContent>
        </Card>
      </div>
    )
  }

  return (
    <div className="grid gap-4 lg:grid-cols-2">
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Resumen por vendedor</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 text-sm">
          {bySeller.map((r) => (
            <div
              key={r.key}
              className="flex flex-wrap items-baseline justify-between gap-x-2 gap-y-1 border-b border-border/60 py-2 last:border-0"
            >
              <span className="font-medium text-foreground">{r.key}</span>
              <span className="text-muted-foreground">
                {r.clientCount} cliente{r.clientCount === 1 ? "" : "s"} →{" "}
                <span className="font-semibold tabular-nums text-foreground">
                  {formatClp(r.totalAmount)}
                </span>
              </span>
            </div>
          ))}
        </CardContent>
      </Card>
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Resumen por ciudad / comuna</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 text-sm">
          {byMunicipality.map((r) => (
            <div
              key={r.key}
              className="flex flex-wrap items-baseline justify-between gap-x-2 gap-y-1 border-b border-border/60 py-2 last:border-0"
            >
              <span className="font-medium text-foreground">{r.key}</span>
              <span className="text-muted-foreground">
                {r.clientCount} cliente{r.clientCount === 1 ? "" : "s"} →{" "}
                <span className="font-semibold tabular-nums text-foreground">
                  {formatClp(r.totalAmount)}
                </span>
              </span>
            </div>
          ))}
        </CardContent>
      </Card>
    </div>
  )
}
