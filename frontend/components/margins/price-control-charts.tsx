"use client"

import { Bar, BarChart, CartesianGrid, Cell, Pie, PieChart, XAxis, YAxis } from "recharts"

import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart"
import { formatMoneyCLP } from "@/lib/costos/format"

const barConfig = {
  pct: { label: "% dentro de política", color: "hsl(160 45% 35%)" },
} satisfies ChartConfig

const statusConfig = {
  count: { label: "Combinaciones", color: "hsl(215 55% 42%)" },
} satisfies ChartConfig

const STATUS_COLORS = [
  "hsl(160 45% 35%)",
  "hsl(0 65% 45%)",
  "hsl(38 90% 45%)",
  "hsl(215 20% 55%)",
  "hsl(25 85% 45%)",
  "hsl(280 45% 45%)",
  "hsl(320 50% 45%)",
  "hsl(200 40% 45%)",
  "hsl(45 80% 40%)",
]

export function ComplianceByDimensionChart({
  data,
  emptyMessage = "Sin datos de cumplimiento.",
}: {
  data: { name: string; pct: number; total: number }[]
  emptyMessage?: string
}) {
  if (!data.length) {
    return <p className="py-10 text-center text-sm text-muted-foreground">{emptyMessage}</p>
  }
  return (
    <ChartContainer config={barConfig} className="h-[240px] w-full">
      <BarChart data={data} margin={{ left: 4, right: 8, top: 8, bottom: 24 }}>
        <CartesianGrid vertical={false} />
        <XAxis
          dataKey="name"
          tickLine={false}
          axisLine={false}
          fontSize={10}
          interval={0}
          angle={-25}
          textAnchor="end"
          height={50}
        />
        <YAxis
          domain={[0, 100]}
          tickLine={false}
          axisLine={false}
          fontSize={11}
          tickFormatter={(v) => `${v}%`}
        />
        <ChartTooltip
          content={
            <ChartTooltipContent
              formatter={(value, _name, item) => {
                const total = (item?.payload as { total?: number })?.total
                return `${Number(value).toFixed(1)}% (${total ?? "—"} filas)`
              }}
            />
          }
        />
        <Bar dataKey="pct" fill="var(--color-pct)" radius={4} />
      </BarChart>
    </ChartContainer>
  )
}

export function StatusDistributionChart({
  data,
}: {
  data: { status: string; label: string; count: number }[]
}) {
  if (!data.some((d) => d.count > 0)) {
    return (
      <p className="py-10 text-center text-sm text-muted-foreground">Sin distribución.</p>
    )
  }
  return (
    <ChartContainer config={statusConfig} className="mx-auto h-[240px] w-full max-w-md">
      <PieChart>
        <ChartTooltip content={<ChartTooltipContent nameKey="label" />} />
        <Pie
          data={data}
          dataKey="count"
          nameKey="label"
          innerRadius={48}
          outerRadius={80}
          paddingAngle={2}
        >
          {data.map((_, i) => (
            <Cell key={i} fill={STATUS_COLORS[i % STATUS_COLORS.length]} />
          ))}
        </Pie>
      </PieChart>
    </ChartContainer>
  )
}

export function TopBelowMinimumList({
  items,
  onSelect,
}: {
  items: {
    key: string
    productName: string | null
    variantName: string | null
    priceListName: string | null
    adjustment: number
    markup: number | null
  }[]
  onSelect?: (key: string) => void
}) {
  if (!items.length) {
    return (
      <p className="py-8 text-center text-sm text-muted-foreground">
        No hay precios bajo el mínimo con ajuste calculable.
      </p>
    )
  }
  return (
    <ul className="space-y-2">
      {items.map((it) => (
        <li key={it.key}>
          <button
            type="button"
            className="flex w-full items-start justify-between gap-3 rounded-md border border-border/60 px-3 py-2 text-left text-sm hover:bg-muted/40"
            onClick={() => onSelect?.(it.key)}
          >
            <span className="min-w-0">
              <span className="block truncate font-medium">
                {it.productName || "Producto"}
                {it.variantName ? ` — ${it.variantName}` : ""}
              </span>
              <span className="text-xs text-muted-foreground">
                {it.priceListName || "Lista"}
                {it.markup != null ? ` · Recargo ${it.markup.toFixed(1)}%` : ""}
              </span>
            </span>
            <span className="shrink-0 tabular-nums text-red-700 dark:text-red-300">
              +{formatMoneyCLP(it.adjustment)}
            </span>
          </button>
        </li>
      ))}
    </ul>
  )
}
