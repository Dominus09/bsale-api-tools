"use client"

import { Area, AreaChart, Bar, BarChart, CartesianGrid, XAxis, YAxis } from "recharts"

import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart"
import type { AgeDistribution, CostChartPoint, TopIncreaseItem } from "@/lib/costos/adapt-cost-analytics"
import { formatMoneyCLP, formatPct } from "@/lib/costos/format"

const evolutionConfig = {
  costGross: { label: "Costo bruto", color: "hsl(215 70% 40%)" },
  costNet: { label: "Costo neto", color: "hsl(215 20% 55%)" },
} satisfies ChartConfig

const ageConfig = {
  count: { label: "Variantes", color: "hsl(160 45% 35%)" },
} satisfies ChartConfig

export function CostHistoryChart({
  data,
  emptyMessage = "Sin series de costo bruto en el período.",
}: {
  data: CostChartPoint[]
  emptyMessage?: string
}) {
  if (!data.length) {
    return (
      <p className="py-10 text-center text-sm text-muted-foreground">{emptyMessage}</p>
    )
  }
  return (
    <ChartContainer config={evolutionConfig} className="h-[260px] w-full">
      <AreaChart data={data} margin={{ left: 4, right: 8, top: 8, bottom: 0 }}>
        <CartesianGrid vertical={false} />
        <XAxis dataKey="date" tickLine={false} axisLine={false} fontSize={11} />
        <YAxis
          tickLine={false}
          axisLine={false}
          fontSize={11}
          tickFormatter={(v) => `${Math.round(Number(v) / 1000)}k`}
        />
        <ChartTooltip
          content={
            <ChartTooltipContent
              formatter={(value) => formatMoneyCLP(Number(value))}
            />
          }
        />
        <Area
          type="monotone"
          dataKey="costGross"
          stroke="var(--color-costGross)"
          fill="var(--color-costGross)"
          fillOpacity={0.15}
          strokeWidth={2}
        />
      </AreaChart>
    </ChartContainer>
  )
}

export function CostAgeDistributionChart({ data }: { data: AgeDistribution[] }) {
  const chartData = data.filter((d) => d.bucket !== "unknown")
  if (!chartData.some((d) => d.count > 0)) {
    return (
      <p className="py-10 text-center text-sm text-muted-foreground">
        Sin datos de antigüedad.
      </p>
    )
  }
  return (
    <ChartContainer config={ageConfig} className="h-[260px] w-full">
      <BarChart data={chartData} margin={{ left: 4, right: 8, top: 8, bottom: 0 }}>
        <CartesianGrid vertical={false} />
        <XAxis dataKey="label" tickLine={false} axisLine={false} fontSize={11} />
        <YAxis allowDecimals={false} tickLine={false} axisLine={false} fontSize={11} />
        <ChartTooltip content={<ChartTooltipContent />} />
        <Bar dataKey="count" fill="var(--color-count)" radius={[4, 4, 0, 0]} />
      </BarChart>
    </ChartContainer>
  )
}

export function CostTopIncreasesList({ items }: { items: TopIncreaseItem[] }) {
  if (!items.length) {
    return (
      <p className="py-8 text-center text-sm text-muted-foreground">
        Sin alzas de costo en el conjunto filtrado.
      </p>
    )
  }
  return (
    <ul className="divide-y divide-border/60">
      {items.map((it) => (
        <li
          key={it.variantId}
          className="flex items-center justify-between gap-3 py-2.5 text-sm"
        >
          <span className="min-w-0 truncate font-medium">{it.label}</span>
          <span className="shrink-0 tabular-nums text-red-600 dark:text-red-400">
            {formatPct(it.variationPct)}
          </span>
        </li>
      ))}
    </ul>
  )
}
