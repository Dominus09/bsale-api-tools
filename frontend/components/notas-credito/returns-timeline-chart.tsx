"use client"

import { Bar, BarChart, CartesianGrid, XAxis, YAxis } from "recharts"

import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart"
import { formatCLP } from "@/components/notas-credito/format"

type Point = { bucket: string; quantity: number; amount: number }

const chartConfig = {
  amount: { label: "Monto NC", color: "hsl(0 72% 45%)" },
  quantity: { label: "Cantidad", color: "hsl(215 20% 45%)" },
} satisfies ChartConfig

export function ReturnsTimelineChart({ data }: { data: Point[] }) {
  if (!data.length) {
    return (
      <p className="py-12 text-center text-sm text-muted-foreground">
        Sin datos en el período seleccionado.
      </p>
    )
  }

  const chartData = data.map((d) => ({
    ...d,
    label: d.bucket.slice(0, 10),
  }))

  return (
    <ChartContainer config={chartConfig} className="h-[320px] w-full">
      <BarChart data={chartData} margin={{ left: 8, right: 8, top: 8, bottom: 0 }}>
        <CartesianGrid vertical={false} />
        <XAxis dataKey="label" tickLine={false} axisLine={false} fontSize={11} />
        <YAxis
          tickLine={false}
          axisLine={false}
          fontSize={11}
          tickFormatter={(v) => `${Math.round(Number(v) / 1000)}k`}
        />
        <ChartTooltip
          content={
            <ChartTooltipContent
              formatter={(value, name) =>
                name === "amount" ? formatCLP(Number(value)) : String(value)
              }
            />
          }
        />
        <Bar dataKey="amount" fill="var(--color-amount)" radius={[4, 4, 0, 0]} />
      </BarChart>
    </ChartContainer>
  )
}
