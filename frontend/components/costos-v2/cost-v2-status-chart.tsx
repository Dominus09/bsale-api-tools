"use client"

import { Cell, Pie, PieChart } from "recharts"

import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart"
import { buildStatusChartData } from "@/lib/costos-v2/labels"

const config = {
  count: { label: "Recepciones", color: "hsl(215 35% 40%)" },
} satisfies ChartConfig

const COLORS = [
  "hsl(38 70% 48%)",
  "hsl(215 16% 47%)",
  "hsl(0 0% 55%)",
  "hsl(160 35% 36%)",
  "hsl(25 70% 45%)",
  "hsl(350 55% 48%)",
  "hsl(200 30% 45%)",
]

export function CostV2StatusChart({
  byStatus,
}: {
  byStatus: Record<string, number> | null | undefined
}) {
  const data = buildStatusChartData(byStatus).filter((d) => d.count > 0)
  if (!data.length) {
    return (
      <p className="py-8 text-center text-sm text-muted-foreground">
        Sin distribución por estado en el rango.
      </p>
    )
  }
  return (
    <ChartContainer config={config} className="mx-auto h-[220px] w-full max-w-sm">
      <PieChart>
        <ChartTooltip content={<ChartTooltipContent nameKey="label" />} />
        <Pie
          data={data}
          dataKey="count"
          nameKey="label"
          innerRadius={44}
          outerRadius={76}
          paddingAngle={2}
        >
          {data.map((_, i) => (
            <Cell key={data[i].status} fill={COLORS[i % COLORS.length]} />
          ))}
        </Pie>
      </PieChart>
    </ChartContainer>
  )
}
