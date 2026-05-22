"use client"

import {
  CircleDollarSign,
  ClipboardList,
  FileCheck2,
  HelpCircle,
  Timer,
} from "lucide-react"
import { cn } from "@/lib/utils"
import type { PreDespachoOperationalStats } from "@/lib/pre-despacho-stats"

function formatClp(n: number): string {
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  }).format(Number.isFinite(n) ? n : 0)
}

type KpiCardProps = {
  label: string
  value: string
  sub?: string
  icon: React.ElementType
  accent?: "default" | "muted" | "green" | "yellow" | "slate"
  loading?: boolean
}

function KpiCard({
  label,
  value,
  sub,
  icon: Icon,
  accent = "default",
  loading,
}: KpiCardProps) {
  const accentBorder = {
    default: "border-border/70",
    muted: "border-border/70",
    green: "border-emerald-200/80 dark:border-emerald-900/60",
    yellow: "border-amber-200/80 dark:border-amber-900/60",
    slate: "border-slate-200/80 dark:border-slate-800/80",
  }[accent]

  const accentIcon = {
    default: "text-muted-foreground",
    muted: "text-muted-foreground",
    green: "text-emerald-600 dark:text-emerald-400",
    yellow: "text-amber-600 dark:text-amber-400",
    slate: "text-slate-500 dark:text-slate-400",
  }[accent]

  return (
    <div
      className={cn(
        "flex min-w-[9.5rem] flex-1 flex-col gap-1 rounded-lg border bg-card/80 px-3.5 py-3 shadow-sm",
        accentBorder,
      )}
    >
      <div className="flex items-center justify-between gap-2">
        <span className="text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
          {label}
        </span>
        <Icon className={cn("size-3.5 shrink-0", accentIcon)} aria-hidden />
      </div>
      <p className="text-xl font-semibold tabular-nums tracking-tight text-foreground">
        {loading ? "—" : value}
      </p>
      {sub ? (
        <p className="text-[11px] leading-snug text-muted-foreground">{sub}</p>
      ) : null}
    </div>
  )
}

type PreDespachoKpiStripProps = {
  stats: PreDespachoOperationalStats
  loading?: boolean
}

export function PreDespachoKpiStrip({ stats, loading }: PreDespachoKpiStripProps) {
  return (
    <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-5">
      <KpiCard
        label="Total órdenes"
        value={stats.totalOrders.toLocaleString("es-CL")}
        icon={ClipboardList}
        loading={loading}
      />
      <KpiCard
        label="Total monto"
        value={formatClp(stats.totalAmount)}
        icon={CircleDollarSign}
        loading={loading}
      />
      <KpiCard
        label="Pendientes"
        value={stats.pending.toLocaleString("es-CL")}
        sub="Sin factura confirmada"
        icon={Timer}
        accent="slate"
        loading={loading}
      />
      <KpiCard
        label="Probables"
        value={stats.probable.toLocaleString("es-CL")}
        sub="Heurística operacional"
        icon={HelpCircle}
        accent="yellow"
        loading={loading}
      />
      <KpiCard
        label="Facturadas"
        value={stats.invoiced.toLocaleString("es-CL")}
        sub="Relación confirmada"
        icon={FileCheck2}
        accent="green"
        loading={loading}
      />
    </div>
  )
}
