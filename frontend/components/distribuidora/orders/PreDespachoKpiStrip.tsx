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
import type { PurchaseInvoiceStatusFilter } from "@/lib/purchase-invoice-status"

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
  active?: boolean
  onClick?: () => void
}

function KpiCard({
  label,
  value,
  sub,
  icon: Icon,
  accent = "default",
  loading,
  active,
  onClick,
}: KpiCardProps) {
  const accentBorder = {
    default: "border-lq-line",
    muted: "border-lq-line",
    green: "border-emerald-200/80 dark:border-emerald-900/60",
    yellow: "border-amber-200/80 dark:border-amber-900/60",
    slate: "border-lq-line",
  }[accent]

  const accentIcon = {
    default: "text-lq-faint",
    muted: "text-lq-faint",
    green: "text-emerald-600 dark:text-emerald-400",
    yellow: "text-amber-600 dark:text-amber-400",
    slate: "text-lq-faint",
  }[accent]

  const Wrapper = onClick ? "button" : "div"

  return (
    <Wrapper
      type={onClick ? "button" : undefined}
      onClick={onClick}
      className={cn(
        "flex min-h-[104px] flex-col gap-1 rounded-xl border bg-lq-surface p-4 text-left shadow-lq-xs transition-colors",
        accentBorder,
        onClick && "hover:border-lq-faint focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/30",
        active && "ring-2 ring-primary border-primary",
      )}
    >
      <div className="flex items-center justify-between gap-2">
        <span className="text-xs font-medium uppercase tracking-wide text-lq-muted">
          {label}
        </span>
        <Icon className={cn("size-4 shrink-0", accentIcon)} aria-hidden />
      </div>
      <p className="text-2xl font-semibold tabular-nums tracking-tight text-lq-ink">
        {loading ? "—" : value}
      </p>
      {sub ? (
        <p className="text-xs leading-snug text-lq-faint">{sub}</p>
      ) : null}
    </Wrapper>
  )
}

type PreDespachoKpiStripProps = {
  stats: PreDespachoOperationalStats
  loading?: boolean
  estadoResumen?: PurchaseInvoiceStatusFilter
  onEstadoResumenChange?: (filter: PurchaseInvoiceStatusFilter) => void
}

const EMPTY_STATS: PreDespachoOperationalStats = {
  totalOrders: 0,
  totalAmount: 0,
  pending: 0,
  invoiced: 0,
  probable: 0,
}

export function PreDespachoKpiStrip({
  stats,
  loading,
  estadoResumen,
  onEstadoResumenChange,
}: PreDespachoKpiStripProps) {
  const s = stats ?? EMPTY_STATS
  return (
    <div className="grid grid-cols-2 gap-2 sm:gap-3 lg:grid-cols-5">
      <KpiCard
        label="Total órdenes"
        value={s.totalOrders.toLocaleString("es-CL")}
        icon={ClipboardList}
        loading={loading}
      />
      <KpiCard
        label="Total monto"
        value={formatClp(s.totalAmount)}
        icon={CircleDollarSign}
        loading={loading}
      />
      <KpiCard
        label="Pendientes"
        value={s.pending.toLocaleString("es-CL")}
        sub="Sin factura confirmada"
        icon={Timer}
        accent="slate"
        loading={loading}
        active={estadoResumen === "pending"}
        onClick={
          onEstadoResumenChange && !loading
            ? () => onEstadoResumenChange("pending")
            : undefined
        }
      />
      <KpiCard
        label="Probables"
        value={s.probable.toLocaleString("es-CL")}
        sub="Heurística operacional"
        icon={HelpCircle}
        accent="yellow"
        loading={loading}
        active={estadoResumen === "probable"}
        onClick={
          onEstadoResumenChange && !loading
            ? () => onEstadoResumenChange("probable")
            : undefined
        }
      />
      <KpiCard
        label="Facturadas"
        value={s.invoiced.toLocaleString("es-CL")}
        sub="Relación confirmada"
        icon={FileCheck2}
        accent="green"
        loading={loading}
        active={estadoResumen === "confirmed"}
        onClick={
          onEstadoResumenChange && !loading
            ? () => onEstadoResumenChange("confirmed")
            : undefined
        }
      />
    </div>
  )
}
