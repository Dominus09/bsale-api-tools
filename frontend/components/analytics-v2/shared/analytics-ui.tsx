"use client"

import type { ReactNode } from "react"
import Link from "next/link"
import {
  ArrowRight,
  CheckCircle2,
  ChevronDown,
  CircleAlert,
  Info,
  Loader2,
  type LucideIcon,
} from "lucide-react"

import { cn } from "@/lib/utils"

import { toneBadges as badgeTones, toneDots, toneSurfaces as tones, toneText, type AnalyticTone } from "./tokens"

export type { AnalyticTone }

/**
 * Contenedor raíz de las pantallas de Analítica V2: ancho fluido completo.
 * El layout del dashboard ya aporta el padding lateral (24px en escritorio);
 * aquí no debe existir ningún max-width ni centrado que desperdicie espacio.
 */
export function AnalyticPageShell({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={cn("flex w-full min-w-0 flex-col gap-6 pb-10 xl:gap-7", className)}>{children}</div>
}

export function AnalyticPageHeader({
  title,
  description,
  eyebrow,
  updatedAt,
  actions,
}: {
  title: string
  description: string
  eyebrow?: string
  updatedAt?: ReactNode
  actions?: ReactNode
}) {
  return (
    <header className="flex flex-col gap-5 border-b border-lq-line pb-6 lg:flex-row lg:items-end lg:justify-between">
      <div className="min-w-0">
        {eyebrow ? <p className="mb-2 text-[11px] font-semibold uppercase tracking-[0.18em] text-primary">{eyebrow}</p> : null}
        <h1 className="text-[28px] font-semibold leading-[1.1] tracking-[-0.03em] text-lq-ink md:text-[32px]">{title}</h1>
        <p className="mt-2.5 max-w-3xl text-sm leading-6 text-lq-muted md:text-[15px]">{description}</p>
        {updatedAt ? <div className="mt-2.5 text-xs tabular-nums text-lq-faint">{updatedAt}</div> : null}
      </div>
      {actions ? <div className="flex shrink-0 flex-wrap items-end gap-2">{actions}</div> : null}
    </header>
  )
}


export function MetricCard({
  label,
  value,
  context,
  unit,
  tone = "neutral",
  icon: Icon,
  href,
  onClick,
}: {
  label: string
  value: ReactNode
  context?: ReactNode
  unit?: string
  tone?: AnalyticTone
  icon?: LucideIcon
  href?: string
  onClick?: () => void
}) {
  const body = (
    <>
      <div className="flex items-start justify-between gap-3">
        <p className="flex items-center gap-2 text-[13px] font-medium leading-5 text-lq-muted">
          <span className={cn("h-1.5 w-1.5 shrink-0 rounded-full", toneDots[tone])} aria-hidden="true" />
          {label}
        </p>
        {Icon ? <Icon className="h-4 w-4 shrink-0 text-lq-faint transition group-hover:text-lq-faint" aria-hidden="true" /> : null}
      </div>
      <div className="mt-3 flex items-baseline gap-2">
        <span className="text-metric font-semibold tabular-nums tracking-[-0.04em] text-lq-ink md:text-metric-lg">{value}</span>
        {unit ? <span className="text-xs font-medium text-lq-faint">{unit}</span> : null}
      </div>
      {context ? <p className="mt-2 min-h-5 text-xs leading-5 text-lq-faint">{context}</p> : null}
    </>
  )
  const cls = cn(
    "group relative min-h-[132px] rounded-card border border-lq-line bg-lq-surface p-4 text-left shadow-lq-xs transition-colors duration-200 md:p-5",
    (href || onClick) && "hover:border-lq-faint focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/30",
  )
  if (href) return <Link href={href} className={cls}>{body}</Link>
  if (onClick) return <button type="button" onClick={onClick} className={cn(cls, "w-full")}>{body}</button>
  return <div className={cls}>{body}</div>
}

export function StatusBadge({
  children,
  tone = "neutral",
  className,
}: {
  children: ReactNode
  tone?: AnalyticTone
  className?: string
}) {
  return (
    <span className={cn("inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-medium leading-none", badgeTones[tone], className)}>
      {children}
    </span>
  )
}

export function DataStatusBar({
  status,
  title,
  summary,
  children,
}: {
  status: AnalyticTone
  title: string
  summary: ReactNode
  children?: ReactNode
}) {
  const Icon = status === "success" ? CheckCircle2 : status === "danger" ? CircleAlert : Info
  return (
    <details className={cn("group rounded-xl border shadow-[0_1px_2px_rgba(15,23,42,0.025)]", tones[status])}>
      <summary className="flex cursor-pointer list-none items-center gap-3 px-4 py-3.5 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/30 [&::-webkit-details-marker]:hidden">
        <Icon className="h-4 w-4 shrink-0 opacity-80" />
        <div className="min-w-0 flex-1">
          <span className="text-sm font-semibold">{title}</span>
          <span className="ml-2 text-xs opacity-70">{summary}</span>
        </div>
        {children ? <span className="hidden text-xs font-medium opacity-70 sm:inline">Ver criterios</span> : null}
        <ChevronDown className="h-4 w-4 shrink-0 opacity-60 transition-transform group-open:rotate-180" />
      </summary>
      {children ? <div className="border-t border-current/10 px-4 py-4 text-sm leading-6">{children}</div> : null}
    </details>
  )
}

export function SectionCard({
  title,
  description,
  action,
  children,
  className,
  contentClassName,
}: {
  title?: string
  description?: string
  action?: ReactNode
  children: ReactNode
  className?: string
  contentClassName?: string
}) {
  return (
    <section className={cn("overflow-hidden rounded-xl border border-lq-line bg-lq-surface shadow-lq-xs", className)}>
      {title || description || action ? (
        <div className="flex flex-col gap-2 border-b border-lq-line-soft px-4 py-4 sm:flex-row sm:items-center sm:justify-between md:px-6">
          <div>
            {title ? <h2 className="text-base font-semibold tracking-[-0.01em] text-lq-ink">{title}</h2> : null}
            {description ? <p className="mt-1 text-[13px] leading-5 text-lq-muted">{description}</p> : null}
          </div>
          {action ? <div className="shrink-0">{action}</div> : null}
        </div>
      ) : null}
      <div className={cn("p-4 md:p-6", contentClassName)}>{children}</div>
    </section>
  )
}


export function SmartFilterBar({ children, advanced }: { children: ReactNode; advanced?: ReactNode }) {
  return (
    <SectionCard className="overflow-visible" contentClassName="p-3 md:p-4">
      <div className="flex flex-col gap-3">{children}</div>
      {advanced ? (
        <details className="group mt-3 border-t border-lq-line-soft pt-3">
          <summary className="inline-flex cursor-pointer list-none items-center gap-1.5 text-xs font-medium text-lq-body hover:text-lq-ink [&::-webkit-details-marker]:hidden">
            Filtros avanzados
            <ChevronDown className="h-3.5 w-3.5 transition-transform group-open:rotate-180" />
          </summary>
          <div className="mt-3">{advanced}</div>
        </details>
      ) : null}
    </SectionCard>
  )
}

export function ProfessionalDataTable({
  children,
  loading,
  className,
}: {
  children: ReactNode
  loading?: boolean
  className?: string
}) {
  return (
    <div className={cn("relative overflow-x-auto rounded-xl border border-lq-line bg-white shadow-[0_1px_3px_rgba(15,23,42,0.035)]", className)}>
      {loading ? <div className="pointer-events-none absolute inset-0 z-10 bg-white/55 backdrop-blur-[1px]" /> : null}
      {children}
    </div>
  )
}

export function EmptyState({
  title,
  description,
}: {
  title: string
  description: string
}) {
  return (
    <div className="flex min-h-52 flex-col items-center justify-center px-6 text-center">
      <span className="rounded-full bg-lq-line-soft p-3"><Info className="h-5 w-5 text-lq-muted" /></span>
      <h3 className="mt-3 text-sm font-semibold text-lq-ink">{title}</h3>
      <p className="mt-1 max-w-md text-sm leading-6 text-lq-muted">{description}</p>
    </div>
  )
}

export function AnalyticsLoading({ label = "Preparando información…" }: { label?: string }) {
  return (
    <div className="flex min-h-56 flex-col items-center justify-center gap-3 rounded-xl border border-lq-line bg-white">
      <Loader2 className="h-6 w-6 animate-spin text-primary" />
      <p className="text-sm text-lq-muted">{label}</p>
    </div>
  )
}

export function AttentionRow({
  label,
  count,
  description,
  href,
  tone = "warning",
}: {
  label: string
  count: number
  description: string
  href: string
  tone?: AnalyticTone
}) {
  return (
    <Link href={href} className="group flex items-center gap-4 px-4 py-4 transition-colors hover:bg-lq-canvas focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-primary/30 md:px-5">
      <span className={cn("min-w-14 text-[26px] font-semibold leading-none tabular-nums tracking-[-0.03em]", toneText[tone])}>{count}</span>
      <span className="min-w-0 flex-1">
        <span className="block text-sm font-medium text-lq-ink">{label}</span>
        <span className="mt-0.5 block text-xs leading-5 text-lq-muted">{description}</span>
      </span>
      <ArrowRight className="h-4 w-4 shrink-0 text-lq-faint transition-transform group-hover:translate-x-0.5 group-hover:text-lq-body" />
    </Link>
  )
}
