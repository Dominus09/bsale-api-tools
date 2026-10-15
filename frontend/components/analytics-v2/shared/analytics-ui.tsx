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

export type AnalyticTone = "neutral" | "success" | "warning" | "danger" | "info"

const tones: Record<AnalyticTone, string> = {
  neutral: "border-slate-200 bg-white text-slate-950",
  success: "border-emerald-200 bg-emerald-50/45 text-emerald-950",
  warning: "border-amber-200 bg-amber-50/55 text-amber-950",
  danger: "border-red-200 bg-red-50/50 text-red-950",
  info: "border-blue-200 bg-blue-50/45 text-blue-950",
}

const badgeTones: Record<AnalyticTone, string> = {
  neutral: "border-slate-200 bg-slate-50 text-slate-700",
  success: "border-emerald-200 bg-emerald-50 text-emerald-800",
  warning: "border-amber-200 bg-amber-50 text-amber-800",
  danger: "border-red-200 bg-red-50 text-red-800",
  info: "border-blue-200 bg-blue-50 text-blue-800",
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
    <header className="flex flex-col gap-5 border-b border-slate-200/80 pb-6 lg:flex-row lg:items-end lg:justify-between">
      <div className="min-w-0">
        {eyebrow ? <p className="mb-1.5 text-xs font-semibold uppercase tracking-[0.16em] text-primary">{eyebrow}</p> : null}
        <h1 className="text-[26px] font-semibold leading-tight tracking-[-0.025em] text-slate-950 md:text-[30px]">{title}</h1>
        <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-600 md:text-[15px]">{description}</p>
        {updatedAt ? <div className="mt-2 text-xs text-slate-500">{updatedAt}</div> : null}
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
        <p className="text-[13px] font-medium leading-5 text-slate-600">{label}</p>
        {Icon ? (
          <span className="rounded-lg border border-current/10 bg-white/65 p-2">
            <Icon className="h-4 w-4 opacity-70" aria-hidden="true" />
          </span>
        ) : null}
      </div>
      <div className="mt-3 flex items-baseline gap-2">
        <span className="text-[30px] font-semibold leading-none tracking-[-0.035em] tabular-nums md:text-[34px]">{value}</span>
        {unit ? <span className="text-xs font-medium text-slate-500">{unit}</span> : null}
      </div>
      {context ? <p className="mt-2 min-h-5 text-xs leading-5 text-slate-500">{context}</p> : null}
    </>
  )
  const cls = cn(
    "group relative min-h-[136px] rounded-xl border p-4 text-left shadow-[0_1px_2px_rgba(15,23,42,0.03)] transition",
    tones[tone],
    (href || onClick) && "hover:-translate-y-0.5 hover:shadow-[0_8px_24px_rgba(15,23,42,0.07)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/35",
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
    <section className={cn("overflow-hidden rounded-xl border border-slate-200 bg-white shadow-[0_1px_3px_rgba(15,23,42,0.035)]", className)}>
      {title || description || action ? (
        <div className="flex flex-col gap-2 border-b border-slate-100 px-4 py-4 sm:flex-row sm:items-center sm:justify-between md:px-5">
          <div>
            {title ? <h2 className="text-[17px] font-semibold tracking-[-0.01em] text-slate-950">{title}</h2> : null}
            {description ? <p className="mt-1 text-[13px] leading-5 text-slate-500">{description}</p> : null}
          </div>
          {action ? <div className="shrink-0">{action}</div> : null}
        </div>
      ) : null}
      <div className={cn("p-4 md:p-5", contentClassName)}>{children}</div>
    </section>
  )
}

export function SmartFilterBar({ children, advanced }: { children: ReactNode; advanced?: ReactNode }) {
  return (
    <SectionCard className="overflow-visible" contentClassName="p-3 md:p-4">
      <div className="flex flex-col gap-3">{children}</div>
      {advanced ? (
        <details className="group mt-3 border-t border-slate-100 pt-3">
          <summary className="inline-flex cursor-pointer list-none items-center gap-1.5 text-xs font-medium text-slate-600 hover:text-slate-950 [&::-webkit-details-marker]:hidden">
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
    <div className={cn("relative overflow-x-auto rounded-xl border border-slate-200 bg-white shadow-[0_1px_3px_rgba(15,23,42,0.035)]", className)}>
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
      <span className="rounded-full bg-slate-100 p-3"><Info className="h-5 w-5 text-slate-500" /></span>
      <h3 className="mt-3 text-sm font-semibold text-slate-900">{title}</h3>
      <p className="mt-1 max-w-md text-sm leading-6 text-slate-500">{description}</p>
    </div>
  )
}

export function AnalyticsLoading({ label = "Preparando información…" }: { label?: string }) {
  return (
    <div className="flex min-h-56 flex-col items-center justify-center gap-3 rounded-xl border border-slate-200 bg-white">
      <Loader2 className="h-6 w-6 animate-spin text-primary" />
      <p className="text-sm text-slate-500">{label}</p>
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
    <Link href={href} className="group flex items-center gap-4 rounded-lg px-3 py-3 transition hover:bg-slate-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/30">
      <span className={cn("flex h-10 min-w-10 items-center justify-center rounded-lg border px-2 text-sm font-semibold tabular-nums", badgeTones[tone])}>{count}</span>
      <span className="min-w-0 flex-1">
        <span className="block text-sm font-medium text-slate-900">{label}</span>
        <span className="mt-0.5 block text-xs leading-5 text-slate-500">{description}</span>
      </span>
      <ArrowRight className="h-4 w-4 shrink-0 text-slate-400 transition-transform group-hover:translate-x-0.5 group-hover:text-slate-700" />
    </Link>
  )
}
