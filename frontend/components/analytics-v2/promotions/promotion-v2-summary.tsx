"use client"

import { Loader2 } from "lucide-react"

import { cn } from "@/lib/utils"
import {
  APPROVAL_LABELS,
  KIND_LABELS,
  TIMELINE_LABELS,
  VERIFICATION_LABELS,
  monthLabel,
} from "@/lib/analytics-v2/promotions-labels"
import type {
  CampaignApproval,
  CampaignKind,
  CampaignTimeline,
  PriceVerification,
  PromotionSection,
  PromotionsSummary,
} from "@/lib/analytics-v2/promotions-types"

type Kpi = { key: string; label: string; value: number | undefined; hint: string; target: PromotionSection | "resumen"; tone?: string }

export function PromotionV2Kpis({
  summary,
  loading,
  onOpen,
}: {
  summary: PromotionsSummary | null
  loading: boolean
  onOpen: (section: PromotionSection | "resumen") => void
}) {
  const k = summary?.kpis
  const kpis: Kpi[] = [
    { key: "vig", label: "Campañas vigentes", value: k?.campaigns_vigentes, hint: "Aprobadas y en fecha hoy", target: "vigentes" },
    { key: "off", label: "Productos en oferta", value: k?.products_on_offer, hint: "Variantes en ofertas vigentes", target: "vigentes" },
    { key: "rem", label: "Productos en remate", value: k?.products_on_clearance, hint: "Variantes en remates vigentes", target: "remates" },
    { key: "fin", label: "Campañas finalizadas", value: k?.campaigns_finished_month, hint: "Terminadas en el mes", target: "historial" },
    {
      key: "ver",
      label: "Excepciones verificadas",
      value: k?.exceptions_verified,
      hint: "Precio promo = precio Bsale",
      target: "vigentes",
      tone: "text-emerald-700 dark:text-emerald-300",
    },
    {
      key: "pen",
      label: "Excepciones pendientes",
      value: k?.exceptions_pending,
      hint: "Sin aprobar, sin verificar o en conflicto",
      target: "pendientes",
      tone: (k?.exceptions_pending ?? 0) > 0 ? "text-orange-700 dark:text-orange-300" : undefined,
    },
  ]
  return (
    <div className="grid grid-cols-2 gap-2 md:grid-cols-3 xl:grid-cols-6">
      {kpis.map((kpi) => (
        <button
          key={kpi.key}
          type="button"
          onClick={() => onOpen(kpi.target)}
          className="rounded-md border border-border/70 bg-card px-3 py-2.5 text-left transition-colors hover:bg-muted/40"
        >
          <p className="text-[11px] font-medium uppercase tracking-wide text-muted-foreground">{kpi.label}</p>
          <p className={cn("mt-0.5 text-2xl font-semibold tabular-nums", kpi.tone)}>
            {loading && kpi.value === undefined ? <Loader2 className="h-5 w-5 animate-spin" /> : (kpi.value ?? "—")}
          </p>
          <p className="text-[11px] text-muted-foreground">{kpi.hint}</p>
        </button>
      ))}
    </div>
  )
}

function Breakdown<K extends string>({ title, data, labels }: { title: string; data: Partial<Record<K, number>>; labels: Record<K, string> }) {
  const entries = Object.entries(data) as [K, number][]
  return (
    <div className="rounded-md border p-3">
      <p className="mb-2 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">{title}</p>
      {entries.length ? (
        <ul className="space-y-1 text-sm">
          {entries.map(([key, n]) => (
            <li key={key} className="flex justify-between gap-2">
              <span>{labels[key] ?? key}</span>
              <span className="tabular-nums font-medium">{n}</span>
            </li>
          ))}
        </ul>
      ) : (
        <p className="text-sm text-muted-foreground">Sin registros.</p>
      )}
    </div>
  )
}

export function PromotionV2MonthOverview({ summary }: { summary: PromotionsSummary | null }) {
  if (!summary) return null
  return (
    <div className="space-y-3">
      <p className="text-sm text-muted-foreground">
        Campañas que se cruzan con <span className="font-medium capitalize text-foreground">{monthLabel(summary.month)}</span>{" "}
        ({summary.month_from} a {summary.month_to}). Pendientes de aprobación: {summary.kpis.campaigns_pending_approval} · Próximas:{" "}
        {summary.kpis.campaigns_upcoming}.
      </p>
      <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-4">
        <Breakdown<CampaignKind> title="Por tipo" data={summary.month_by_kind} labels={KIND_LABELS} />
        <Breakdown<CampaignApproval> title="Por aprobación" data={summary.month_by_approval} labels={APPROVAL_LABELS} />
        <Breakdown<CampaignTimeline> title="Por vigencia" data={summary.month_by_timeline} labels={TIMELINE_LABELS} />
        <Breakdown<PriceVerification> title="Verificación contra Bsale (hoy)" data={summary.verification_live} labels={VERIFICATION_LABELS} />
      </div>
    </div>
  )
}
