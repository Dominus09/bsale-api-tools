"use client"

import type { ReactNode } from "react"
import Link from "next/link"
import { AlertTriangle, Boxes, CircleDollarSign, Info, ReceiptText, ShieldAlert, TrendingDown, Warehouse } from "lucide-react"

import { ReliabilityBadge } from "@/components/analytics-v2/costs/badges"
import { AttentionRow, MetricCard, SectionCard, StatusBadge } from "@/components/analytics-v2/shared/analytics-ui"
import { cn } from "@/lib/utils"
import {
  BLOCK_REASON_LABELS,
  DATA_CLASS_HINTS,
  DATA_CLASS_LABELS,
  DATA_CLASS_TONES,
  FRESHNESS_LABELS,
  FRESHNESS_TONES,
  RAW_RESOURCE_LABELS,
} from "@/lib/analytics-v2/dashboard-labels"
import type {
  Block,
  CompactMarginRow,
  Count,
  DashboardCosts,
  DashboardFreshness,
  DashboardMargins,
  DashboardPacks,
  DashboardPromotions,
  DashboardResponse,
  DashboardTaxes,
  DataClass,
  FreshnessStatus,
} from "@/lib/analytics-v2/dashboard-types"
import {
  ANOMALY_LABELS,
  RELIABILITY_LABELS,
  TAX_ISSUE_LABELS,
  TAX_LABELS,
  TAX_VALIDATION_LABELS,
  formatDate,
  formatDateTime,
  formatMoney,
  formatPct,
} from "@/lib/analytics-v2/labels"
import { LIST_SCOPE_LABELS, MARGIN_RISK_LABELS, MARGIN_STATUS_LABELS, MARGIN_STATUS_TONES } from "@/lib/analytics-v2/margins-labels"
import type { PriceListScopeStatus } from "@/lib/analytics-v2/margins-types"
import { PACK_PROBLEM_LABELS, PACK_STATUS_LABELS } from "@/lib/analytics-v2/packs-labels"
import type { PackCostStatus, PackProblem } from "@/lib/analytics-v2/packs-types"
import { withQuery } from "@/lib/analytics-v2/url"
import type { AnomalyCode, CostReliability } from "@/lib/analytics-v2/types"

const chip = "inline-flex items-center whitespace-nowrap rounded-md border px-1.5 py-0.5 text-[11px] font-medium"
const sectionTitle = "text-[11px] font-medium uppercase tracking-wide text-muted-foreground"

export const MARGINS_PATH = "/analitica-v2/margenes"
export const COSTS_PATH = "/analitica-v2/costos"
export const PROMOTIONS_PATH = "/analitica-v2/promociones"
export const PACKS_PATH = "/analitica-v2/packs"

export function DataClassBadge({ value }: { value: DataClass }) {
  return (
    <span className={cn(chip, DATA_CLASS_TONES[value])} title={DATA_CLASS_HINTS[value]}>
      {DATA_CLASS_LABELS[value]}
    </span>
  )
}

export function FreshnessBadge({ value }: { value: FreshnessStatus }) {
  return <span className={cn(chip, FRESHNESS_TONES[value])}>{FRESHNESS_LABELS[value]}</span>
}

/** Placeholder para bloques sin permiso o que fallaron; el resto del dashboard sigue visible. */
export function BlockState<T>({ title, block, children }: { title: string; block: Block<T>; children: (data: T) => ReactNode }) {
  if (block.status === "ok" && block.data) return <>{children(block.data)}</>
  const forbidden = block.status === "sin_permiso"
  return (
    <section className="rounded-md border border-dashed p-4">
      <p className={sectionTitle}>{title}</p>
      <p className={cn("mt-1 flex items-center gap-1.5 text-sm", forbidden ? "text-muted-foreground" : "text-red-700")}>
        {forbidden ? <Info className="h-4 w-4" /> : <AlertTriangle className="h-4 w-4" />}
        {BLOCK_REASON_LABELS[block.reason ?? ""] ?? "No disponible."}
      </p>
    </section>
  )
}

export function DashboardExecutiveSummary({ data }: { data: DashboardResponse }) {
  const margins = data.margins.status === "ok" ? data.margins.data : null
  const taxes = data.taxes.status === "ok" ? data.taxes.data : null
  const packs = data.packs.status === "ok" ? data.packs.data : null
  const noCost = margins?.by_status.sin_costo
  const taxReview = margins?.by_status.requiere_revision_tributaria
  return (
    <section aria-labelledby="executive-title">
      <div className="mb-3 flex flex-wrap items-end justify-between gap-2">
        <div>
          <h2 id="executive-title" className="text-[19px] font-semibold tracking-[-0.015em] text-slate-950">Panorama comercial</h2>
          <p className="mt-1 text-[13px] text-slate-500">Una lectura rápida del alcance y los principales riesgos actuales.</p>
        </div>
        {margins ? (
          <p className="text-xs text-slate-500">
            <strong className="font-semibold text-slate-700">{margins.totals.skus}</strong> SKUs únicos ·{" "}
            <strong className="font-semibold text-slate-700">{margins.totals.combinations}</strong> combinaciones SKU × lista
          </p>
        ) : null}
      </div>
      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-6">
        <MetricCard
          label="SKUs únicos evaluados"
          value={margins?.totals.skus ?? "—"}
          context={margins ? `${margins.totals.combinations} combinaciones en ${margins.totals.price_lists} listas` : "Márgenes no disponibles"}
          icon={Warehouse}
          href={MARGINS_PATH}
          tone="info"
        />
        <MetricCard
          label="Productos bajo política"
          value={margins?.alerts.skus ?? "—"}
          context={margins ? `${margins.alerts.combinations} combinaciones afectadas` : "Márgenes no disponibles"}
          icon={TrendingDown}
          href={withQuery(MARGINS_PATH, { status: "bajo_politica", sort: "gap_asc" })}
          tone={margins?.alerts.skus ? "danger" : "success"}
        />
        <MetricCard
          label="Venta bajo costo"
          value={margins?.by_risk.venta_bajo_costo?.skus ?? "—"}
          context={margins ? `${margins.by_risk.venta_bajo_costo?.combinations ?? 0} combinaciones SKU × lista` : "Márgenes no disponibles"}
          icon={CircleDollarSign}
          href={withQuery(MARGINS_PATH, { sort: "markup_asc" })}
          tone={margins?.by_risk.venta_bajo_costo?.skus ? "danger" : "success"}
        />
        <MetricCard
          label="Sin costo utilizable"
          value={noCost?.skus ?? "—"}
          context={noCost ? `${noCost.combinations} combinaciones no evaluables` : "Sin información disponible"}
          icon={ReceiptText}
          href={withQuery(MARGINS_PATH, { status: "sin_costo" })}
          tone={noCost?.skus ? "warning" : "success"}
        />
        <MetricCard
          label="Revisión tributaria"
          value={taxReview?.skus ?? taxes?.skus_tax_review ?? "—"}
          context="IVA/ILA pendientes de revisión"
          icon={ShieldAlert}
          href={withQuery(MARGINS_PATH, { status: "requiere_revision_tributaria" })}
          tone={(taxReview?.skus ?? taxes?.skus_tax_review) ? "warning" : "success"}
        />
        <MetricCard
          label="Packs con problemas"
          value={packs?.packs_active_pending ?? "—"}
          context={packs ? `${packs.packs_active_calculable} de ${packs.packs_active} con costo calculable` : "Packs no disponibles"}
          icon={Boxes}
          href={PACKS_PATH}
          tone={packs?.packs_active_pending ? "warning" : "success"}
        />
      </div>
    </section>
  )
}

export function AttentionPanel({ data }: { data: DashboardResponse }) {
  const margins = data.margins.status === "ok" ? data.margins.data : null
  const costs = data.costs.status === "ok" ? data.costs.data : null
  const taxes = data.taxes.status === "ok" ? data.taxes.data : null
  const packs = data.packs.status === "ok" ? data.packs.data : null
  return (
    <SectionCard
      title="Lo que necesita atención"
      description="Prioridades operativas basadas en las señales disponibles; no representan una estimación monetaria."
      contentClassName="grid p-2 md:grid-cols-2 xl:grid-cols-3"
    >
      <AttentionRow
        label="Productos vendidos bajo costo"
        count={margins?.by_risk.venta_bajo_costo?.skus ?? 0}
        description="Revisar precio, costo de referencia y excepciones vigentes."
        href={withQuery(MARGINS_PATH, { sort: "markup_asc" })}
        tone="danger"
      />
      <AttentionRow
        label="Productos bajo política"
        count={margins?.alerts.skus ?? 0}
        description="Recargo inferior al mínimo configurado para su lista."
        href={withQuery(MARGINS_PATH, { status: "bajo_politica", sort: "gap_asc" })}
        tone="danger"
      />
      <AttentionRow
        label="Packs con costo pendiente"
        count={packs?.packs_active_pending ?? 0}
        description="Composición, componentes o costos requieren revisión."
        href={PACKS_PATH}
        tone="warning"
      />
      <AttentionRow
        label="Costos con anomalías"
        count={costs?.skus_with_high_anomaly ?? 0}
        description="Cambios de costo o recepciones con señales de revisión."
        href={withQuery(COSTS_PATH, { reliability: "requiere_revision" })}
        tone="warning"
      />
      <AttentionRow
        label="Revisión tributaria"
        count={taxes?.skus_tax_review ?? 0}
        description="Impuestos IVA/ILA que impiden obtener costo bruto."
        href={withQuery(MARGINS_PATH, { status: "requiere_revision_tributaria" })}
        tone="warning"
      />
    </SectionCard>
  )
}

function Kpi({
  title,
  count,
  value,
  hint,
  href,
  tone,
}: {
  title: string
  count?: Count
  value?: number | string
  hint?: string
  href?: string
  tone?: string
}) {
  const body = (
    <>
      <p className={sectionTitle}>{title}</p>
      {count ? (
        <p className="mt-1 flex items-baseline gap-1.5">
          <span className="text-2xl font-semibold tabular-nums tracking-tight">{count.skus}</span>
          <span className="text-[11px] text-muted-foreground">SKUs únicos</span>
        </p>
      ) : (
        <p className="mt-1 text-2xl font-semibold tabular-nums tracking-tight">{value ?? "—"}</p>
      )}
      <p className="mt-0.5 truncate text-[11px] text-muted-foreground">
        {count ? `${count.combinations} combinaciones SKU × lista` : ""}
        {count && hint ? " · " : ""}
        {hint}
      </p>
    </>
  )
  const cls = cn("block rounded-md border border-border/70 bg-card px-3 py-2.5 text-left", tone)
  if (!href) return <div className={cls}>{body}</div>
  return (
    <Link
      href={href}
      className={cn(
        cls,
        "transition-colors hover:border-red-700/40 hover:bg-muted/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-red-700/40",
      )}
    >
      {body}
    </Link>
  )
}

export function FreshnessPanel({ freshness }: { freshness: DashboardFreshness }) {
  const issues = freshness.resources.filter((r) => r.status !== "al_dia").length
  return (
    <SectionCard
      title="Datos actualizados — Bsale RAW"
      description={`Dato más antiguo: ${formatDateTime(freshness.oldest_success_at)} · se considera atrasado después de ${freshness.stale_after_hours} h`}
      action={<div className="flex items-center gap-2"><FreshnessBadge value={freshness.overall} />{issues ? <StatusBadge tone="warning">{issues} recursos por revisar</StatusBadge> : null}</div>}
      contentClassName="p-0"
    >
      <details className="group">
        <summary className="flex cursor-pointer list-none items-center justify-between px-4 py-3 text-sm font-medium text-slate-700 hover:bg-slate-50 md:px-5 [&::-webkit-details-marker]:hidden">
          Ver sincronizaciones individuales
          <span className="text-xs font-normal text-slate-500">{freshness.resources.length} recursos</span>
        </summary>
        <div className="grid gap-2 border-t border-slate-100 p-4 sm:grid-cols-2 lg:grid-cols-4 md:p-5">
          {freshness.resources.map((r) => (
            <div key={r.resource} className="rounded-lg border border-slate-200 px-3 py-2.5" title={r.raw_status ?? undefined}>
              <div className="flex items-center justify-between gap-2">
                <span className="truncate text-xs font-medium text-slate-800">{RAW_RESOURCE_LABELS[r.resource] ?? r.resource}</span>
                <FreshnessBadge value={r.status} />
              </div>
              <p className="mt-1.5 text-xs text-slate-500">Último éxito: {formatDateTime(r.last_success_at)}</p>
            </div>
          ))}
        </div>
      </details>
    </SectionCard>
  )
}

export function MarginsOverview({ data, costBasis }: { data: DashboardMargins; costBasis: string }) {
  const link = (params: Record<string, string | number | null>) => withQuery(MARGINS_PATH, { cost_basis: costBasis, ...params })
  return (
    <SectionCard
      title="Márgenes por lista"
      description={`${data.totals.skus} SKUs únicos · ${data.totals.combinations} combinaciones SKU × lista · ${data.totals.price_lists} listas vigentes autorizadas`}
      action={<Link href={MARGINS_PATH} className="text-xs font-medium text-primary hover:underline">Explorar productos</Link>}
      contentClassName="p-0"
    >
      <PriceListTable data={data} link={link} />
    </SectionCard>
  )
}

function PriceListTable({
  data,
  link,
}: {
  data: DashboardMargins
  link: (params: Record<string, string | number | null>) => string
}) {
  const cell = (n: number, href: string, tone?: string) =>
    n ? (
      <Link href={href} className={cn("rounded border px-1.5 py-0.5 text-xs hover:underline", tone)}>
        {n}
      </Link>
    ) : (
      <span className="text-muted-foreground">0</span>
    )
  return (
    <div>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[720px] text-sm">
          <thead className="border-b border-slate-100 bg-slate-50/70 text-left text-xs text-slate-500">
            <tr>
              <th className="px-5 py-3 font-medium">Lista de precios</th>
              <th className="px-4 py-3 text-right font-medium">Evaluados</th>
              <th className="px-4 py-3 text-right font-medium">Bajo política</th>
              <th className="px-4 py-3 text-right font-medium">Dentro</th>
              <th className="px-4 py-3 text-right font-medium">Sobre</th>
              <th className="px-5 py-3 text-right font-medium">No evaluables</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {data.by_price_list.map((l) => (
              <tr key={l.price_list_id} className="transition-colors hover:bg-slate-50/70">
                <td className="px-5 py-3">
                  <span className="font-medium text-slate-900">{l.label ?? l.name}</span>
                  <span className="mt-0.5 block text-xs text-slate-500">
                    #{l.price_list_id} · {l.name}
                  </span>
                </td>
                <td className="px-4 py-3 text-right font-medium tabular-nums text-slate-800">{l.combinations}</td>
                <td className="px-4 py-3 text-right tabular-nums">
                  {cell(l.alerts, link({ price_list_id: l.price_list_id, status: "bajo_politica", sort: "gap_asc" }), MARGIN_STATUS_TONES.bajo_politica)}
                </td>
                <td className="px-4 py-3 text-right tabular-nums">
                  {cell(l.within, link({ price_list_id: l.price_list_id, status: "dentro_politica" }), MARGIN_STATUS_TONES.dentro_politica)}
                </td>
                <td className="px-4 py-3 text-right tabular-nums">
                  {cell(l.above, link({ price_list_id: l.price_list_id, status: "sobre_politica" }), MARGIN_STATUS_TONES.sobre_politica)}
                </td>
                <td className="px-5 py-3 text-right tabular-nums">
                  {cell(l.unavailable, link({ price_list_id: l.price_list_id, status: "sin_costo" }), "border-amber-200 bg-amber-50 text-amber-800")}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {data.scope.excluded.length ? (
        <p className="border-t border-slate-100 px-5 py-3 text-xs leading-5 text-slate-500">
          Fuera del análisis:{" "}
          {data.scope.excluded
            .map((e) => `#${e.price_list_id} ${e.name ?? ""} (${LIST_SCOPE_LABELS[e.status as PriceListScopeStatus] ?? e.status})`)
            .join(" · ")}
          {data.scope.discrepancies ? ` · ${data.scope.discrepancies} discrepancias para revisión administrativa` : ""}
        </p>
      ) : null}
    </div>
  )
}

export function MarginRowsTable({
  title,
  rows,
  empty,
  onOpenProduct,
}: {
  title: string
  rows: CompactMarginRow[]
  empty: string
  onOpenProduct: (variantId: number) => void
}) {
  return (
    <section className="rounded-md border">
      <p className={cn(sectionTitle, "border-b px-3 py-2")}>{title}</p>
      {rows.length ? (
        <div className="overflow-x-auto">
          <table className="w-full min-w-[640px] text-sm">
            <thead className="text-left text-[11px] uppercase tracking-wide text-muted-foreground">
              <tr>
                <th className="px-3 py-1.5 font-medium">Producto</th>
                <th className="px-3 py-1.5 font-medium">Lista</th>
                <th className="px-3 py-1.5 text-right font-medium">Precio</th>
                <th className="px-3 py-1.5 text-right font-medium">Costo ref.</th>
                <th className="px-3 py-1.5 text-right font-medium">Recargo / mín.</th>
                <th className="px-3 py-1.5 font-medium">Estado</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border/60">
              {rows.map((r) => (
                <tr key={`${r.variant_id}-${r.price_list.id}`} className="align-top">
                  <td className="px-3 py-1.5">
                    <button
                      type="button"
                      className="text-left font-medium hover:underline"
                      onClick={() => onOpenProduct(r.variant_id)}
                    >
                      {r.product_name ?? `SKU ${r.variant_id}`}
                    </button>
                    <span className="block text-[11px] text-muted-foreground">
                      {r.code ?? "—"}
                      {r.variant_name ? ` · ${r.variant_name}` : ""}
                      {r.campaign_name ? ` · ${r.campaign_name}` : ""}
                    </span>
                  </td>
                  <td className="px-3 py-1.5 text-xs">{r.price_list.label ?? r.price_list.name}</td>
                  <td className="px-3 py-1.5 text-right tabular-nums">{formatMoney(r.price_gross)}</td>
                  <td className="px-3 py-1.5 text-right tabular-nums">{formatMoney(r.cost_reference_gross)}</td>
                  <td className="px-3 py-1.5 text-right tabular-nums">
                    {formatPct(r.markup_pct)}
                    <span className="block text-[11px] text-muted-foreground">mín. {formatPct(r.min_markup_pct)}</span>
                  </td>
                  <td className="px-3 py-1.5">
                    <div className="flex flex-wrap gap-1">
                      <span className={cn(chip, MARGIN_STATUS_TONES[r.status])}>{MARGIN_STATUS_LABELS[r.status]}</span>
                      <DataClassBadge value={r.data_class} />
                      {r.risks
                        .filter((k) => k === "venta_bajo_costo" || k === "costo_sospechoso" || k === "costo_desactualizado")
                        .map((k) => (
                          <span key={k} className={cn(chip, "border-red-300 bg-red-50 text-red-800")}>
                            {MARGIN_RISK_LABELS[k]}
                          </span>
                        ))}
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <p className="px-3 py-3 text-sm text-muted-foreground">{empty}</p>
      )}
    </section>
  )
}

export function PolicyCard({ data }: { data: DashboardMargins }) {
  const p = data.policy
  return (
    <section className="rounded-md border p-3 text-sm">
      <p className={sectionTitle}>Política de márgenes</p>
      <ul className="mt-1.5 space-y-0.5">
        <li>
          {p.rules_in_scope} reglas en listas evaluadas (de {p.rules_total}; {p.rules_out_of_scope} fuera de alcance)
        </li>
        <li>{p.rules_pending_review} pendientes de revisión</li>
        <li>{p.rules_max_zero_in_scope} con máximo 0 · {p.rules_with_issues_in_scope} con observaciones</li>
        <li>{p.missing_rule_combinations} combinaciones lista × tipo sin regla</li>
      </ul>
      <Link href={withQuery(MARGINS_PATH, { tab: "politica" })} className="mt-2 inline-block text-xs text-red-700 hover:underline">
        Revisar política
      </Link>
    </section>
  )
}

export function CostsOverview({ data, onOpenProduct }: { data: DashboardCosts; onOpenProduct: (variantId: number) => void }) {
  const reliabilities = Object.keys(RELIABILITY_LABELS) as CostReliability[]
  return (
    <section className="space-y-2">
      <div className="flex flex-wrap items-baseline gap-2">
        <h2 className="text-base font-semibold">Costos</h2>
        <span className="text-[11px] text-muted-foreground">
          {data.skus_active} SKUs activos · costo promedio sincronizado {formatDateTime(data.data_dates.average_cost_fetched_max)} ·
          última recepción {formatDate(data.data_dates.last_reception_date)}
        </span>
      </div>
      <div className="grid grid-cols-2 gap-2 lg:grid-cols-4">
        <Kpi
          title="Costo confiable"
          value={data.skus_reliable}
          hint={`de ${data.skus_active} SKUs activos`}
          href={withQuery(COSTS_PATH, { reliability: "consistente" })}
        />
        <Kpi
          title="Requiere revisión"
          value={data.skus_by_reliability.requiere_revision ?? 0}
          hint={`${data.skus_with_high_anomaly} con anomalía alta`}
          href={withQuery(COSTS_PATH, { reliability: "requiere_revision" })}
          tone={data.skus_by_reliability.requiere_revision ? "border-red-300" : undefined}
        />
        <Kpi
          title={`Alzas ≥ ${formatPct(data.relevant_increases.threshold_pct)}`}
          value={data.relevant_increases.total}
          hint="Última compra vs anterior comparable"
          href={withQuery(COSTS_PATH, { sort: "change_pct_desc" })}
        />
        <Kpi
          title="Alertas altas (30 días)"
          value={data.high_alerts.total}
          hint={`${formatDate(data.high_alerts.date_from)} – ${formatDate(data.high_alerts.date_to)}`}
          href={withQuery(COSTS_PATH, { tab: "alertas" })}
        />
      </div>
      <div className="flex flex-wrap gap-1.5">
        {reliabilities.map((r) => (
          <Link key={r} href={withQuery(COSTS_PATH, { reliability: r })} className="inline-flex items-center gap-1 text-xs hover:underline">
            <ReliabilityBadge status={r} />
            <span className="tabular-nums">{data.skus_by_reliability[r] ?? 0}</span>
          </Link>
        ))}
      </div>
      <div className="grid gap-2 lg:grid-cols-2">
        <section className="rounded-md border">
          <p className={cn(sectionTitle, "border-b px-3 py-2")}>Mayores alzas de costo</p>
          {data.relevant_increases.top.length ? (
            <ul className="divide-y divide-border/60 text-sm">
              {data.relevant_increases.top.map((r) => (
                <li key={r.variant_id} className="flex items-start justify-between gap-2 px-3 py-1.5">
                  <button type="button" className="text-left hover:underline" onClick={() => onOpenProduct(r.variant_id)}>
                    <span className="font-medium">{r.product_name ?? `SKU ${r.variant_id}`}</span>
                    <span className="block text-[11px] text-muted-foreground">
                      {r.code ?? "—"} · {formatMoney(r.previous_comparable?.cost)} → {formatMoney(r.last_comparable?.cost)}
                    </span>
                  </button>
                  <span className="shrink-0 tabular-nums font-medium text-red-700">{formatPct(r.change_pct)}</span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="px-3 py-3 text-sm text-muted-foreground">Sin alzas relevantes.</p>
          )}
        </section>
        <section className="rounded-md border">
          <p className={cn(sectionTitle, "border-b px-3 py-2")}>Alertas de severidad alta (30 días)</p>
          {Object.keys(data.high_alerts.counts_by_code).length ? (
            <p className="flex flex-wrap gap-1 border-b px-3 py-1.5">
              {Object.entries(data.high_alerts.counts_by_code).map(([code, n]) => (
                <span key={code} className={cn(chip, "border-red-300 bg-red-50 text-red-800")}>
                  {ANOMALY_LABELS[code as AnomalyCode] ?? code}: {n}
                </span>
              ))}
            </p>
          ) : null}
          {data.high_alerts.top.length ? (
            <ul className="divide-y divide-border/60 text-sm">
              {data.high_alerts.top.map((a) => (
                <li key={a.detail_id} className="px-3 py-1.5">
                  {a.variant_id != null ? (
                    <button type="button" className="text-left font-medium hover:underline" onClick={() => onOpenProduct(a.variant_id as number)}>
                      {a.product_name ?? `SKU ${a.variant_id}`}
                    </button>
                  ) : (
                    <span className="font-medium">{a.product_name ?? "—"}</span>
                  )}
                  <span className="block text-[11px] text-muted-foreground">
                    {formatDate(a.admission_date)} · {a.document ?? "Recepción"} {a.document_number ?? `#${a.reception_id}`} ·{" "}
                    {a.anomalies
                      .filter((x) => x.severity === "alta")
                      .map((x) => ANOMALY_LABELS[x.code])
                      .join(", ")}
                  </span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="px-3 py-3 text-sm text-muted-foreground">Sin alertas altas en el período.</p>
          )}
        </section>
      </div>
    </section>
  )
}

export function TaxesOverview({ data, canSeeCosts }: { data: DashboardTaxes; canSeeCosts: boolean }) {
  const costsLink = (status: string) => (canSeeCosts ? withQuery(COSTS_PATH, { tax_status: status }) : undefined)
  const pct = (v: string | null) => (v == null ? "—" : `${v}%`)
  return (
    <section className="space-y-2">
      <div className="flex flex-wrap items-baseline gap-2">
        <h2 className="text-base font-semibold">Impuestos y costo bruto comercial</h2>
        <span className="text-[11px] text-muted-foreground">
          {data.skus_active} SKUs activos · bruto = costo neto promedio × (1 + (IVA + ILA) / 100) · tasas desde BSALE RAW
        </span>
        {!data.tax_config_verified ? (
          <span className={cn(chip, DATA_CLASS_TONES.provisional)}>Configuración no contrastada: márgenes provisionales</span>
        ) : null}
      </div>
      <div className="grid grid-cols-2 gap-2 lg:grid-cols-5">
        <Kpi
          title="Impuestos identificados"
          value={data.skus_taxes_identified}
          hint={`${pct(data.coverage_pct.taxes_identified)} de cobertura`}
        />
        <Kpi
          title="Bruto calculable"
          value={data.skus_gross_calculable}
          hint={`${pct(data.coverage_pct.gross_calculable)} · provisional`}
        />
        <Kpi title="Sin costo" value={data.skus_without_cost} hint="Sin costo promedio Bsale positivo" />
        <Kpi
          title="Impuestos sospechosos"
          value={data.skus_tax_review}
          hint="Requieren revisión tributaria"
          tone={data.skus_tax_review ? "border-orange-300" : undefined}
        />
        <Kpi
          title="Sin configuración"
          value={data.skus_without_tax_config}
          hint="Bsale no informa impuestos: no se asume exento"
          href={costsLink("sin_impuestos")}
          tone={data.skus_without_tax_config ? "border-amber-300" : undefined}
        />
      </div>
      <div className="grid gap-2 lg:grid-cols-2">
        <section className="rounded-md border">
          <p className={cn(sectionTitle, "border-b px-3 py-2")}>Combinaciones de impuestos</p>
          <ul className="divide-y divide-border/60 text-sm">
            {data.combinations.slice(0, 12).map((c) => (
              <li key={String(c.tax_ids)} className="flex items-start justify-between gap-2 px-3 py-1.5">
                <span>
                  <span className="font-medium">{c.taxes.map((t) => t.name).join(" + ") || "Sin impuestos"}</span>
                  <span className="block text-[11px] text-muted-foreground">
                    {TAX_LABELS[c.status]} · {TAX_VALIDATION_LABELS[c.validation]}
                    {c.total_rate_pct ? ` · ${c.total_rate_pct}%` : ""}
                  </span>
                </span>
                <span className="shrink-0 text-right tabular-nums">
                  {c.skus}
                  <span className="block text-[11px] text-muted-foreground">{c.products} productos</span>
                </span>
              </li>
            ))}
          </ul>
        </section>
        <section className="rounded-md border">
          <p className={cn(sectionTitle, "border-b px-3 py-2")}>Anomalías tributarias</p>
          {data.anomalies.length ? (
            <ul className="divide-y divide-border/60 text-sm">
              {data.anomalies.map((a) => (
                <li key={a.code} className="px-3 py-1.5">
                  <span className="flex items-baseline justify-between gap-2">
                    <span className="font-medium">{TAX_ISSUE_LABELS[a.code] ?? a.code}</span>
                    <span className="tabular-nums">
                      {a.skus} SKUs · {a.products} productos
                    </span>
                  </span>
                  <span className="block truncate text-[11px] text-muted-foreground">
                    {a.examples.map((e) => e.product_name ?? e.code ?? `SKU ${e.variant_id}`).join(" · ")}
                  </span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="px-3 py-3 text-sm text-muted-foreground">Sin anomalías tributarias.</p>
          )}
        </section>
      </div>
      <details className="rounded-md border px-3 py-2 text-sm">
        <summary className="cursor-pointer text-xs font-medium">Catálogo de impuestos BSALE RAW ({data.catalog.length})</summary>
        <div className="mt-2 overflow-x-auto">
          <table className="w-full min-w-[640px] text-xs">
            <thead className="text-left uppercase tracking-wide text-muted-foreground">
              <tr>
                <th className="py-1 font-medium">ID</th>
                <th className="py-1 font-medium">Nombre</th>
                <th className="py-1 font-medium">Código SII</th>
                <th className="py-1 text-right font-medium">Tasa</th>
                <th className="py-1 font-medium">Tipo</th>
                <th className="py-1 font-medium">Observaciones</th>
                <th className="py-1 text-right font-medium">SKUs</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border/60">
              {data.catalog.map((t) => (
                <tr key={t.tax_id}>
                  <td className="py-1 tabular-nums">{t.tax_id}</td>
                  <td className="py-1">{t.name}</td>
                  <td className="py-1">{t.code ?? "—"}</td>
                  <td className="py-1 text-right tabular-nums">{t.percentage ?? "—"}%</td>
                  <td className="py-1">{TAX_KIND_LABELS[t.kind] ?? t.kind}</td>
                  <td className="py-1 text-muted-foreground">{t.issues.map((i) => TAX_ISSUE_LABELS[i] ?? i).join(", ") || "—"}</td>
                  <td className="py-1 text-right tabular-nums">{t.skus_using}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </details>
    </section>
  )
}

const TAX_KIND_LABELS: Record<string, string> = {
  iva: "IVA",
  ila: "ILA",
  iva_anticipado: "IVA anticipado (contable)",
  desconocido: "No clasificado",
}

export function PromotionsOverview({ data }: { data: DashboardPromotions }) {
  const k = data.kpis
  const link = (tab: string) => withQuery(PROMOTIONS_PATH, { tab })
  return (
    <section className="space-y-2">
      <div className="flex flex-wrap items-baseline gap-2">
        <h2 className="text-base font-semibold">Ofertas y remates</h2>
        <span className="text-[11px] text-muted-foreground">Mes {data.month} · solo lectura, no modifica precios de Bsale</span>
      </div>
      {!data.tables_ready ? (
        <p className="rounded-md border border-amber-300 bg-amber-50 px-3 py-2 text-sm text-amber-800">
          Las tablas de Promociones V2 no están disponibles en esta base. Los márgenes no pueden excluir promociones.
        </p>
      ) : null}
      <div className="grid grid-cols-2 gap-2 lg:grid-cols-4">
        <Kpi title="Campañas vigentes" value={k.campaigns_vigentes} hint={`${k.campaigns_upcoming} próximas`} href={link("vigentes")} />
        <Kpi
          title="Productos en oferta / remate"
          value={`${k.products_on_offer} / ${k.products_on_clearance}`}
          hint="Ítems en campañas vigentes"
          href={link("remates")}
        />
        <Kpi
          title="Pendientes"
          value={k.campaigns_pending_approval + k.exceptions_pending}
          hint={`${k.campaigns_pending_approval} por aprobar · ${k.exceptions_pending} excepciones sin verificar`}
          href={link("pendientes")}
          tone={k.campaigns_pending_approval + k.exceptions_pending ? "border-amber-300" : undefined}
        />
        <Kpi
          title="Vencidas sin revertir"
          value={k.items_expired_not_reverted}
          hint="Precio Bsale aún en valor promocional"
          href={link("historial")}
          tone={k.items_expired_not_reverted ? "border-red-300" : undefined}
        />
      </div>
      {data.legacy.promotions ? (
        <p className="text-[11px] text-muted-foreground">
          Módulo anterior: {data.legacy.promotions} promociones ({data.legacy.active_flag} marcadas activas), no se usan para excluir alertas.
        </p>
      ) : null}
    </section>
  )
}

export function PacksOverview({ data, costBasis }: { data: DashboardPacks; costBasis: string }) {
  const link = (params: Record<string, string>) => withQuery(PACKS_PATH, { cost_basis: costBasis, ...params })
  const pending = (Object.entries(data.by_pack_status) as [PackCostStatus, number][]).filter(([s, n]) => s !== "calculado" && n > 0)
  const problems = (Object.entries(data.packs_by_problem) as [PackProblem, number][]).filter(([, n]) => n > 0)
  return (
    <section className="space-y-2">
      <div className="flex flex-wrap items-baseline gap-2">
        <h2 className="text-base font-semibold">Packs Bsale</h2>
        <span className="text-[11px] text-muted-foreground">Costo = suma de componentes con sus propios impuestos · rentabilidad actual estimada</span>
      </div>
      <div className="grid grid-cols-2 gap-2 lg:grid-cols-4">
        <Kpi title="Packs activos" value={data.packs_active} href={link({})} />
        <Kpi
          title="Con costo calculable"
          value={data.packs_active_calculable}
          hint={data.coverage_pct != null ? `Cobertura ${formatPct(data.coverage_pct)}` : undefined}
          href={link({ pack_status: "calculado" })}
        />
        <Kpi
          title="Costo pendiente"
          value={data.packs_active_pending}
          hint="Sin margen definitivo"
          tone={data.packs_active_pending ? "border-amber-300" : undefined}
        />
        <Kpi
          title="Bajo costo / bajo política"
          value={`${data.packs_by_problem.precio_bajo_costo ?? 0} / ${data.packs_by_problem.bajo_politica ?? 0}`}
          hint="Packs con al menos una lista afectada"
          href={link({ problem: "precio_bajo_costo" })}
          tone={data.packs_by_problem.precio_bajo_costo ? "border-red-300" : undefined}
        />
      </div>
      {pending.length || problems.length ? (
        <p className="text-[11px] text-muted-foreground">
          {pending.map(([s, n]) => `${PACK_STATUS_LABELS[s]}: ${n}`).join(" · ")}
          {pending.length && problems.length ? " — " : ""}
          {problems.map(([p, n]) => `${PACK_PROBLEM_LABELS[p]}: ${n}`).join(" · ")}
        </p>
      ) : null}
    </section>
  )
}
