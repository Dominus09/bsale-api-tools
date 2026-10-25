"use client"

import { Loader2 } from "lucide-react"

import { cn } from "@/lib/utils"
import { formatDate, formatDateTime, formatMoney, formatPct, formatQty } from "@/lib/analytics-v2/labels"
import { ProductIdentityCell } from "@/components/analytics-v2/shared/product-identity-cell"
import { CLEARANCE_LABELS, HISTORY_EVENT_LABELS } from "@/lib/analytics-v2/promotions-labels"
import type { CampaignItem, CampaignSummaryRow, HistoryEvent } from "@/lib/analytics-v2/promotions-types"

import { ApprovalBadge, ExceptionBadge, KindBadge, TimelineBadge, VerificationBadge } from "./promotion-badges"

const th = "px-3 py-2 font-medium"
const thead = "bg-muted/40 text-left text-[11px] uppercase tracking-wide text-muted-foreground"

function Shell({ loading, empty, minWidth, children }: { loading: boolean; empty: boolean; minWidth: string; children: React.ReactNode }) {
  if (loading && empty) {
    return (
      <div className="flex justify-center py-10">
        <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      </div>
    )
  }
  return (
    <div className={cn("overflow-x-auto rounded-md border", loading && "opacity-60")}>
      <table className={cn("w-full text-sm", minWidth)}>{children}</table>
    </div>
  )
}

function EmptyRow({ colSpan, text }: { colSpan: number; text: string }) {
  return (
    <tr>
      <td colSpan={colSpan} className="px-3 py-8 text-center text-muted-foreground">
        {text}
      </td>
    </tr>
  )
}

export function PromotionItemsTable({
  items,
  loading,
  emptyText,
  onOpenCampaign,
}: {
  items: CampaignItem[]
  loading: boolean
  emptyText: string
  onOpenCampaign: (campaignId: number) => void
}) {
  return (
    <Shell loading={loading} empty={items.length === 0} minWidth="min-w-[1180px]">
      <thead className={thead}>
        <tr>
          <th className={th}>Producto</th>
          <th className={th}>Lista</th>
          <th className={th}>Campaña</th>
          <th className={th}>Vigencia</th>
          <th className={cn(th, "text-right")}>Normal (registro)</th>
          <th className={cn(th, "text-right")}>Precio promo</th>
          <th className={cn(th, "text-right")}>Bsale hoy</th>
          <th className={th}>Verificación</th>
          <th className={th}>Excepción de margen</th>
        </tr>
      </thead>
      <tbody className="divide-y divide-border/60">
        {items.map((i) => (
          <tr key={i.item_id} className="align-top">
            <td className="max-w-[260px] px-3 py-2">
              <ProductIdentityCell
                compact
                productTypeName={i.product_type?.name}
                productName={i.product_name}
                variantName={i.variant_name}
                barcode={i.barcode}
              />
              <span className="mt-0.5 block truncate text-[11px] text-muted-foreground">
                {i.sku_code ? `SKU ${i.sku_code}` : "Sin SKU"}
              </span>
              {i.kind === "remate" ? (
                <span className="block text-[11px] text-orange-800 dark:text-orange-300">
                  {i.clearance_reason ? CLEARANCE_LABELS[i.clearance_reason] : "Sin motivo"} · stock ref. {formatQty(i.reference_stock)}
                </span>
              ) : null}
            </td>
            <td className="px-3 py-2 text-xs">
              <span className="font-medium">{i.price_list.label ?? i.price_list.name ?? "—"}</span>
              <span className="block text-[11px] text-muted-foreground">#{i.price_list.id}</span>
              {!i.price_list.in_scope ? <span className="block text-[11px] text-red-700 dark:text-red-300">Fuera de alcance</span> : null}
            </td>
            <td className="max-w-[220px] px-3 py-2">
              <button type="button" className="block truncate text-left font-medium text-primary hover:underline" onClick={() => onOpenCampaign(i.campaign_id)}>
                {i.campaign_name}
              </button>
              <div className="mt-1 flex flex-wrap gap-1">
                <KindBadge kind={i.kind} />
                <ApprovalBadge status={i.approval_status} />
              </div>
            </td>
            <td className="px-3 py-2 text-xs">
              <TimelineBadge timeline={i.timeline} />
              <span className="mt-1 block text-[11px] text-muted-foreground">
                {formatDate(i.starts_on)} → {formatDate(i.ends_on)}
              </span>
            </td>
            <td className="px-3 py-2 text-right tabular-nums">
              {formatMoney(i.normal_gross_price)}
              <span className="block text-[11px] text-muted-foreground">{formatDate(i.normal_price_fetched_at)}</span>
            </td>
            <td className="px-3 py-2 text-right tabular-nums font-medium">
              {formatMoney(i.promo_gross_price)}
              <span className="block text-[11px] text-muted-foreground">{i.discount_pct != null ? formatPct(i.discount_pct) : ""}</span>
            </td>
            <td className="px-3 py-2 text-right tabular-nums">
              {formatMoney(i.bsale_current_gross)}
              <span className="block text-[11px] text-muted-foreground">{formatDateTime(i.bsale_fetched_at)}</span>
            </td>
            <td className="px-3 py-2">
              <VerificationBadge verification={i.verification} />
            </td>
            <td className="max-w-[220px] px-3 py-2">
              <ExceptionBadge check={i.exception_check} />
              {i.exception && i.exception.applies_to_this_item === false && i.exception.exception ? (
                <span className="mt-1 block text-[11px] text-muted-foreground">
                  Aplica otra campaña: {i.exception.exception.campaign_name ?? i.exception.exception.reference ?? "—"}
                </span>
              ) : null}
            </td>
          </tr>
        ))}
        {!loading && items.length === 0 ? <EmptyRow colSpan={9} text={emptyText} /> : null}
      </tbody>
    </Shell>
  )
}

export function PromotionCampaignsTable({
  campaigns,
  loading,
  onOpen,
}: {
  campaigns: CampaignSummaryRow[]
  loading: boolean
  onOpen: (campaignId: number) => void
}) {
  return (
    <Shell loading={loading} empty={campaigns.length === 0} minWidth="min-w-[1000px]">
      <thead className={thead}>
        <tr>
          <th className={th}>Campaña</th>
          <th className={th}>Fechas</th>
          <th className={th}>Estado</th>
          <th className={cn(th, "text-right")}>Productos</th>
          <th className={th}>Verificación Bsale</th>
          <th className={th}>Aprobación</th>
        </tr>
      </thead>
      <tbody className="divide-y divide-border/60">
        {campaigns.map((c) => (
          <tr key={c.id} className="cursor-pointer align-top hover:bg-muted/30" onClick={() => onOpen(c.id)}>
            <td className="max-w-[300px] px-3 py-2">
              <span className="block truncate font-medium">{c.name}</span>
              <div className="mt-1 flex flex-wrap items-center gap-1">
                <KindBadge kind={c.kind} />
                {c.legacy_promotion_id ? <span className="text-[11px] text-muted-foreground">Importada de promoción #{c.legacy_promotion_id}</span> : null}
              </div>
              {c.responsible ? <span className="block text-[11px] text-muted-foreground">Responsable: {c.responsible}</span> : null}
            </td>
            <td className="px-3 py-2 text-xs">
              {formatDate(c.starts_on)} → {formatDate(c.ends_on)}
              {c.effective_end_on !== c.ends_on ? (
                <span className="block text-[11px] text-muted-foreground">Fin real {formatDate(c.effective_end_on)}</span>
              ) : null}
            </td>
            <td className="px-3 py-2">
              <div className="flex flex-wrap gap-1">
                <TimelineBadge timeline={c.timeline} />
                <ApprovalBadge status={c.approval_status} />
              </div>
            </td>
            <td className="px-3 py-2 text-right tabular-nums">
              {c.products}
              <span className="block text-[11px] text-muted-foreground">listas {c.price_list_ids.join(", ") || "—"}</span>
            </td>
            <td className="px-3 py-2 text-[11px]">
              {Object.entries(c.verification).length
                ? Object.entries(c.verification).map(([k, n]) => (
                    <span key={k} className="mr-1 inline-flex items-center gap-1">
                      <VerificationBadge verification={k as CampaignItem["verification"]} /> {n}
                    </span>
                  ))
                : "—"}
            </td>
            <td className="px-3 py-2 text-[11px] text-muted-foreground">
              {c.approved_by ? `${c.approved_by} · ${formatDateTime(c.approved_at)}` : c.status_reason ?? "—"}
            </td>
          </tr>
        ))}
        {!loading && campaigns.length === 0 ? <EmptyRow colSpan={6} text="Sin campañas en el mes con los filtros aplicados." /> : null}
      </tbody>
    </Shell>
  )
}

function ValueDiff({ prev, next }: { prev: Record<string, unknown> | null; next: Record<string, unknown> | null }) {
  const keys = Array.from(new Set([...Object.keys(prev ?? {}), ...Object.keys(next ?? {})]))
  if (!keys.length) return <span className="text-muted-foreground">—</span>
  return (
    <ul className="space-y-0.5">
      {keys.map((k) => (
        <li key={k}>
          <span className="text-muted-foreground">{k}:</span> {prev && k in prev ? String(prev[k] ?? "—") : ""}
          {prev && k in prev && next && k in next ? " → " : ""}
          {next && k in next ? String(next[k] ?? "—") : ""}
        </li>
      ))}
    </ul>
  )
}

export function PromotionHistoryTable({
  events,
  loading,
  showCampaign,
  onOpenCampaign,
}: {
  events: HistoryEvent[]
  loading: boolean
  showCampaign: boolean
  onOpenCampaign?: (campaignId: number) => void
}) {
  return (
    <Shell loading={loading} empty={events.length === 0} minWidth="min-w-[900px]">
      <thead className={thead}>
        <tr>
          <th className={th}>Fecha</th>
          {showCampaign ? <th className={th}>Campaña</th> : null}
          <th className={th}>Evento</th>
          <th className={th}>Cambios</th>
          <th className={th}>Motivo</th>
          <th className={th}>Usuario</th>
        </tr>
      </thead>
      <tbody className="divide-y divide-border/60">
        {events.map((e) => (
          <tr key={e.id} className="align-top text-xs">
            <td className="whitespace-nowrap px-3 py-2">{formatDateTime(e.occurred_at)}</td>
            {showCampaign ? (
              <td className="max-w-[220px] px-3 py-2">
                <button type="button" className="truncate text-left text-primary hover:underline" onClick={() => onOpenCampaign?.(e.campaign_id)}>
                  {e.campaign_name ?? `Campaña ${e.campaign_id}`}
                </button>
                {e.product_name ? <span className="block truncate text-muted-foreground">{e.product_name}</span> : null}
              </td>
            ) : null}
            <td className="px-3 py-2">
              <span className="font-medium">{HISTORY_EVENT_LABELS[e.event] ?? e.event}</span>
              {e.previous_status || e.new_status ? (
                <span className="block text-muted-foreground">
                  {e.previous_status ?? "—"} → {e.new_status ?? "—"}
                </span>
              ) : null}
              {e.item_id ? <span className="block text-muted-foreground">Producto #{e.item_id}</span> : null}
            </td>
            <td className="max-w-[320px] px-3 py-2 break-words">
              <ValueDiff prev={e.previous_values} next={e.new_values} />
            </td>
            <td className="max-w-[220px] px-3 py-2">{e.reason ?? "—"}</td>
            <td className="px-3 py-2">
              {e.actor ?? "—"}
              {e.actor_role ? <span className="block text-muted-foreground">{e.actor_role}</span> : null}
            </td>
          </tr>
        ))}
        {!loading && events.length === 0 ? <EmptyRow colSpan={showCampaign ? 6 : 5} text="Sin movimientos registrados." /> : null}
      </tbody>
    </Shell>
  )
}
