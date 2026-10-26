"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import { AlertTriangle, ChevronLeft, ChevronRight, Info, Loader2, Plus } from "lucide-react"

import { Pager } from "@/components/analytics-v2/costs/cost-v2-products"
import { PromotionCampaignSheet } from "@/components/analytics-v2/promotions/promotion-v2-campaign-sheet"
import { CampaignFormDialog } from "@/components/analytics-v2/promotions/promotion-v2-dialogs"
import { PromotionLegacyPanel } from "@/components/analytics-v2/promotions/promotion-v2-legacy"
import { PromotionPacksInfo } from "@/components/analytics-v2/promotions/promotion-v2-packs"
import { PromotionV2Kpis, PromotionV2MonthOverview } from "@/components/analytics-v2/promotions/promotion-v2-summary"
import {
  PromotionCampaignsTable,
  PromotionHistoryTable,
  PromotionItemsTable,
} from "@/components/analytics-v2/promotions/promotion-v2-tables"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { getStoredCompanyId } from "@/lib/api"
import {
  AnalyticsV2Error,
  getPromotionCampaigns,
  getPromotionItems,
  getPromotionsFilters,
  getPromotionsHistory,
  getPromotionsSummary,
} from "@/lib/analytics-v2/api"
import { queryEnum, queryInt, readQuery } from "@/lib/analytics-v2/url"
import { cn } from "@/lib/utils"
import {
  APPROVAL_LABELS,
  KIND_LABELS,
  SECTION_LABELS,
  TIMELINE_LABELS,
  currentMonth,
  monthLabel,
  shiftMonth,
} from "@/lib/analytics-v2/promotions-labels"
import type {
  CampaignApproval,
  CampaignItem,
  CampaignKind,
  CampaignSummaryRow,
  CampaignTimeline,
  HistoryEvent,
  Paged,
  PromotionSection,
  PromotionsFilters,
  PromotionsSummary,
} from "@/lib/analytics-v2/promotions-types"

const PAGE_SIZE = 50
const ITEM_SECTIONS: PromotionSection[] = ["vigentes", "remates", "proximas", "pendientes"]

type Tab = "resumen" | PromotionSection | "anteriores" | "packs"

const URL_TABS: readonly Tab[] = ["resumen", "vigentes", "remates", "proximas", "historial", "pendientes", "anteriores", "packs"]

type FilterDraft = {
  priceListId: string
  kind: CampaignKind | ""
  approval: CampaignApproval | ""
  timeline: CampaignTimeline | ""
  q: string
}

const EMPTY_FILTERS: FilterDraft = { priceListId: "", kind: "", approval: "", timeline: "", q: "" }

const EMPTY_TEXT: Record<PromotionSection, string> = {
  vigentes: "No hay ofertas aprobadas vigentes hoy.",
  remates: "No hay remates vigentes ni programados.",
  proximas: "No hay campañas programadas en los próximos 180 días.",
  pendientes: "No hay campañas pendientes de aprobación ni excepciones sin verificar.",
  historial: "Sin registros.",
}

const selectClass = "h-9 rounded-md border border-input bg-background px-2 text-sm"
const labelClass = "flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground"

export default function PromotionsV2Page() {
  const [companyId, setCompanyId] = useState<number | null>(null)
  const [month, setMonth] = useState(currentMonth)
  const [summary, setSummary] = useState<PromotionsSummary | null>(null)
  const [filters, setFilters] = useState<PromotionsFilters | null>(null)
  const [draft, setDraft] = useState<FilterDraft>(EMPTY_FILTERS)
  const [applied, setApplied] = useState<FilterDraft>(EMPTY_FILTERS)
  const [tab, setTab] = useState<Tab>("resumen")
  const [page, setPage] = useState(1)
  const [items, setItems] = useState<Paged<CampaignItem> | null>(null)
  const [campaigns, setCampaigns] = useState<Paged<CampaignSummaryRow> | null>(null)
  const [history, setHistory] = useState<Paged<HistoryEvent> | null>(null)
  const [loadingSummary, setLoadingSummary] = useState(false)
  const [loadingList, setLoadingList] = useState(false)
  const [blocking, setBlocking] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [campaignId, setCampaignId] = useState<number | null>(null)
  const [createOpen, setCreateOpen] = useState(false)
  const [reloadKey, setReloadKey] = useState(0)
  const listAbort = useRef<AbortController | null>(null)

  useEffect(() => {
    setCompanyId(getStoredCompanyId())
    const qs = readQuery()
    const initialTab = queryEnum<Tab>(qs, "tab", URL_TABS)
    if (initialTab) setTab(initialTab)
    const campaign = queryInt(qs, "campaign_id")
    if (campaign) setCampaignId(campaign)
  }, [])

  useEffect(() => {
    if (companyId == null) return
    const ac = new AbortController()
    setLoadingSummary(true)
    setBlocking(null)
    Promise.all([getPromotionsSummary(companyId, month, ac.signal), getPromotionsFilters(companyId, ac.signal)])
      .then(([s, f]) => {
        setSummary(s)
        setFilters(f)
      })
      .catch((e: unknown) => {
        if (ac.signal.aborted) return
        if (e instanceof AnalyticsV2Error && [401, 403, 409, 422].includes(e.status)) setBlocking(e.message)
        else setError(e instanceof Error ? e.message : "Error al cargar el resumen")
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoadingSummary(false)
      })
    return () => ac.abort()
  }, [companyId, month, reloadKey])

  const loadList = useCallback(() => {
    if (companyId == null || blocking || tab === "anteriores" || tab === "packs") return
    listAbort.current?.abort()
    const ac = new AbortController()
    listAbort.current = ac
    setLoadingList(true)
    setError(null)
    const common = {
      company_id: companyId,
      month,
      kind: applied.kind,
      approval: applied.approval,
      price_list_id: applied.priceListId ? Number(applied.priceListId) : "",
      q: applied.q.trim() || undefined,
      page,
      page_size: PAGE_SIZE,
    } as const
    let request: Promise<unknown>
    if (tab === "resumen") {
      request = getPromotionCampaigns({ ...common, timeline: applied.timeline }, ac.signal).then(setCampaigns)
    } else if (tab === "historial") {
      request = getPromotionsHistory({ company_id: companyId, month, page, page_size: PAGE_SIZE }, ac.signal).then(setHistory)
    } else {
      request = getPromotionItems({ ...common, section: tab }, ac.signal).then(setItems)
    }
    request
      .catch((e: unknown) => {
        if (!ac.signal.aborted) setError(e instanceof Error ? e.message : "Error al cargar la sección")
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoadingList(false)
      })
  }, [companyId, blocking, tab, month, applied, page])

  useEffect(() => {
    loadList()
  }, [loadList, reloadKey])

  const changeTab = (next: string) => {
    setTab(next as Tab)
    setPage(1)
    setItems(null)
  }

  const applyFilters = (next: FilterDraft = draft) => {
    setDraft(next)
    setApplied(next)
    setPage(1)
  }

  const refresh = () => setReloadKey((k) => k + 1)

  if (companyId == null) {
    return <p className="p-6 text-sm text-muted-foreground">Seleccione una empresa para usar Promociones V2.</p>
  }

  const meta = summary?.meta
  const caps = meta?.capabilities
  const tablesReady = meta?.tables_ready ?? false
  const isItemTab = (ITEM_SECTIONS as string[]).includes(tab)
  const paged = tab === "resumen" ? campaigns : tab === "historial" ? history : isItemTab ? items : null

  return (
    <div className="mx-auto flex w-full max-w-[1440px] flex-col gap-3 p-3 md:p-4">
      <header className="flex flex-col gap-2 border-b border-border/60 pb-3 md:flex-row md:items-end md:justify-between">
        <div className="flex flex-col gap-1">
          <div className="flex flex-wrap items-center gap-2">
            <h1 className="text-[28px] font-semibold leading-8 tracking-[-0.02em] text-lq-ink sm:text-[32px] sm:leading-9 font-semibold">Promociones y Remates V2</h1>
            <span className="rounded-md border border-amber-300 bg-amber-50 px-1.5 py-0.5 text-[11px] font-medium text-amber-800">
              En validación
            </span>
          </div>
          <p className="text-sm text-muted-foreground">
            {meta?.company_name ?? `Empresa ${companyId}`} · Registro administrativo de campañas · Precios actuales: BSALE RAW (solo lectura)
          </p>
          <p className="text-xs text-muted-foreground">
            Hoy {meta?.today ?? "—"} (hora Chile) · Este módulo no cambia precios en Bsale.
          </p>
        </div>
        <div className="flex flex-wrap items-end gap-2">
          <div className="flex items-center gap-1">
            <Button size="icon" variant="outline" aria-label="Mes anterior" onClick={() => setMonth((m) => shiftMonth(m, -1))}>
              <ChevronLeft className="h-4 w-4" />
            </Button>
            <Input
              type="month"
              className="h-9 w-[150px]"
              value={month}
              onChange={(e) => {
                if (/^\d{4}-\d{2}$/.test(e.target.value)) setMonth(e.target.value)
              }}
            />
            <Button size="icon" variant="outline" aria-label="Mes siguiente" onClick={() => setMonth((m) => shiftMonth(m, 1))}>
              <ChevronRight className="h-4 w-4" />
            </Button>
            {month !== currentMonth() ? (
              <Button size="sm" variant="ghost" onClick={() => setMonth(currentMonth())}>
                Mes actual
              </Button>
            ) : null}
          </div>
          {caps?.create && tablesReady ? (
            <Button onClick={() => setCreateOpen(true)}>
              <Plus className="mr-1 h-4 w-4" /> Nueva campaña
            </Button>
          ) : null}
        </div>
      </header>

      {blocking ? (
        <Alert>
          <Info className="h-4 w-4" />
          <AlertTitle>Promociones V2 no disponible</AlertTitle>
          <AlertDescription>{blocking}</AlertDescription>
        </Alert>
      ) : (
        <>
          {error ? (
            <Alert variant="destructive">
              <AlertTriangle className="h-4 w-4" />
              <AlertTitle>Error</AlertTitle>
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          ) : null}

          {meta && !tablesReady ? (
            <Alert>
              <Info className="h-4 w-4" />
              <AlertTitle>Registro V2 aún no habilitado</AlertTitle>
              <AlertDescription>
                Las tablas de Promociones V2 no existen todavía en esta base de datos (migración pendiente de autorización). Solo se muestran las
                promociones del módulo anterior en modo lectura, y Márgenes V2 no descarta alertas por promociones.
              </AlertDescription>
            </Alert>
          ) : null}

          {meta ? (
            <Alert>
              <Info className="h-4 w-4" />
              <AlertTitle>Cómo se verifica una promoción</AlertTitle>
              <AlertDescription>
                Registrar o aprobar una campaña no prueba que el precio esté aplicado. Una oferta o remate solo excluye la alerta de margen cuando
                está aprobada, vigente hoy, en la misma empresa, variante y lista, sin conflicto con otra campaña, y su precio coincide con el precio
                Bsale leído hace menos de {meta.price_max_age_days} días. Al terminar la fecha deja de aplicarse sola. Los riesgos de costo (venta
                bajo costo, costo sospechoso o sin costo válido) nunca se ocultan.
              </AlertDescription>
            </Alert>
          ) : null}

          <PromotionV2Kpis summary={summary} loading={loadingSummary} onOpen={(s) => changeTab(s)} />

          <form
            className="flex flex-wrap items-end gap-2 rounded-md border p-3"
            onSubmit={(e) => {
              e.preventDefault()
              applyFilters()
            }}
          >
            <label className={cn(labelClass, "min-w-[220px] flex-1")}>
              Producto o campaña
              <Input value={draft.q} maxLength={100} onChange={(e) => setDraft((d) => ({ ...d, q: e.target.value }))} placeholder="SKU, código o nombre" />
            </label>
            <label className={labelClass}>
              Lista
              <select className={selectClass} value={draft.priceListId} onChange={(e) => setDraft((d) => ({ ...d, priceListId: e.target.value }))}>
                <option value="">Todas las evaluadas</option>
                {(filters?.price_lists ?? []).map((l) => (
                  <option key={l.id} value={l.id}>
                    {l.label ?? l.name}
                  </option>
                ))}
              </select>
            </label>
            <label className={labelClass}>
              Tipo de campaña
              <select className={selectClass} value={draft.kind} onChange={(e) => setDraft((d) => ({ ...d, kind: e.target.value as CampaignKind | "" }))}>
                <option value="">Todos</option>
                {(Object.keys(KIND_LABELS) as CampaignKind[]).map((k) => (
                  <option key={k} value={k}>
                    {KIND_LABELS[k]}
                  </option>
                ))}
              </select>
            </label>
            <label className={labelClass}>
              Aprobación
              <select
                className={selectClass}
                value={draft.approval}
                onChange={(e) => setDraft((d) => ({ ...d, approval: e.target.value as CampaignApproval | "" }))}
              >
                <option value="">Todas</option>
                {(Object.keys(APPROVAL_LABELS) as CampaignApproval[]).map((a) => (
                  <option key={a} value={a}>
                    {APPROVAL_LABELS[a]}
                  </option>
                ))}
              </select>
            </label>
            <label className={labelClass}>
              Vigencia
              <select
                className={selectClass}
                value={draft.timeline}
                disabled={tab !== "resumen"}
                onChange={(e) => setDraft((d) => ({ ...d, timeline: e.target.value as CampaignTimeline | "" }))}
              >
                <option value="">Todas</option>
                {(Object.keys(TIMELINE_LABELS) as CampaignTimeline[]).map((t) => (
                  <option key={t} value={t}>
                    {TIMELINE_LABELS[t]}
                  </option>
                ))}
              </select>
            </label>
            <div className="flex gap-2">
              <Button type="submit" disabled={loadingList}>
                {loadingList ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                Aplicar
              </Button>
              <Button type="button" variant="outline" disabled={loadingList} onClick={() => applyFilters(EMPTY_FILTERS)}>
                Limpiar
              </Button>
            </div>
          </form>

          <Tabs value={tab} onValueChange={changeTab} className="gap-3">
            <TabsList className="flex h-auto flex-wrap justify-start">
              <TabsTrigger value="resumen">Resumen mensual</TabsTrigger>
              <TabsTrigger value="vigentes">{SECTION_LABELS.vigentes}</TabsTrigger>
              <TabsTrigger value="remates">{SECTION_LABELS.remates}</TabsTrigger>
              <TabsTrigger value="proximas">{SECTION_LABELS.proximas}</TabsTrigger>
              <TabsTrigger value="historial">Historial</TabsTrigger>
              <TabsTrigger value="pendientes">
                {SECTION_LABELS.pendientes}
                {summary && summary.kpis.exceptions_pending + summary.kpis.campaigns_pending_approval > 0 ? (
                  <span className="ml-1 rounded-full bg-orange-100 px-1.5 text-[11px] text-orange-800 dark:bg-orange-950/60 dark:text-orange-300">
                    {summary.kpis.exceptions_pending + summary.kpis.campaigns_pending_approval}
                  </span>
                ) : null}
              </TabsTrigger>
              <TabsTrigger value="anteriores">Promociones anteriores</TabsTrigger>
              <TabsTrigger value="packs">Packs Bsale</TabsTrigger>
            </TabsList>

            <TabsContent value="resumen" className="space-y-3">
              <PromotionV2MonthOverview summary={summary} />
              <h2 className="text-sm font-semibold capitalize">Campañas de {monthLabel(month)}</h2>
              <PromotionCampaignsTable campaigns={campaigns?.items ?? []} loading={loadingList} onOpen={setCampaignId} />
            </TabsContent>

            {ITEM_SECTIONS.map((s) => (
              <TabsContent key={s} value={s} className="space-y-3">
                {s === "vigentes" || s === "remates" || s === "pendientes" ? (
                  <p className="text-xs text-muted-foreground">Estado a hoy, independiente del mes seleccionado.</p>
                ) : null}
                {s === "remates" ? (
                  <p className="text-xs text-muted-foreground">
                    Incluye remates vigentes y programados en cualquier estado de aprobación. Un remate registrado no prueba que su precio esté en
                    Bsale.
                  </p>
                ) : null}
                <PromotionItemsTable items={tab === s ? (items?.items ?? []) : []} loading={loadingList} emptyText={EMPTY_TEXT[s]} onOpenCampaign={setCampaignId} />
              </TabsContent>
            ))}

            <TabsContent value="historial" className="space-y-3">
              <p className="text-xs text-muted-foreground">
                Registro permanente de cambios de {monthLabel(month)}: altas, cambios de precio, aprobaciones, rechazos, cancelaciones y retiros. No se
                puede editar ni borrar.
              </p>
              <PromotionHistoryTable events={history?.items ?? []} loading={loadingList} showCampaign onOpenCampaign={setCampaignId} />
            </TabsContent>

            <TabsContent value="anteriores">
              <PromotionLegacyPanel
                companyId={companyId}
                canImport={Boolean(caps?.create)}
                tablesReady={tablesReady}
                onOpenCampaign={setCampaignId}
                onImported={refresh}
              />
            </TabsContent>

            <TabsContent value="packs">
              {tab === "packs" ? <PromotionPacksInfo companyId={companyId} /> : null}
            </TabsContent>
          </Tabs>

          {paged && tab !== "anteriores" ? (
            <Pager page={paged.page} totalPages={paged.total_pages} total={paged.total} loading={loadingList} onPage={setPage} />
          ) : null}
        </>
      )}

      <PromotionCampaignSheet
        companyId={companyId}
        campaignId={campaignId}
        onOpenChange={(open) => {
          if (!open) setCampaignId(null)
        }}
        onChanged={refresh}
      />
      <CampaignFormDialog
        companyId={companyId}
        open={createOpen}
        campaign={null}
        defaultKind="oferta"
        onOpenChange={setCreateOpen}
        onSaved={(c) => {
          refresh()
          setCampaignId(c.id)
        }}
      />
    </div>
  )
}
