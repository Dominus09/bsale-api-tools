"use client"

import { useCallback, useEffect, useState } from "react"
import { Loader2 } from "lucide-react"

import {
  ProfessionalDataTable,
  SectionCard,
  SmartFilterBar,
  StatusBadge,
} from "@/components/analytics-v2/shared/analytics-ui"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from "@/components/ui/sheet"
import { Textarea } from "@/components/ui/textarea"
import {
  AnalyticsV2Error,
  discardCostReview,
  getCostReview,
  getCostReviews,
  resolveCostReview,
  startCostReview,
  updateCostReviewObservations,
  updateCostReviewPriority,
} from "@/lib/analytics-v2/api"
import { ANOMALY_LABELS, formatDateTime, formatMoney, formatPct } from "@/lib/analytics-v2/labels"
import type {
  CostReviewDetailResponse,
  CostReviewItem,
  CostReviewPriority,
  CostReviewStatus,
  CostReviewsListResponse,
} from "@/lib/analytics-v2/types"
import { hasManagementAccess } from "@/lib/permissions"
import { useAuthMe } from "@/lib/use-auth-me"
import { cn } from "@/lib/utils"

import { Pager } from "./cost-v2-products"

const selectClass = "h-9 rounded-md border border-input bg-background px-2 text-sm"

const STATUS_LABELS: Record<CostReviewStatus, string> = {
  pendiente: "Pendiente",
  en_revision: "En revisión",
  resuelto: "Resuelto",
  descartado: "Descartado",
}

const STATUS_TONES: Record<CostReviewStatus, "neutral" | "warning" | "success" | "danger"> = {
  pendiente: "warning",
  en_revision: "neutral",
  resuelto: "success",
  descartado: "danger",
}

const PRIORITY_LABELS: Record<CostReviewPriority, string> = {
  alta: "Alta",
  media: "Media",
  baja: "Baja",
}

const SORT_OPTIONS = [
  { value: "status", label: "Estado (operativo)" },
  { value: "updated_at_desc", label: "Última actualización ↓" },
  { value: "updated_at_asc", label: "Última actualización ↑" },
  { value: "priority_desc", label: "Prioridad" },
  { value: "started_at_desc", label: "Inicio" },
  { value: "product_name", label: "Producto" },
] as const

function moneyNum(v: number | null | undefined): string {
  return v == null ? "—" : formatMoney(v)
}

function anomalyLabel(code: string): string {
  return (ANOMALY_LABELS as Record<string, string>)[code] ?? code
}

function ReviewDetailSheet({
  companyId,
  reviewId,
  open,
  onOpenChange,
  canMutate,
  onChanged,
  onOpenProduct,
}: {
  companyId: number
  reviewId: number | null
  open: boolean
  onOpenChange: (open: boolean) => void
  canMutate: boolean
  onChanged: () => void
  onOpenProduct: (variantId: number) => void
}) {
  const [detail, setDetail] = useState<CostReviewDetailResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [observations, setObservations] = useState("")
  const [priority, setPriority] = useState<CostReviewPriority>("media")
  const [conclusion, setConclusion] = useState("")
  const [busy, setBusy] = useState(false)
  const [savedFlash, setSavedFlash] = useState<string | null>(null)

  const dirty =
    detail != null &&
    !["resuelto", "descartado"].includes(detail.status) &&
    (observations !== (detail.observations ?? "") || priority !== detail.priority)

  const reload = useCallback(() => {
    if (reviewId == null) return
    const ac = new AbortController()
    setLoading(true)
    setError(null)
    getCostReview(companyId, reviewId, ac.signal)
      .then((d) => {
        setDetail(d)
        setObservations(d.observations ?? "")
        setPriority(d.priority)
        setConclusion(d.conclusion ?? "")
      })
      .catch((e: unknown) => {
        if (!ac.signal.aborted) setError(e instanceof Error ? e.message : "Error al cargar")
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoading(false)
      })
    return () => ac.abort()
  }, [companyId, reviewId])

  useEffect(() => {
    if (!open || reviewId == null) {
      setDetail(null)
      return
    }
    return reload()
  }, [open, reviewId, reload])

  useEffect(() => {
    if (!dirty) return
    const onBeforeUnload = (e: BeforeUnloadEvent) => {
      e.preventDefault()
      e.returnValue = ""
    }
    window.addEventListener("beforeunload", onBeforeUnload)
    return () => window.removeEventListener("beforeunload", onBeforeUnload)
  }, [dirty])

  const requestClose = (nextOpen: boolean) => {
    if (!nextOpen && dirty) {
      const ok = window.confirm("Hay cambios sin guardar. ¿Cerrar de todos modos?")
      if (!ok) return
    }
    onOpenChange(nextOpen)
  }

  const run = async (fn: () => Promise<unknown>, flash?: string) => {
    setBusy(true)
    setError(null)
    setSavedFlash(null)
    try {
      await fn()
      if (flash) setSavedFlash(flash)
      onChanged()
      reload()
    } catch (e: unknown) {
      setError(e instanceof AnalyticsV2Error ? e.message : e instanceof Error ? e.message : "Error")
    } finally {
      setBusy(false)
    }
  }

  const saveInline = async () => {
    if (!detail) return
    setBusy(true)
    setError(null)
    setSavedFlash(null)
    try {
      if (priority !== detail.priority) {
        await updateCostReviewPriority(companyId, detail.id, priority)
      }
      if (observations !== (detail.observations ?? "")) {
        await updateCostReviewObservations(companyId, detail.id, observations)
      }
      setSavedFlash("Cambios guardados")
      onChanged()
      reload()
    } catch (e: unknown) {
      setError(e instanceof AnalyticsV2Error ? e.message : e instanceof Error ? e.message : "Error al guardar")
    } finally {
      setBusy(false)
    }
  }

  const closed = detail?.status === "resuelto" || detail?.status === "descartado"

  return (
    <Sheet open={open} onOpenChange={requestClose}>
      <SheetContent side="right" className="w-full overflow-y-auto border-l-lq-line bg-lq-canvas p-0 sm:max-w-[min(720px,94vw)]">
        <SheetHeader className="sticky top-0 z-20 border-b border-lq-line bg-lq-surface/95 px-5 py-4 backdrop-blur">
          <SheetTitle className="pr-8 text-xl tracking-[-0.015em]">
            {detail?.product_name ?? (loading ? "Cargando…" : "Revisión de costo")}
          </SheetTitle>
          <SheetDescription>
            Caso humano. Los costos mostrados son una fotografía al crear el caso; no se editan costos Bsale.
          </SheetDescription>
        </SheetHeader>

        <div className="space-y-5 px-5 py-4 md:px-6">
          {error ? <p className="text-sm text-destructive">{error}</p> : null}
          {savedFlash ? <p className="text-sm text-emerald-700">{savedFlash}</p> : null}
          {loading && !detail ? (
            <div className="flex justify-center py-10">
              <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
            </div>
          ) : null}

          {detail ? (
            <>
              <div className="flex flex-wrap items-center gap-2">
                <StatusBadge tone={STATUS_TONES[detail.status]}>{STATUS_LABELS[detail.status]}</StatusBadge>
                <StatusBadge tone="warning">Revisión humana</StatusBadge>
                {dirty ? <StatusBadge tone="danger">Sin guardar</StatusBadge> : null}
              </div>

              <dl className="grid gap-3 rounded-xl border border-lq-line bg-lq-surface p-4 text-sm sm:grid-cols-2">
                <div>
                  <dt className="text-[11px] uppercase text-muted-foreground">Empresa</dt>
                  <dd>{detail.company_name || detail.company?.name || `Empresa ${detail.company_id}`}</dd>
                </div>
                <div>
                  <dt className="text-[11px] uppercase text-muted-foreground">Motivo</dt>
                  <dd>{detail.reason_label || anomalyLabel(detail.anomaly_code)}</dd>
                </div>
                <div>
                  <dt className="text-[11px] uppercase text-muted-foreground">SEC / SKU</dt>
                  <dd className="font-mono text-xs">{detail.sku_code || "—"}</dd>
                </div>
                <div>
                  <dt className="text-[11px] uppercase text-muted-foreground">Código de barras</dt>
                  <dd className="font-mono text-xs">{detail.barcode || "—"}</dd>
                </div>
                <div>
                  <dt className="text-[11px] uppercase text-muted-foreground">Responsable</dt>
                  <dd>{detail.assignee_email || "Sin asignar (aún no iniciada)"}</dd>
                </div>
                <div>
                  <dt className="text-[11px] uppercase text-muted-foreground">Costo actual (foto)</dt>
                  <dd className="tabular-nums">{moneyNum(detail.current_cost_ref)}</dd>
                </div>
                <div>
                  <dt className="text-[11px] uppercase text-muted-foreground">Costo anterior (foto)</dt>
                  <dd className="tabular-nums">{moneyNum(detail.previous_cost_ref)}</dd>
                </div>
                <div>
                  <dt className="text-[11px] uppercase text-muted-foreground">Variación (foto)</dt>
                  <dd className="tabular-nums">
                    {detail.change_pct_ref != null ? formatPct(detail.change_pct_ref) : "—"}
                  </dd>
                </div>
              </dl>

              <div className="flex flex-wrap gap-2">
                <Button type="button" variant="outline" size="sm" onClick={() => onOpenProduct(detail.variant_id)}>
                  Abrir ficha del producto
                </Button>
                {canMutate && detail.status === "pendiente" ? (
                  <Button
                    type="button"
                    size="sm"
                    disabled={busy}
                    onClick={() => void run(() => startCostReview(companyId, detail.id))}
                  >
                    {busy ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                    Iniciar revisión (me asigna)
                  </Button>
                ) : null}
              </div>

              {canMutate && !closed ? (
                <section className="space-y-3 rounded-xl border border-lq-line bg-lq-surface p-4">
                  <h3 className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
                    Edición en línea
                  </h3>
                  <label className="flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground">
                    Prioridad
                    <select
                      className={selectClass}
                      value={priority}
                      onChange={(e) => setPriority(e.target.value as CostReviewPriority)}
                    >
                      {(Object.keys(PRIORITY_LABELS) as CostReviewPriority[]).map((k) => (
                        <option key={k} value={k}>
                          {PRIORITY_LABELS[k]}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground">
                    Observaciones
                    <Textarea
                      value={observations}
                      onChange={(e) => setObservations(e.target.value)}
                      rows={3}
                      placeholder="Notas de seguimiento (no son la conclusión de cierre)"
                    />
                  </label>
                  <Button type="button" size="sm" disabled={busy || !dirty} onClick={() => void saveInline()}>
                    {busy ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                    Guardar cambios
                  </Button>
                </section>
              ) : (
                <>
                  <p className="text-sm">
                    <span className="text-[11px] uppercase text-muted-foreground">Prioridad · </span>
                    {PRIORITY_LABELS[detail.priority]}
                  </p>
                  {detail.observations ? (
                    <section>
                      <h3 className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
                        Observaciones
                      </h3>
                      <p className="mt-1 whitespace-pre-wrap text-sm">{detail.observations}</p>
                    </section>
                  ) : null}
                </>
              )}

              {canMutate && !closed ? (
                <section className="space-y-2 rounded-xl border border-lq-line bg-lq-surface p-4">
                  <h3 className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
                    Acciones de estado (conclusión obligatoria)
                  </h3>
                  <p className="text-xs text-muted-foreground">
                    Resolver o descartar no modifica el costo de origen en Bsale. La fotografía del caso se conserva.
                  </p>
                  <Textarea
                    value={conclusion}
                    onChange={(e) => setConclusion(e.target.value)}
                    rows={3}
                    placeholder="Conclusión mínima 5 caracteres"
                  />
                  <div className="flex flex-wrap gap-2">
                    <Button
                      type="button"
                      size="sm"
                      disabled={busy || conclusion.trim().length < 5 || dirty}
                      title={dirty ? "Guarde los cambios antes de cerrar" : undefined}
                      onClick={() =>
                        void run(() => resolveCostReview(companyId, detail.id, { conclusion: conclusion.trim() }))
                      }
                    >
                      Resolver
                    </Button>
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      disabled={busy || conclusion.trim().length < 5 || dirty}
                      title={dirty ? "Guarde los cambios antes de cerrar" : undefined}
                      onClick={() =>
                        void run(() => discardCostReview(companyId, detail.id, { conclusion: conclusion.trim() }))
                      }
                    >
                      Descartar
                    </Button>
                  </div>
                </section>
              ) : detail.conclusion ? (
                <section>
                  <h3 className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
                    Conclusión
                  </h3>
                  <p className="mt-1 whitespace-pre-wrap text-sm">{detail.conclusion}</p>
                </section>
              ) : null}

              <section className="space-y-2">
                <h3 className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">
                  Historial de cambios
                </h3>
                <ul className="space-y-2">
                  {(detail.events ?? []).map((ev) => (
                    <li key={ev.id} className="rounded-lg border border-lq-line bg-lq-surface px-3 py-2 text-sm">
                      <div className="flex flex-wrap items-baseline justify-between gap-2">
                        <span className="font-medium">{ev.action}</span>
                        <span className="text-[11px] text-muted-foreground">{formatDateTime(ev.created_at)}</span>
                      </div>
                      <p className="text-xs text-muted-foreground">
                        {ev.actor_email || "sistema"}
                        {ev.status_from || ev.status_to
                          ? ` · ${ev.status_from ?? "—"} → ${ev.status_to ?? "—"}`
                          : ""}
                      </p>
                      {ev.note ? <p className="mt-1 text-xs">{ev.note}</p> : null}
                    </li>
                  ))}
                  {(detail.events ?? []).length === 0 ? (
                    <li className="text-sm text-muted-foreground">Sin eventos registrados.</li>
                  ) : null}
                </ul>
              </section>
            </>
          ) : null}
        </div>
      </SheetContent>
    </Sheet>
  )
}

function InlinePriorityCell({
  companyId,
  row,
  canMutate,
  onSaved,
}: {
  companyId: number
  row: CostReviewItem
  canMutate: boolean
  onSaved: () => void
}) {
  const [value, setValue] = useState(row.priority)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const closed = row.status === "resuelto" || row.status === "descartado"

  useEffect(() => {
    setValue(row.priority)
  }, [row.priority, row.id])

  if (!canMutate || closed) {
    return <span className="text-xs">{PRIORITY_LABELS[row.priority]}</span>
  }

  return (
    <div className="space-y-1">
      <select
        className={cn(selectClass, "h-8 text-xs")}
        value={value}
        disabled={busy}
        onChange={(e) => {
          const next = e.target.value as CostReviewPriority
          setValue(next)
          if (next === row.priority) return
          setBusy(true)
          setErr(null)
          updateCostReviewPriority(companyId, row.id, next)
            .then(() => onSaved())
            .catch((ex: unknown) => {
              setValue(row.priority)
              setErr(ex instanceof Error ? ex.message : "Error")
            })
            .finally(() => setBusy(false))
        }}
      >
        {(Object.keys(PRIORITY_LABELS) as CostReviewPriority[]).map((k) => (
          <option key={k} value={k}>
            {PRIORITY_LABELS[k]}
          </option>
        ))}
      </select>
      {err ? <p className="text-[10px] text-destructive">{err}</p> : null}
    </div>
  )
}

function InlineObservationsCell({
  companyId,
  row,
  canMutate,
  onSaved,
}: {
  companyId: number
  row: CostReviewItem
  canMutate: boolean
  onSaved: () => void
}) {
  const [value, setValue] = useState(row.observations ?? "")
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [ok, setOk] = useState(false)
  const closed = row.status === "resuelto" || row.status === "descartado"
  const dirty = value !== (row.observations ?? "")

  useEffect(() => {
    setValue(row.observations ?? "")
    setOk(false)
  }, [row.observations, row.id])

  if (!canMutate || closed) {
    return <span className="max-w-[160px] truncate text-xs text-muted-foreground">{row.observations || "—"}</span>
  }

  return (
    <div className="min-w-[160px] space-y-1">
      <Input
        className="h-8 text-xs"
        value={value}
        disabled={busy}
        onChange={(e) => {
          setValue(e.target.value)
          setOk(false)
        }}
        onKeyDown={(e) => {
          if (e.key === "Enter" && dirty) {
            e.preventDefault()
            setBusy(true)
            setErr(null)
            updateCostReviewObservations(companyId, row.id, value)
              .then(() => {
                setOk(true)
                onSaved()
              })
              .catch((ex: unknown) => setErr(ex instanceof Error ? ex.message : "Error"))
              .finally(() => setBusy(false))
          }
        }}
      />
      <div className="flex items-center gap-1">
        <Button
          type="button"
          size="sm"
          variant="ghost"
          className="h-6 px-1 text-[10px]"
          disabled={busy || !dirty}
          onClick={() => {
            setBusy(true)
            setErr(null)
            updateCostReviewObservations(companyId, row.id, value)
              .then(() => {
                setOk(true)
                onSaved()
              })
              .catch((ex: unknown) => setErr(ex instanceof Error ? ex.message : "Error"))
              .finally(() => setBusy(false))
          }}
        >
          {busy ? <Loader2 className="h-3 w-3 animate-spin" /> : "Guardar"}
        </Button>
        {ok ? <span className="text-[10px] text-emerald-700">OK</span> : null}
        {dirty ? <span className="text-[10px] text-amber-700">Sin guardar</span> : null}
      </div>
      {err ? <p className="text-[10px] text-destructive">{err}</p> : null}
    </div>
  )
}

export function CostV2Reviews({
  companyId,
  onOpenProduct,
}: {
  companyId: number
  onOpenProduct: (variantId: number) => void
}) {
  const { meState, staff } = useAuthMe()
  const canMutate = meState === "ok" && hasManagementAccess(staff)

  const [draft, setDraft] = useState({
    status: "" as CostReviewStatus | "",
    priority: "" as CostReviewPriority | "",
    q: "",
    sort: "status",
  })
  const [applied, setApplied] = useState(draft)
  const [page, setPage] = useState(1)
  const [data, setData] = useState<CostReviewsListResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [selectedId, setSelectedId] = useState<number | null>(null)
  const [tick, setTick] = useState(0)

  const load = useCallback(() => {
    const ac = new AbortController()
    setLoading(true)
    setError(null)
    getCostReviews(
      {
        company_id: companyId,
        status: applied.status || null,
        priority: applied.priority || null,
        q: applied.q.trim() || null,
        sort: applied.sort,
        page,
        page_size: 50,
      },
      ac.signal,
    )
      .then(setData)
      .catch((e: unknown) => {
        if (!ac.signal.aborted) {
          setError(e instanceof AnalyticsV2Error ? e.message : e instanceof Error ? e.message : "Error al cargar")
        }
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoading(false)
      })
    return () => ac.abort()
  }, [companyId, applied, page, tick])

  useEffect(() => load(), [load])

  const totalPages = data ? Math.max(1, Math.ceil(data.total / data.page_size)) : 1
  const companyLabel = data?.company?.name || (data?.items[0]?.company_name ?? null)

  return (
    <div className="space-y-3">
      <SectionCard
        title="Gestión de revisiones"
        description="Casos humanos de seguimiento. Las alertas automáticas de Costos V2 no crean revisiones por sí solas."
      >
        <p className="text-xs text-muted-foreground">
          Flujo: Pendiente → En revisión → Resuelto / Descartado. Los costos son instantánea de Costos V2 al crear el
          caso. Abrir un producto no asigna responsable.
          {companyLabel ? (
            <>
              {" "}
              Empresa activa (backend): <strong>{companyLabel}</strong>.
            </>
          ) : null}
        </p>
      </SectionCard>

      <form
        onSubmit={(e) => {
          e.preventDefault()
          setPage(1)
          setApplied({ ...draft })
        }}
      >
        <SmartFilterBar>
          <div className="flex flex-wrap items-end gap-3">
            <label className="flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground">
              Estado
              <select
                className={selectClass}
                value={draft.status}
                onChange={(e) => setDraft((d) => ({ ...d, status: e.target.value as CostReviewStatus | "" }))}
              >
                <option value="">Todos</option>
                {(Object.keys(STATUS_LABELS) as CostReviewStatus[]).map((k) => (
                  <option key={k} value={k}>
                    {STATUS_LABELS[k]}
                  </option>
                ))}
              </select>
            </label>
            <label className="flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground">
              Prioridad
              <select
                className={selectClass}
                value={draft.priority}
                onChange={(e) => setDraft((d) => ({ ...d, priority: e.target.value as CostReviewPriority | "" }))}
              >
                <option value="">Todas</option>
                {(Object.keys(PRIORITY_LABELS) as CostReviewPriority[]).map((k) => (
                  <option key={k} value={k}>
                    {PRIORITY_LABELS[k]}
                  </option>
                ))}
              </select>
            </label>
            <label className="flex flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground">
              Orden
              <select
                className={selectClass}
                value={draft.sort}
                onChange={(e) => setDraft((d) => ({ ...d, sort: e.target.value }))}
              >
                {SORT_OPTIONS.map((o) => (
                  <option key={o.value} value={o.value}>
                    {o.label}
                  </option>
                ))}
              </select>
            </label>
            <label className="flex min-w-[14rem] flex-1 flex-col gap-1 text-[11px] font-medium uppercase text-muted-foreground">
              Buscar
              <Input
                value={draft.q}
                onChange={(e) => setDraft((d) => ({ ...d, q: e.target.value }))}
                placeholder="Producto, SEC, barcode o variant_id"
              />
            </label>
            <Button type="submit" disabled={loading}>
              {loading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
              Aplicar
            </Button>
          </div>
        </SmartFilterBar>
      </form>

      {error ? <p className="text-sm text-destructive">{error}</p> : null}

      <ProfessionalDataTable loading={loading}>
        <div className="overflow-x-auto">
          <table className="w-full min-w-[1280px] text-sm">
            <thead className="bg-muted/40 text-left text-[11px] uppercase tracking-wide text-muted-foreground">
              <tr>
                <th className="px-3 py-2 font-medium">Empresa</th>
                <th className="px-3 py-2 font-medium">Producto</th>
                <th className="px-3 py-2 font-medium">SEC</th>
                <th className="px-3 py-2 font-medium">Barras</th>
                <th className="px-3 py-2 font-medium">Motivo</th>
                <th className="px-3 py-2 text-right font-medium">Costo act.*</th>
                <th className="px-3 py-2 text-right font-medium">Anterior*</th>
                <th className="px-3 py-2 font-medium">Prioridad</th>
                <th className="px-3 py-2 font-medium">Estado</th>
                <th className="px-3 py-2 font-medium">Responsable</th>
                <th className="px-3 py-2 font-medium">Inicio</th>
                <th className="px-3 py-2 font-medium">Actualización</th>
                <th className="px-3 py-2 font-medium">Observaciones</th>
                <th className="px-3 py-2 font-medium">Acciones</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-border/60">
              {(data?.items ?? []).map((row: CostReviewItem) => (
                <tr key={row.id} className="align-top">
                  <td className="px-3 py-2 text-xs text-muted-foreground">
                    {row.company_name || `Empresa ${row.company_id}`}
                  </td>
                  <td className="max-w-[220px] px-3 py-2">
                    <button
                      type="button"
                      className="text-left hover:underline"
                      onClick={() => onOpenProduct(row.variant_id)}
                    >
                      {row.product_name || `Variante ${row.variant_id}`}
                    </button>
                  </td>
                  <td className="px-3 py-2 font-mono text-xs">{row.sku_code || "—"}</td>
                  <td className="px-3 py-2 font-mono text-xs">{row.barcode || "—"}</td>
                  <td className="max-w-[180px] px-3 py-2 text-xs">{row.reason_label}</td>
                  <td className="px-3 py-2 text-right tabular-nums">{moneyNum(row.current_cost_ref)}</td>
                  <td className="px-3 py-2 text-right tabular-nums">{moneyNum(row.previous_cost_ref)}</td>
                  <td className="px-3 py-2">
                    <InlinePriorityCell
                      companyId={companyId}
                      row={row}
                      canMutate={canMutate}
                      onSaved={() => setTick((t) => t + 1)}
                    />
                  </td>
                  <td className="px-3 py-2">
                    <StatusBadge tone={STATUS_TONES[row.status]}>{STATUS_LABELS[row.status]}</StatusBadge>
                  </td>
                  <td className="max-w-[140px] truncate px-3 py-2 text-xs">{row.assignee_email || "—"}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-xs">{formatDateTime(row.started_at) || "—"}</td>
                  <td className="whitespace-nowrap px-3 py-2 text-xs">{formatDateTime(row.updated_at) || "—"}</td>
                  <td className="px-3 py-2">
                    <InlineObservationsCell
                      companyId={companyId}
                      row={row}
                      canMutate={canMutate}
                      onSaved={() => setTick((t) => t + 1)}
                    />
                  </td>
                  <td className="px-3 py-2">
                    <div className="flex flex-col gap-1">
                      <Button type="button" size="sm" variant="outline" onClick={() => setSelectedId(row.id)}>
                        Abrir
                      </Button>
                      {canMutate && row.status === "pendiente" ? (
                        <Button
                          type="button"
                          size="sm"
                          disabled={loading}
                          onClick={() => {
                            void startCostReview(companyId, row.id)
                              .then(() => setTick((t) => t + 1))
                              .catch((e: unknown) =>
                                setError(e instanceof Error ? e.message : "No se pudo iniciar"),
                              )
                          }}
                        >
                          Iniciar
                        </Button>
                      ) : null}
                    </div>
                  </td>
                </tr>
              ))}
              {data && data.items.length === 0 ? (
                <tr>
                  <td colSpan={14} className="px-3 py-8 text-center text-muted-foreground">
                    Sin casos de revisión. Puede iniciarlos desde Alertas o Análisis de recepciones.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </ProfessionalDataTable>
      <p className="text-[11px] text-muted-foreground">
        * Costos de referencia: fotografía al crear el caso (Costos V2). No editables; no reflejan syncs posteriores.
      </p>

      {data ? (
        <Pager page={data.page} totalPages={totalPages} total={data.total} loading={loading} onPage={setPage} />
      ) : null}

      <ReviewDetailSheet
        companyId={companyId}
        reviewId={selectedId}
        open={selectedId != null}
        onOpenChange={(o) => {
          if (!o) setSelectedId(null)
        }}
        canMutate={canMutate}
        onChanged={() => setTick((t) => t + 1)}
        onOpenProduct={onOpenProduct}
      />
    </div>
  )
}
