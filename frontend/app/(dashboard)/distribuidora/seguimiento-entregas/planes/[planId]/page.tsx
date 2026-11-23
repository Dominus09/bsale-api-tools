"use client"

/**
 * Detalle de despacho — Centro de control (solo lectura).
 */

import { Fragment, useCallback, useEffect, useMemo, useState } from "react"
import Link from "next/link"
import { useParams } from "next/navigation"
import {
  ArrowLeft,
  ChevronDown,
  ChevronRight,
  Eye,
  Loader2,
  RefreshCw,
} from "lucide-react"

import {
  getSeguimientoPlanClientes,
  getSeguimientoVisitDetail,
  type DeliveryVisitErpDetail,
  type SeguimientoClientRow,
  type SeguimientoProductRow,
} from "@/lib/api"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Skeleton } from "@/components/ui/skeleton"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { cn } from "@/lib/utils"
import {
  TONE_BADGE,
  TONE_DOT,
  clientResultLabel,
  resultTone,
} from "@/components/distribuidora/seguimiento-entregas/status"

const FILTERS = [
  { value: "", label: "Todos" },
  { value: "completed", label: "Entregados" },
  { value: "partial", label: "Parciales" },
  { value: "not_delivered", label: "No entregados" },
  { value: "pending", label: "Pendientes" },
]

function productRowKey(p: SeguimientoProductRow, idx: number): string {
  if (p.source_detail_id != null) return `d-${p.source_detail_id}`
  if (p.id != null) return `id-${p.id}`
  if (p.variant_id != null) return `v-${p.variant_id}-${idx}`
  return `p-${idx}`
}

function ProductIdentity({ p }: { p: SeguimientoProductRow }) {
  const commercial = p.commercial_name || p.product_name || "Producto"
  const presentation = p.presentation || p.variant_name
  const meta: string[] = []
  if (p.product_type) meta.push(p.product_type)
  if (presentation) meta.push(presentation)
  if (p.units_per_box != null && p.units_per_box > 0) {
    meta.push(`${p.units_per_box} u/caja`)
  }
  if (p.sku_code) meta.push(`SKU ${p.sku_code}`)
  return (
    <div className="min-w-0">
      <p className="font-medium text-lq-on-graphite">{commercial}</p>
      {meta.length > 0 ? (
        <p className="mt-0.5 text-[13px] leading-snug text-lq-on-graphite-muted">
          {meta.join(" · ")}
        </p>
      ) : null}
      <p className="mt-1 tabular-nums text-sm text-lq-on-graphite">
        {p.delivered_units ?? 0}/{p.planned_units ?? "—"} und.
        {p.undelivered_reason ? (
          <span className="ml-2 text-lq-on-graphite-muted">
            ({p.undelivered_reason})
          </span>
        ) : null}
      </p>
    </div>
  )
}

export default function SeguimientoPlanClientesPage() {
  const params = useParams()
  const planId = Number(params?.planId)

  const [status, setStatus] = useState("")
  const [clientInput, setClientInput] = useState("")
  const [clientQ, setClientQ] = useState("")
  const [plan, setPlan] = useState<{
    id: number
    planning_code?: string
    planning_date?: string
    route_name?: string | null
    driver_name?: string | null
    vehicle_label?: string | null
  } | null>(null)
  const [clients, setClients] = useState<SeguimientoClientRow[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [expanded, setExpanded] = useState<Record<string, boolean>>({})

  const [detailOpen, setDetailOpen] = useState(false)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailError, setDetailError] = useState<string | null>(null)
  const [detail, setDetail] = useState<DeliveryVisitErpDetail | null>(null)
  const [selected, setSelected] = useState<SeguimientoClientRow | null>(null)

  const load = useCallback(async () => {
    if (!Number.isFinite(planId) || planId <= 0) return
    setLoading(true)
    setError(null)
    try {
      const res = await getSeguimientoPlanClientes(planId, {
        status: status || undefined,
        client_q: clientQ || undefined,
      })
      setPlan(res.plan)
      setClients(res.clients || [])
    } catch (e) {
      setError(e instanceof Error ? e.message : "Error al cargar")
    } finally {
      setLoading(false)
    }
  }, [planId, status, clientQ])

  useEffect(() => {
    void load()
  }, [load])

  const summary = useMemo(() => {
    const total = clients.length
    const completed = clients.filter((c) => c.result_status === "completed").length
    const partial = clients.filter((c) => c.result_status === "partial").length
    const notDel = clients.filter((c) => c.result_status === "not_delivered").length
    const pending = clients.filter(
      (c) => !c.result_status || c.result_status === "pending" || c.result_status === "in_progress",
    ).length
    const managed = completed + partial + notDel
    const pct = total <= 0 ? 0 : Math.round((1000 * managed) / total) / 10
    const times = clients
      .map((c) => c.managed_at_local)
      .filter((t): t is string => !!t)
      .sort()
    return {
      total,
      completed,
      partial,
      notDel,
      pending,
      managed,
      pct,
      first: times[0] || null,
      last: times.length ? times[times.length - 1] : null,
    }
  }, [clients])

  async function openEvidence(c: SeguimientoClientRow) {
    if (!c.visit_id) return
    setSelected(c)
    setDetailOpen(true)
    setDetail(null)
    setDetailError(null)
    setDetailLoading(true)
    try {
      const res = await getSeguimientoVisitDetail(c.visit_id)
      setDetail(res)
    } catch (e) {
      setDetailError(e instanceof Error ? e.message : "Error al cargar evidencia")
    } finally {
      setDetailLoading(false)
    }
  }

  return (
    <div className="-mx-4 min-h-[calc(100vh-4rem)] bg-lq-graphite-deep px-4 py-5 text-lq-on-graphite sm:-mx-6 sm:px-6 lg:px-8">
      <div className="mx-auto max-w-[1400px] space-y-5">
        <Link
          href="/distribuidora/seguimiento-entregas"
          className="inline-flex items-center gap-1.5 text-sm text-lq-on-graphite-muted transition-colors hover:text-lq-on-graphite"
        >
          <ArrowLeft className="h-4 w-4" /> Centro de control
        </Link>

        <header className="rounded-2xl border border-white/10 bg-lq-graphite p-5 ring-1 ring-black/25">
          <div className="flex flex-wrap items-start justify-between gap-4">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.14em] text-[var(--lq-brand)]">
                Despacho
              </p>
              <h1 className="mt-1 text-3xl font-semibold tracking-[-0.03em]">
                {plan?.planning_code || `Despacho ${planId}`}
              </h1>
              <p className="mt-2 text-base text-lq-on-graphite-muted">
                {[plan?.driver_name, plan?.vehicle_label, plan?.route_name, plan?.planning_date]
                  .filter(Boolean)
                  .join(" · ") || "Cargando…"}
              </p>
            </div>
            <Button
              variant="outline"
              size="sm"
              onClick={() => void load()}
              disabled={loading}
              className="border-white/20 bg-white/5 text-lq-on-graphite hover:bg-white/10"
            >
              {loading ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <RefreshCw className="h-4 w-4" />
              )}
              <span className="ml-2">Actualizar</span>
            </Button>
          </div>

          <div className="mt-5 grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
            <div className="rounded-xl border border-white/10 bg-black/25 px-4 py-3">
              <p className="text-[11px] font-semibold uppercase tracking-wider text-lq-on-graphite-muted">
                Avance
              </p>
              <p className="mt-1 text-3xl font-semibold tabular-nums">{summary.pct}%</p>
              <div className="mt-2 h-2 overflow-hidden rounded-full bg-black/40">
                <div
                  className="h-full rounded-full bg-[var(--lq-brand)]"
                  style={{ width: `${Math.min(100, summary.pct)}%` }}
                />
              </div>
              <p className="mt-2 text-sm text-lq-on-graphite-muted">
                {summary.managed}/{summary.total} gestionados
              </p>
            </div>
            <div className="rounded-xl border border-white/10 bg-black/25 px-4 py-3">
              <p className="text-[11px] font-semibold uppercase tracking-wider text-lq-on-graphite-muted">
                Resultados
              </p>
              <div className="mt-2 grid grid-cols-3 gap-2 text-center">
                <div>
                  <p className="text-xl font-semibold text-emerald-300 tabular-nums">
                    {summary.completed}
                  </p>
                  <p className="text-[11px] text-lq-on-graphite-muted">Completas</p>
                </div>
                <div>
                  <p className="text-xl font-semibold text-amber-300 tabular-nums">
                    {summary.partial}
                  </p>
                  <p className="text-[11px] text-lq-on-graphite-muted">Parciales</p>
                </div>
                <div>
                  <p className="text-xl font-semibold text-red-300 tabular-nums">
                    {summary.notDel}
                  </p>
                  <p className="text-[11px] text-lq-on-graphite-muted">No ent.</p>
                </div>
              </div>
            </div>
            <div className="rounded-xl border border-white/10 bg-black/25 px-4 py-3">
              <p className="text-[11px] font-semibold uppercase tracking-wider text-lq-on-graphite-muted">
                Primera entrega
              </p>
              <p className="mt-2 text-[1.65rem] font-semibold leading-none tabular-nums tracking-tight">
                {summary.first || "—"}
              </p>
              <p className="mt-2 text-sm text-lq-on-graphite-muted">
                {summary.first
                  ? "Hora de gestión confirmada"
                  : "Sin entregas registradas aún"}
              </p>
            </div>
            <div className="rounded-xl border border-white/10 bg-black/25 px-4 py-3">
              <p className="text-[11px] font-semibold uppercase tracking-wider text-lq-on-graphite-muted">
                Última gestión
              </p>
              <p className="mt-2 text-[1.65rem] font-semibold leading-none tabular-nums tracking-tight">
                {summary.last || "—"}
              </p>
              <p className="mt-2 text-sm text-lq-on-graphite-muted">
                Pendientes: {summary.pending}
              </p>
            </div>
          </div>
        </header>

        <div className="flex flex-wrap items-center gap-2">
          {FILTERS.map((f) => (
            <button
              key={f.value || "all"}
              type="button"
              onClick={() => setStatus(f.value)}
              className={cn(
                "rounded-full border px-3 py-1.5 text-sm transition-colors",
                status === f.value
                  ? "border-[var(--lq-brand)] bg-[var(--lq-brand)]/20 text-white"
                  : "border-white/15 bg-white/5 text-lq-on-graphite-muted hover:bg-white/10",
              )}
            >
              {f.label}
            </button>
          ))}
          <div className="ml-auto flex gap-2">
            <Input
              value={clientInput}
              onChange={(e) => setClientInput(e.target.value)}
              placeholder="Buscar cliente"
              className="w-[200px] border-white/15 bg-black/30 text-lq-on-graphite"
              onKeyDown={(e) => {
                if (e.key === "Enter") setClientQ(clientInput.trim())
              }}
            />
            <Button
              className="bg-[var(--lq-brand)] text-white hover:bg-[var(--lq-brand-strong)]"
              onClick={() => setClientQ(clientInput.trim())}
            >
              Filtrar
            </Button>
          </div>
        </div>

        {error ? (
          <div className="rounded-xl border border-red-500/40 bg-red-500/10 px-4 py-3 text-sm text-red-200">
            {error}
          </div>
        ) : null}

        {loading && clients.length === 0 ? (
          <div className="space-y-2">
            {Array.from({ length: 5 }).map((_, i) => (
              <Skeleton key={i} className="h-20 rounded-xl bg-white/5" />
            ))}
          </div>
        ) : clients.length === 0 ? (
          <div className="rounded-2xl border border-dashed border-white/15 bg-lq-graphite px-6 py-12 text-center">
            <p className="text-base font-medium">
              {status || clientQ
                ? "No hay clientes con ese filtro."
                : "Este despacho no tiene clientes en el plan de carga."}
            </p>
            <p className="mt-1 text-sm text-lq-on-graphite-muted">
              {status || clientQ
                ? "Prueba con otro estado o limpia la búsqueda."
                : "Cuando el plan tenga clientes asignados, aparecerán aquí incluso sin gestiones."}
            </p>
          </div>
        ) : (
          <div className="space-y-2">
            {clients.map((c) => {
              const open = !!expanded[c.customer_key]
              const tone = resultTone(c.result_status, c.requires_review)
              return (
                <Fragment key={c.customer_key}>
                  <article className="rounded-xl border border-white/10 bg-lq-graphite px-4 py-3 transition-colors hover:bg-lq-graphite-elev">
                    <div className="flex flex-wrap items-center gap-3">
                      <button
                        type="button"
                        className="text-lq-on-graphite-muted"
                        onClick={() =>
                          setExpanded((prev) => ({
                            ...prev,
                            [c.customer_key]: !prev[c.customer_key],
                          }))
                        }
                        aria-label="Expandir documentos"
                      >
                        {open ? (
                          <ChevronDown className="h-4 w-4" />
                        ) : (
                          <ChevronRight className="h-4 w-4" />
                        )}
                      </button>
                      <span className="w-8 text-center text-sm tabular-nums text-lq-on-graphite-muted">
                        {c.route_order ?? "—"}
                      </span>
                      <span className={cn("h-2.5 w-2.5 rounded-full", TONE_DOT[tone])} />
                      <div className="min-w-0 flex-1">
                        <p className="truncate text-[15px] font-medium text-lq-on-graphite">
                          {c.commercial_name || "—"}
                        </p>
                        <p className="text-sm text-lq-on-graphite-muted">
                          {c.document_count ?? 0} doc.
                          {c.observation || c.result_reason_es
                            ? ` · ${c.observation || c.result_reason_es}`
                            : !c.visit_id
                              ? " · Sin gestión aún"
                              : ""}
                        </p>
                      </div>
                      <span
                        className={cn(
                          "rounded-full border px-2.5 py-0.5 text-xs font-medium",
                          TONE_BADGE[tone],
                        )}
                      >
                        {clientResultLabel(c.result_status, c.result_reason_es)}
                      </span>
                      <div className="min-w-[7.5rem] text-right text-sm text-lq-on-graphite-muted">
                        <p className="tabular-nums text-lq-on-graphite">
                          {c.managed_at_local || "—"}
                        </p>
                        <p className="text-[13px]">
                          {c.device_capture
                            ? "dispositivo"
                            : c.managed_at_source === "server"
                              ? "servidor"
                              : "—"}
                          {c.synced_at_local ? ` · sync ${c.synced_at_local}` : ""}
                        </p>
                        <p
                          className={cn(
                            "text-[13px]",
                            c.requires_review ? "text-sky-300" : "text-lq-on-graphite-muted",
                          )}
                        >
                          {c.gps_status_es ||
                            (c.requires_review
                              ? "GPS pendiente de revisión"
                              : c.visit_id
                                ? "GPS registrado"
                                : "Sin gestión")}
                        </p>
                      </div>
                      {c.visit_id ? (
                        <Button
                          variant="ghost"
                          size="sm"
                          className="text-lq-on-graphite hover:bg-white/10"
                          onClick={() => void openEvidence(c)}
                        >
                          <Eye className="mr-1.5 h-3.5 w-3.5" />
                          Evidencia
                        </Button>
                      ) : null}
                    </div>
                    {open ? (
                      <div className="mt-3 space-y-3 border-t border-white/10 pt-3 text-sm">
                        <div>
                          <p className="mb-2 font-medium text-lq-on-graphite-muted">
                            Documentos y productos
                          </p>
                          {(c.documents || []).length === 0 &&
                          (c.products || []).length === 0 ? (
                            <p className="text-lq-on-graphite-muted">
                              {c.visit_id
                                ? "Sin documentos ni líneas de resultado"
                                : "Sin gestiones: los productos se mostrarán al confirmar la visita"}
                            </p>
                          ) : (c.documents || []).length > 0 ? (
                            <ul className="space-y-3">
                              {(c.documents || []).map((d) => {
                                const docProducts =
                                  d.products && d.products.length > 0
                                    ? d.products
                                    : (c.products || []).filter(
                                        (p) =>
                                          d.id != null &&
                                          p.document_result_id != null &&
                                          Number(p.document_result_id) === Number(d.id),
                                      )
                                return (
                                  <li
                                    key={d.id ?? `${d.document_type}-${d.document_number}`}
                                    className="rounded-lg border border-white/10 bg-black/20 px-3 py-2.5"
                                  >
                                    <p className="font-medium text-lq-on-graphite">
                                      {d.document_type || "Doc"} {d.document_number ?? "—"}
                                      <span className="ml-2 text-lq-on-graphite-muted">
                                        · {d.result_status || "—"}
                                      </span>
                                    </p>
                                    {docProducts.length === 0 ? (
                                      <p className="mt-1.5 text-lq-on-graphite-muted">
                                        Sin líneas de producto en este documento
                                      </p>
                                    ) : (
                                      <ul className="mt-2 space-y-2 border-t border-white/10 pt-2">
                                        {docProducts.map((p, idx) => (
                                          <li key={productRowKey(p, idx)}>
                                            <ProductIdentity p={p} />
                                          </li>
                                        ))}
                                      </ul>
                                    )}
                                  </li>
                                )
                              })}
                            </ul>
                          ) : (
                            <ul className="space-y-2">
                              {(c.products || []).map((p, idx) => (
                                <li
                                  key={productRowKey(p, idx)}
                                  className="rounded-lg border border-white/10 bg-black/20 px-3 py-2.5"
                                >
                                  <ProductIdentity p={p} />
                                </li>
                              ))}
                            </ul>
                          )}
                        </div>
                        <p className="text-sm text-lq-on-graphite-muted">
                          Evidencia: {c.evidence_label_es || "—"}
                          {c.payment_declared ? ` · Pago: ${c.payment_declared}` : ""}
                          {c.sync_confirmed ? " · Sync confirmado" : ""}
                          {c.observation ? ` · Obs.: ${c.observation}` : ""}
                        </p>
                      </div>
                    ) : null}
                  </article>
                </Fragment>
              )
            })}
          </div>
        )}
      </div>

      <Dialog open={detailOpen} onOpenChange={setDetailOpen}>
        <DialogContent className="max-h-[90vh] max-w-2xl overflow-y-auto border-white/10 bg-lq-graphite text-lq-on-graphite">
          <DialogHeader>
            <DialogTitle>{selected?.commercial_name || "Evidencia"}</DialogTitle>
            <DialogDescription className="text-lq-on-graphite-muted">
              URL firmada de corta duración. Bucket privado.
            </DialogDescription>
          </DialogHeader>
          {detailLoading ? (
            <div className="flex items-center gap-2 text-sm text-lq-on-graphite-muted">
              <Loader2 className="h-4 w-4 animate-spin" /> Cargando…
            </div>
          ) : null}
          {detailError ? (
            <p className="text-sm text-red-300 whitespace-pre-wrap">{detailError}</p>
          ) : null}
          {detail ? (
            <div className="space-y-3">
              {(detail.incident_evidence || []).length === 0 ? (
                <p className="text-sm text-lq-on-graphite-muted">Sin evidencia fotográfica</p>
              ) : (
                (detail.incident_evidence || []).map((item, idx) =>
                  item.url ? (
                    // eslint-disable-next-line @next/next/no-img-element
                    <img
                      key={item.id ?? idx}
                      src={item.url}
                      alt={item.label_es || `Evidencia ${idx + 1}`}
                      className="max-h-[60vh] w-full rounded-lg object-contain bg-black/40"
                    />
                  ) : (
                    <p key={item.id ?? idx} className="text-sm text-lq-on-graphite-muted">
                      Sin evidencia fotográfica
                    </p>
                  ),
                )
              )}
              {detail.conformity?.signature_url ? (
                // eslint-disable-next-line @next/next/no-img-element
                <img
                  src={detail.conformity.signature_url}
                  alt="Firma histórica (no requerida para cierre V1)"
                  className="max-h-[40vh] w-full rounded-lg object-contain bg-black/40"
                />
              ) : null}
            </div>
          ) : null}
        </DialogContent>
      </Dialog>
    </div>
  )
}
