"use client"

import Link from "next/link"
import { useRouter } from "next/navigation"
import { useEffect, useMemo, useRef, useState } from "react"
import {
  ApiError,
  type DriverDocument,
  getVisitDetail,
  saveVisitDocumentLines,
} from "@/lib/api"
import { DeliveryStepChrome } from "@/components/delivery-step-chrome"
import { buildEntregaHref, docKeyFromDocument, moneyCLP } from "@/lib/delivery-flow"
import {
  commercialProductLabel,
  lineOutcomeEs,
  productBarcodeLabel,
  productCategoryLabel,
} from "@/lib/delivery-labels"
import {
  filterLines,
  lineOutcome,
  lineProgress,
  reasonLabel,
  type LineFilter,
  type LineUiRow,
} from "@/lib/delivery-lines-ui"
import { useDeliveryParams } from "@/lib/use-delivery-params"
import { useVisitSession } from "@/lib/use-visit-session"

const REASONS = [
  { value: "physical_shortage", label: "Faltante físico" },
  { value: "damaged", label: "Producto dañado" },
  { value: "customer_rejection", label: "Rechazo del cliente" },
  { value: "prep_error", label: "Error de preparación" },
  { value: "other", label: "Otro" },
]

type LineDraft = LineUiRow & {
  key: string
  line_number?: number | null
  variant_id?: number | null
}

function buildLines(doc: DriverDocument, savedByDetail: Map<number, Record<string, unknown>>): LineDraft[] {
  return (doc.products || [])
    .filter((p) => p.source_detail_id != null)
    .map((p, idx) => {
      const planned = typeof p.unidades === "number" ? p.unidades : 0
      const detailId = Number(p.source_detail_id)
      const saved = savedByDetail.get(detailId)
      const persisted = Boolean(saved)
      const delivered = persisted
        ? Number(saved?.delivered_units ?? planned)
        : planned
      return {
        key: `detail-${detailId}-${idx}`,
        source_detail_id: detailId,
        line_number: p.line_number ?? null,
        variant_id: p.variant_id,
        sku_code: p.sku_code != null ? String(p.sku_code) : null,
        barcode: productBarcodeLabel(p),
        category_name: productCategoryLabel(p),
        product_name: commercialProductLabel(p),
        planned,
        delivered,
        reason: persisted
          ? String(saved?.undelivered_reason || "physical_shortage")
          : "physical_shortage",
        note: persisted ? String(saved?.undelivered_note || "") : "",
        persisted,
      }
    })
}

function validateLine(ln: LineDraft): string | null {
  if (ln.delivered < 0 || ln.delivered > ln.planned) {
    return "Cantidad entregada inválida (0 … documentada)"
  }
  if (ln.delivered < ln.planned && !ln.reason) {
    return "Motivo obligatorio para faltantes"
  }
  if (ln.delivered < ln.planned && ln.reason === "other" && !ln.note.trim()) {
    return "Observación obligatoria cuando el motivo es Otro"
  }
  return null
}

export default function EntregaDocumentoDetallePage() {
  const { planId, customerKey, docKey } = useDeliveryParams()
  const router = useRouter()
  const session = useVisitSession(planId, customerKey)
  const listHref = buildEntregaHref(planId, customerKey, "documentos")
  const listRef = useRef<HTMLDivElement | null>(null)

  const doc = useMemo(
    () =>
      session.documents.find((d) => docKeyFromDocument(d) === String(docKey || "")) ||
      null,
    [session.documents, docKey],
  )

  const [lines, setLines] = useState<LineDraft[]>([])
  const [filter, setFilter] = useState<LineFilter>("pending")
  const [query, setQuery] = useState("")
  const [busyId, setBusyId] = useState<number | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [msg, setMsg] = useState<string | null>(null)
  const [hydrated, setHydrated] = useState(false)
  const [visitVersion, setVisitVersion] = useState<number>(
    session.activeVisit?.version || 1,
  )
  const linesRef = useRef(lines)
  linesRef.current = lines

  useEffect(() => {
    if (session.activeVisit?.version != null) {
      setVisitVersion(session.activeVisit.version)
    }
  }, [session.activeVisit?.version])

  async function hydrateFromServer() {
    if (!doc || !session.activeVisit?.id) return
    const baseSaved = new Map<number, Record<string, unknown>>()
    try {
      const detail = await getVisitDetail(planId, session.activeVisit.id)
      const docMeta = (detail.documents || []).find(
        (d) =>
          String(d.related_document_id ?? d.document_number ?? "") ===
          docKeyFromDocument(doc),
      )
      const docResId = docMeta ? String(docMeta.id) : null
      for (const ln of detail.lines || []) {
        if (docResId && String(ln.document_result_id ?? "") !== docResId) continue
        const sid = Number(ln.source_detail_id)
        if (Number.isFinite(sid)) baseSaved.set(sid, ln)
      }
    } catch {
      /* primera visita */
    }
    setLines(buildLines(doc, baseSaved))
    setHydrated(true)
  }

  useEffect(() => {
    if (!doc || !session.activeVisit?.id) return
    let cancelled = false
    void (async () => {
      await hydrateFromServer()
      if (cancelled) return
    })()
    return () => {
      cancelled = true
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [doc, planId, session.activeVisit?.id, docKey])

  const progress = useMemo(() => lineProgress(lines), [lines])
  const visible = useMemo(
    () => filterLines(lines, filter, query),
    [lines, filter, query],
  )

  async function persistLine(ln: LineDraft, opts?: { stayOnRegistered?: boolean }) {
    if (!doc?.related_document_id || !session.activeVisit) return
    const validation = validateLine(ln)
    if (validation) {
      setErr(validation)
      return
    }
    const scrollY = typeof window !== "undefined" ? window.scrollY : 0
    setBusyId(ln.source_detail_id)
    setErr(null)
    setMsg(null)
    try {
      const res = (await saveVisitDocumentLines(planId, session.activeVisit.id, {
        related_document_id: Number(doc.related_document_id),
        oc_document_id: doc.oc_document_id ?? null,
        document_number: doc.document_number ?? null,
        document_type: doc.document_type ?? null,
        document_total: doc.document_total ?? null,
        result_status: "pending",
        expected_version: visitVersion,
        lines: [
          {
            source_detail_id: ln.source_detail_id,
            line_number: ln.line_number ?? null,
            variant_id: ln.variant_id ?? null,
            sku_code: ln.sku_code,
            product_name: ln.product_name,
            planned_units: ln.planned,
            delivered_units: ln.delivered,
            undelivered_reason: ln.delivered < ln.planned ? ln.reason : null,
            undelivered_note:
              ln.delivered < ln.planned && ln.note.trim() ? ln.note.trim() : null,
          },
        ],
      })) as {
        lines?: Array<Record<string, unknown>>
        document_complete?: boolean
        persisted?: boolean
        visit_version?: number
      }
      if (!res.persisted) {
        setErr("El servidor no confirmó el guardado de la línea")
        return
      }
      if (typeof res.visit_version === "number") {
        setVisitVersion(res.visit_version)
      }
      const savedByDetail = new Map<number, Record<string, unknown>>()
      for (const s of res.lines || []) {
        const sid = Number(s.source_detail_id)
        if (Number.isFinite(sid)) savedByDetail.set(sid, s)
      }
      // Conservar drafts locales de líneas aún no persistidas.
      setLines((prev) => {
        const next = prev.map((row) => {
          const saved = savedByDetail.get(row.source_detail_id)
          if (!saved) return row
          return {
            ...row,
            delivered: Number(saved.delivered_units ?? row.delivered),
            reason: String(saved.undelivered_reason || row.reason || "physical_shortage"),
            note: String(saved.undelivered_note || ""),
            persisted: true,
          }
        })
        return next
      })
      setMsg(
        res.document_complete
          ? "Documento completo en servidor"
          : `Línea ${ln.source_detail_id} registrada`,
      )
      if (!opts?.stayOnRegistered && filter === "pending") {
        /* la línea sale sola del filtro; no forzar scroll al top */
      }
      session.reload()
      requestAnimationFrame(() => {
        if (typeof window !== "undefined") window.scrollTo({ top: scrollY })
      })
    } catch (e) {
      if (e instanceof ApiError && e.status === 409) {
        setErr(
          e.message ||
            "Conflicto de versión: recargue la visita e intente de nuevo.",
        )
        session.reload()
      } else {
        setErr(e instanceof ApiError ? e.message : "No se pudo guardar la línea")
      }
    } finally {
      setBusyId(null)
    }
  }

  if (session.loading || !hydrated) return <div className="empty">Cargando…</div>
  if (session.error) {
    return <div className="banner banner-error">{session.error}</div>
  }
  if (!session.activeVisit || session.activeVisit.status !== "in_progress") {
    return (
      <div className="stack-gap">
        <div className="banner banner-info">Visita no activa.</div>
        <Link href={buildEntregaHref(planId, customerKey, "ficha")} className="btn btn-primary">
          Ir a ficha
        </Link>
      </div>
    )
  }
  if (!doc) {
    return (
      <div className="stack-gap">
        <div className="banner banner-error">Documento no encontrado en la ficha.</div>
        <Link href={listHref} className="btn btn-ghost">
          Volver a documentos
        </Link>
      </div>
    )
  }

  const registrable = lines.length > 0
  const allDone = progress.pending === 0 && progress.total > 0

  return (
    <DeliveryStepChrome
      clientName={session.clientName}
      step={1}
      title={`${doc.document_type || "Doc"} ${doc.document_number ?? ""}`}
      backHref={listHref}
      footer={
        registrable ? (
          <button
            type="button"
            className="btn btn-primary"
            disabled={!allDone || busyId != null}
            onClick={() => {
              if (!allDone) {
                setErr(`Faltan ${progress.pending} línea(s) por registrar`)
                setFilter("pending")
                return
              }
              router.push(listHref)
            }}
          >
            {allDone ? "Volver a documentos" : `Faltan ${progress.pending} líneas`}
          </button>
        ) : null
      }
    >
      <div className="card" style={{ padding: 12 }}>
        <strong>
          {doc.document_type || "Documento"} {doc.document_number ?? "—"}
        </strong>
        <div className="muted" style={{ fontSize: "0.85rem", marginTop: 4 }}>
          {moneyCLP(doc.document_total)} · {progress.total} línea(s)
        </div>
        <div className="line-progress" role="status">
          Registrados {progress.registered} de {progress.total} · Pendientes{" "}
          {progress.pending}
        </div>
      </div>

      {!registrable ? (
        <div className="banner banner-info" role="status">
          Sin source_detail_id fiable. Solo lectura; no se puede registrar entrega
          verificada.
        </div>
      ) : (
        <>
          <div className="line-toolbar card">
            <label className="field" style={{ marginBottom: 8 }}>
              Buscar producto o código de barras
              <input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Nombre o código de barras"
                autoComplete="off"
                enterKeyHint="search"
              />
            </label>
            <div className="line-filters" role="tablist" aria-label="Filtro de líneas">
              {(
                [
                  ["pending", `Pendientes ${progress.pending}`],
                  ["registered", `Registrados ${progress.registered}`],
                  ["all", `Todos ${progress.total}`],
                ] as const
              ).map(([key, label]) => (
                <button
                  key={key}
                  type="button"
                  role="tab"
                  aria-selected={filter === key}
                  className={`pill${filter === key ? " pill-active" : ""}`}
                  onClick={() => setFilter(key)}
                >
                  {label}
                </button>
              ))}
            </div>
            <p className="muted" style={{ margin: "8px 0 0", fontSize: "0.75rem" }}>
              Una línea solo cuenta como registrada cuando el servidor confirma el
              guardado. La cantidad precargada no implica entrega.
            </p>
            {progress.pending > 0 ? (
              <button
                type="button"
                className="btn btn-ghost"
                style={{ marginTop: 8 }}
                onClick={() => {
                  setFilter("pending")
                  setQuery("")
                  requestAnimationFrame(() => {
                    listRef.current
                      ?.querySelector(".line-card")
                      ?.scrollIntoView({ behavior: "smooth", block: "start" })
                  })
                }}
              >
                Siguiente pendiente
              </button>
            ) : null}
          </div>

          <div className="stack-gap" ref={listRef}>
            {visible.length === 0 ? (
              <div className="empty" style={{ padding: 24 }}>
                {filter === "pending"
                  ? progress.total === 0
                    ? "Sin líneas"
                    : "No hay pendientes con este filtro/búsqueda"
                  : "Sin resultados"}
              </div>
            ) : (
              visible.map((ln) => {
                const outcome = lineOutcome(ln)
                const diff = Math.max(0, ln.planned - ln.delivered)
                const busy = busyId === ln.source_detail_id
                function patchLine(patch: Partial<LineDraft>) {
                  setLines((all) =>
                    all.map((x) => (x.key === ln.key ? { ...x, ...patch } : x)),
                  )
                }
                return (
                  <div
                    key={ln.key}
                    className={`card line-card${ln.persisted ? " line-card-saved" : ""}`}
                    style={{ padding: 12 }}
                  >
                    <div className="client-card-head">
                      <div className="product-identity">
                        {ln.category_name ? (
                          <div className="product-category">{ln.category_name}</div>
                        ) : null}
                        <div className="product-name">{ln.product_name}</div>
                      </div>
                      <span
                        className={`pill${
                          outcome === "full"
                            ? " pill-ok"
                            : outcome === "partial" || outcome === "none"
                              ? " pill-warn"
                              : ""
                        }`}
                      >
                        {lineOutcomeEs(outcome)}
                      </span>
                    </div>
                    <div className="muted" style={{ fontSize: "0.8rem" }}>
                      {ln.barcode
                        ? `Código de barras: ${ln.barcode}`
                        : "Código de barras: no disponible"}
                    </div>
                    <div className="meta-row">
                      <span className="muted">Documentado</span>
                      <span>{ln.planned}</span>
                    </div>
                    <div className="btn-row" style={{ marginBottom: 8 }}>
                      <button
                        type="button"
                        className="btn btn-ghost btn-compact"
                        disabled={busy}
                        onClick={() =>
                          patchLine({ delivered: ln.planned, reason: "physical_shortage", note: "" })
                        }
                      >
                        Entregar todo
                      </button>
                      <button
                        type="button"
                        className="btn btn-ghost btn-compact"
                        disabled={busy}
                        onClick={() =>
                          patchLine({
                            delivered: Math.max(0, Math.min(ln.planned, Math.floor(ln.planned / 2))),
                            reason: ln.reason || "physical_shortage",
                          })
                        }
                      >
                        Parcial
                      </button>
                      <button
                        type="button"
                        className="btn btn-ghost btn-compact"
                        disabled={busy}
                        onClick={() =>
                          patchLine({
                            delivered: 0,
                            reason: ln.reason || "physical_shortage",
                          })
                        }
                      >
                        No entregado
                      </button>
                    </div>
                    <label className="field">
                      Entregado
                      <input
                        inputMode="decimal"
                        value={String(ln.delivered)}
                        disabled={busy}
                        onChange={(e) => {
                          const v = Number(e.target.value)
                          patchLine({ delivered: Number.isFinite(v) ? v : 0 })
                        }}
                      />
                    </label>
                    <div className="meta-row">
                      <span className="muted">Diferencia</span>
                      <span>{diff}</span>
                    </div>
                    {diff > 0 ? (
                      <>
                        <label className="field">
                          Motivo del faltante
                          <select
                            value={ln.reason || "physical_shortage"}
                            disabled={busy}
                            onChange={(e) => patchLine({ reason: e.target.value })}
                          >
                            {REASONS.map((r) => (
                              <option key={r.value} value={r.value}>
                                {r.label}
                              </option>
                            ))}
                          </select>
                        </label>
                        <label className="field">
                          Observación{ln.reason === "other" ? " *" : ""}
                          <input
                            value={ln.note || ""}
                            disabled={busy}
                            onChange={(e) => patchLine({ note: e.target.value })}
                          />
                        </label>
                        {ln.persisted ? (
                          <div className="muted" style={{ fontSize: "0.78rem" }}>
                            Motivo guardado: {reasonLabel(ln.reason)}
                          </div>
                        ) : null}
                      </>
                    ) : null}
                    <button
                      type="button"
                      className="btn btn-secondary"
                      style={{ marginTop: 8 }}
                      disabled={busy}
                      onClick={() => {
                        const current =
                          linesRef.current.find((x) => x.key === ln.key) || ln
                        void persistLine(current, {
                          stayOnRegistered: current.persisted,
                        })
                      }}
                    >
                      {busy
                        ? "Guardando…"
                        : ln.persisted
                          ? "Guardar cambios"
                          : "Registrar línea"}
                    </button>
                  </div>
                )
              })
            )}
          </div>
        </>
      )}

      {!registrable
        ? (doc.products || []).map((p, idx) => (
            <div key={`ro-${idx}`} className="card" style={{ padding: 12 }}>
              <div className="product-identity">
                {productCategoryLabel(p) ? (
                  <div className="product-category">{productCategoryLabel(p)}</div>
                ) : null}
                <div className="product-name">{commercialProductLabel(p)}</div>
              </div>
              <div className="muted" style={{ fontSize: "0.8rem" }}>
                {productBarcodeLabel(p)
                  ? `Código de barras: ${productBarcodeLabel(p)}`
                  : "Código de barras: no disponible"}
                {typeof p.unidades === "number" ? ` · ${p.unidades} un.` : ""}
              </div>
            </div>
          ))
        : null}

      {err ? <div className="banner banner-error">{err}</div> : null}
      {msg ? <div className="banner banner-info">{msg}</div> : null}
    </DeliveryStepChrome>
  )
}
