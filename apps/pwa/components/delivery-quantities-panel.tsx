"use client"

import { useEffect, useMemo, useState } from "react"
import {
  ApiError,
  type DriverDocument,
  getVisitDetail,
  saveVisitDocumentLines,
} from "@/lib/api"
import {
  commercialProductLabel,
  productBarcodeLabel,
  productCategoryLabel,
} from "@/lib/delivery-labels"

type LineDraft = {
  key: string
  source_detail_id: number
  line_number?: number | null
  variant_id?: number | null
  sku_code?: string | null
  category_name?: string | null
  barcode?: string | null
  product_name: string
  planned: number
  delivered: number
  reason: string
  note: string
  boxesLabel?: string | null
}

type Props = {
  planId: number
  visitId: number
  documents: DriverDocument[]
  onSaved?: () => void
}

const REASONS = [
  { value: "physical_shortage", label: "Faltante físico" },
  { value: "damaged", label: "Producto dañado" },
  { value: "customer_rejection", label: "Rechazo del cliente" },
  { value: "prep_error", label: "Error de preparación" },
  { value: "other", label: "Otro" },
]

function buildDrafts(docs: DriverDocument[]): Record<string, LineDraft[]> {
  const out: Record<string, LineDraft[]> = {}
  for (const doc of docs) {
    const id = String(doc.related_document_id || doc.document_number || "")
    out[id] = (doc.products || [])
      .filter((p) => p.source_detail_id != null)
      .map((p, idx) => {
        const planned = typeof p.unidades === "number" ? p.unidades : 0
        const detailId = Number(p.source_detail_id)
        return {
          key: `${id}-detail-${detailId}-${idx}`,
          source_detail_id: detailId,
          line_number: p.line_number ?? null,
          variant_id: p.variant_id,
          sku_code: p.sku_code != null ? String(p.sku_code) : null,
          category_name: productCategoryLabel(p),
          barcode: productBarcodeLabel(p),
          product_name: commercialProductLabel(p),
          planned,
          delivered: planned,
          reason: "physical_shortage",
          note: "",
          boxesLabel:
            p.equivalence_status === "verified" && p.cajas_equivalentes != null
              ? `${p.cajas_equivalentes} cajas (ref.)`
              : p.equivalence_label || null,
        }
      })
  }
  return out
}

export function DeliveryQuantitiesPanel(props: Props) {
  const [drafts, setDrafts] = useState<Record<string, LineDraft[]>>({})
  const [open, setOpen] = useState<Record<string, boolean>>({})
  const [busyDoc, setBusyDoc] = useState<string | null>(null)
  const [msg, setMsg] = useState<string | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [savedDocs, setSavedDocs] = useState<Record<string, boolean>>({})

  useEffect(() => {
    let cancelled = false
    async function hydrate() {
      const base = buildDrafts(props.documents)
      const saved: Record<string, boolean> = {}
      try {
        const detail = await getVisitDetail(props.planId, props.visitId)
        const byDoc = new Map<string, Array<Record<string, unknown>>>()
        for (const ln of detail.lines || []) {
          const docResId = String(ln.document_result_id ?? "")
          // Map document_result → related_document_id
          const docMeta = (detail.documents || []).find(
            (d) => String(d.id) === docResId,
          )
          const rel = String(
            docMeta?.related_document_id ?? docMeta?.document_number ?? "",
          )
          if (!rel) continue
          const arr = byDoc.get(rel) || []
          arr.push(ln)
          byDoc.set(rel, arr)
        }
        for (const [rel, lines] of byDoc) {
          if (!base[rel]) continue
          saved[rel] = true
          base[rel] = base[rel].map((draft) => {
            const match = lines.find(
              (l) => Number(l.source_detail_id) === Number(draft.source_detail_id),
            )
            if (!match) return draft
            return {
              ...draft,
              delivered: Number(match.delivered_units ?? draft.delivered),
              reason: String(match.undelivered_reason || draft.reason),
              note: String(match.undelivered_note || ""),
            }
          })
        }
      } catch {
        /* primera visita o sin red: usar planificado */
      }
      if (cancelled) return
      setDrafts(base)
      setSavedDocs(saved)
      const first = props.documents[0]
      if (first) {
        const id = String(first.related_document_id || first.document_number || "")
        setOpen({ [id]: true })
      }
    }
    void hydrate()
    return () => {
      cancelled = true
    }
  }, [props.documents, props.visitId, props.planId])

  const progress = useMemo(() => {
    const total = props.documents.filter((d) => d.related_document_id).length
    const done = Object.keys(savedDocs).length
    return { total, done }
  }, [props.documents, savedDocs])

  async function saveDoc(doc: DriverDocument) {
    const id = String(doc.related_document_id || doc.document_number || "")
    const lines = drafts[id] || []
    if (!doc.related_document_id) {
      setErr("Documento sin related_document_id; no se puede guardar")
      return
    }
    if (!lines.length) {
      setErr("Sin líneas con source_detail_id; no se puede guardar")
      return
    }
    for (const ln of lines) {
      if (!ln.source_detail_id) {
        setErr("Falta identidad de línea (source_detail_id)")
        return
      }
      if (ln.delivered < 0 || ln.delivered > ln.planned) {
        setErr("Cantidad entregada inválida")
        return
      }
      if (ln.delivered < ln.planned && !ln.reason) {
        setErr("Motivo requerido para faltantes")
        return
      }
      if (ln.delivered < ln.planned && ln.reason === "other" && !ln.note.trim()) {
        setErr("Observación obligatoria cuando el motivo es Otro")
        return
      }
    }
    setBusyDoc(id)
    setErr(null)
    setMsg(null)
    try {
      const undelTotal = lines.reduce((a, l) => a + Math.max(0, l.planned - l.delivered), 0)
      const delTotal = lines.reduce((a, l) => a + l.delivered, 0)
      const result_status =
        undelTotal <= 0 ? "full" : delTotal <= 0 ? "none" : "partial"
      await saveVisitDocumentLines(props.planId, props.visitId, {
        related_document_id: Number(doc.related_document_id),
        oc_document_id: doc.oc_document_id ?? null,
        document_number: doc.document_number ?? null,
        document_type: doc.document_type ?? null,
        document_total: doc.document_total ?? null,
        result_status,
        lines: lines.map((l) => ({
          source_detail_id: l.source_detail_id,
          line_number: l.line_number ?? null,
          variant_id: l.variant_id ?? null,
          sku_code: l.sku_code,
          product_name: l.product_name,
          planned_units: l.planned,
          delivered_units: l.delivered,
          undelivered_reason: l.delivered < l.planned ? l.reason : null,
          undelivered_note:
            l.delivered < l.planned && l.note.trim() ? l.note.trim() : null,
        })),
      })
      setSavedDocs((s) => ({ ...s, [id]: true }))
      setMsg(`Documento ${doc.document_number ?? id} guardado`)
      props.onSaved?.()
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : "No se pudo guardar")
    } finally {
      setBusyDoc(null)
    }
  }

  if (!props.documents.length) {
    return <p className="muted">Sin documentos en la ficha.</p>
  }

  return (
    <div className="stack-gap">
      <div className="ficha-summary">
        <span className={`pill${progress.done >= progress.total && progress.total > 0 ? " pill-ok" : ""}`}>
          Cantidades {progress.done}/{progress.total} doc.
        </span>
      </div>

      {props.documents.map((doc) => {
        const id = String(doc.related_document_id || doc.document_number || Math.random())
        const isOpen = Boolean(open[id])
        const lines = drafts[id] || []
        const registrable = doc.quantities_registrable !== false && lines.length > 0
        const ambiguous =
          doc.line_identity === "ambiguous_display_only" ||
          (doc.products || []).some((p) => p.source_detail_id == null)
        return (
          <div key={id} className="card doc-card">
            <button
              type="button"
              className="doc-toggle"
              aria-expanded={isOpen}
              onClick={() => setOpen((s) => ({ ...s, [id]: !s[id] }))}
            >
              <div className="client-card-head">
                <h3 style={{ fontSize: "1rem" }}>
                  {doc.document_type || "Documento"} {doc.document_number ?? "—"}
                  {savedDocs[id] ? " · ✓" : ""}
                </h3>
              </div>
              <div className="doc-summary muted">
                {registrable
                  ? `${lines.length} líneas · `
                  : ambiguous
                    ? "Solo lectura (sin detail_id) · "
                    : "Sin líneas · "}
                {isOpen ? "Ocultar ▲" : registrable ? "Registrar cantidades ▼" : "Ver detalle ▼"}
              </div>
            </button>
            {ambiguous && !registrable ? (
              <p className="muted" style={{ fontSize: "0.82rem", marginTop: 6 }} role="status">
                Productos visibles sin identidad de línea confiable. No se registran
                cantidades ni se puede verificar la entrega sobre líneas ambiguas.
              </p>
            ) : null}

            {isOpen ? (
              <div className="stack-gap" style={{ marginTop: 10 }}>
                {!registrable
                  ? (doc.products || []).map((p, idx) => (
                      <div
                        key={`${id}-ro-${idx}`}
                        style={{ borderTop: "1px solid var(--border)", paddingTop: 8 }}
                      >
                        <div className="product-identity">
                          {productCategoryLabel(p) ? (
                            <div className="product-category">
                              {productCategoryLabel(p)}
                            </div>
                          ) : null}
                          <div className="product-name">
                            {commercialProductLabel(p)}
                          </div>
                        </div>
                        <div className="muted" style={{ fontSize: "0.8rem" }}>
                          {productBarcodeLabel(p)
                            ? `Código de barras: ${productBarcodeLabel(p)} · `
                            : ""}
                          {typeof p.unidades === "number" ? `${p.unidades} un.` : "—"}
                          {" · sin detail_id (no editable)"}
                        </div>
                      </div>
                    ))
                  : lines.map((ln) => {
                      const pending = Math.max(0, ln.planned - ln.delivered)
                      return (
                        <div
                          key={ln.key}
                          style={{ borderTop: "1px solid var(--border)", paddingTop: 8 }}
                        >
                          <div className="product-identity">
                            {ln.category_name ? (
                              <div className="product-category">{ln.category_name}</div>
                            ) : null}
                            <div className="product-name">{ln.product_name}</div>
                          </div>
                          <div className="muted" style={{ fontSize: "0.8rem" }}>
                            {ln.barcode ? `Código de barras: ${ln.barcode} · ` : ""}
                            Planificado: {ln.planned} un.
                            {ln.boxesLabel ? ` · ${ln.boxesLabel}` : ""}
                          </div>
                          <label className="field">
                            Entregado
                            <input
                              inputMode="decimal"
                              value={String(ln.delivered)}
                              onChange={(e) => {
                                const v = Number(e.target.value)
                                setDrafts((all) => ({
                                  ...all,
                                  [id]: (all[id] || []).map((x) =>
                                    x.key === ln.key
                                      ? {
                                          ...x,
                                          delivered: Number.isFinite(v) ? v : 0,
                                        }
                                      : x,
                                  ),
                                }))
                                setSavedDocs((s) => {
                                  const n = { ...s }
                                  delete n[id]
                                  return n
                                })
                              }}
                            />
                          </label>
                          {pending > 0 ? (
                            <>
                              <div className="muted" style={{ fontSize: "0.8rem" }}>
                                Pendiente: {pending}
                              </div>
                              <label className="field">
                                Motivo
                                <select
                                  value={ln.reason}
                                  onChange={(e) =>
                                    setDrafts((all) => ({
                                      ...all,
                                      [id]: (all[id] || []).map((x) =>
                                        x.key === ln.key
                                          ? { ...x, reason: e.target.value }
                                          : x,
                                      ),
                                    }))
                                  }
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
                                  value={ln.note}
                                  onChange={(e) =>
                                    setDrafts((all) => ({
                                      ...all,
                                      [id]: (all[id] || []).map((x) =>
                                        x.key === ln.key
                                          ? { ...x, note: e.target.value }
                                          : x,
                                      ),
                                    }))
                                  }
                                />
                              </label>
                            </>
                          ) : null}
                        </div>
                      )
                    })}
                {registrable ? (
                  <button
                    type="button"
                    className="btn btn-secondary"
                    disabled={busyDoc === id || !doc.related_document_id}
                    onClick={() => void saveDoc(doc)}
                  >
                    {busyDoc === id ? "Guardando…" : "Guardar cantidades del documento"}
                  </button>
                ) : null}
              </div>
            ) : null}
          </div>
        )
      })}

      {err ? <div className="banner banner-error">{err}</div> : null}
      {msg ? <div className="banner banner-info">{msg}</div> : null}
    </div>
  )
}
