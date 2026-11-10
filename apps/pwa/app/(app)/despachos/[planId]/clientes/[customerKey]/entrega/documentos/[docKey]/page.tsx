"use client"

import Link from "next/link"
import { useRouter } from "next/navigation"
import { useEffect, useMemo, useState } from "react"
import {
  ApiError,
  type DriverDocument,
  getVisitDetail,
  saveVisitDocumentLines,
} from "@/lib/api"
import { DeliveryStepChrome } from "@/components/delivery-step-chrome"
import { buildEntregaHref, docKeyFromDocument, moneyCLP } from "@/lib/delivery-flow"
import { useDeliveryParams } from "@/lib/use-delivery-params"
import { useVisitSession } from "@/lib/use-visit-session"

const REASONS = [
  { value: "physical_shortage", label: "Faltante físico" },
  { value: "damaged", label: "Producto dañado" },
  { value: "customer_rejection", label: "Rechazo del cliente" },
  { value: "prep_error", label: "Error de preparación" },
  { value: "other", label: "Otro" },
]

type LineDraft = {
  key: string
  source_detail_id: number
  line_number?: number | null
  variant_id?: number | null
  sku_code?: string | null
  product_name: string
  planned: number
  delivered: number
  reason: string
  note: string
}

function buildLines(doc: DriverDocument): LineDraft[] {
  return (doc.products || [])
    .filter((p) => p.source_detail_id != null)
    .map((p, idx) => {
      const planned = typeof p.unidades === "number" ? p.unidades : 0
      const detailId = Number(p.source_detail_id)
      return {
        key: `detail-${detailId}-${idx}`,
        source_detail_id: detailId,
        line_number: p.line_number ?? null,
        variant_id: p.variant_id,
        sku_code: p.sku_code != null ? String(p.sku_code) : null,
        product_name: p.name_short || p.description || p.producto || "Producto",
        planned,
        delivered: planned,
        reason: "physical_shortage",
        note: "",
      }
    })
}

export default function EntregaDocumentoDetallePage() {
  const { planId, customerKey, docKey } = useDeliveryParams()
  const router = useRouter()
  const session = useVisitSession(planId, customerKey)
  const listHref = buildEntregaHref(planId, customerKey, "documentos")

  const doc = useMemo(
    () =>
      session.documents.find((d) => docKeyFromDocument(d) === String(docKey || "")) ||
      null,
    [session.documents, docKey],
  )

  const [lines, setLines] = useState<LineDraft[]>([])
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [msg, setMsg] = useState<string | null>(null)

  useEffect(() => {
    if (!doc || !session.activeVisit?.id) return
    let cancelled = false
    async function hydrate() {
      const base = buildLines(doc!)
      try {
        const detail = await getVisitDetail(planId, session.activeVisit!.id)
        const docMeta = (detail.documents || []).find(
          (d) =>
            String(d.related_document_id ?? d.document_number ?? "") ===
            docKeyFromDocument(doc!),
        )
        const docResId = docMeta ? String(docMeta.id) : null
        const saved = (detail.lines || []).filter(
          (ln) => String(ln.document_result_id ?? "") === String(docResId),
        )
        if (!cancelled && saved.length) {
          setLines(
            base.map((draft) => {
              const match = saved.find(
                (l) => Number(l.source_detail_id) === Number(draft.source_detail_id),
              )
              if (!match) return draft
              return {
                ...draft,
                delivered: Number(match.delivered_units ?? draft.delivered),
                reason: String(match.undelivered_reason || draft.reason),
                note: String(match.undelivered_note || ""),
              }
            }),
          )
          return
        }
      } catch {
        /* primera vez */
      }
      if (!cancelled) setLines(base)
    }
    void hydrate()
    return () => {
      cancelled = true
    }
  }, [doc, planId, session.activeVisit, docKey])

  async function save() {
    if (!doc?.related_document_id || !session.activeVisit) return
    for (const ln of lines) {
      if (ln.delivered < 0 || ln.delivered > ln.planned) {
        setErr("Cantidad entregada inválida (0 … documentada)")
        return
      }
      if (ln.delivered < ln.planned && !ln.reason) {
        setErr("Motivo obligatorio para faltantes")
        return
      }
      if (ln.delivered < ln.planned && ln.reason === "other" && !ln.note.trim()) {
        setErr("Observación obligatoria cuando el motivo es Otro")
        return
      }
    }
    setBusy(true)
    setErr(null)
    setMsg(null)
    try {
      const undelTotal = lines.reduce((a, l) => a + Math.max(0, l.planned - l.delivered), 0)
      const delTotal = lines.reduce((a, l) => a + l.delivered, 0)
      const result_status =
        undelTotal <= 0 ? "full" : delTotal <= 0 ? "none" : "partial"
      await saveVisitDocumentLines(planId, session.activeVisit.id, {
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
      setMsg("Documento guardado en servidor")
      session.reload()
      router.push(listHref)
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : "No se pudo guardar")
    } finally {
      setBusy(false)
    }
  }

  if (session.loading) return <div className="empty">Cargando…</div>
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
            disabled={busy}
            onClick={() => void save()}
          >
            {busy ? "Guardando…" : "Guardar documento"}
          </button>
        ) : null
      }
    >
      <div className="card" style={{ padding: 12 }}>
        <strong>
          {doc.document_type || "Documento"} {doc.document_number ?? "—"}
        </strong>
        <div className="muted" style={{ fontSize: "0.85rem", marginTop: 4 }}>
          {moneyCLP(doc.document_total)} · {lines.length || (doc.products || []).length}{" "}
          línea(s)
        </div>
      </div>

      {!registrable ? (
        <div className="banner banner-info" role="status">
          Sin source_detail_id fiable. Solo lectura; no se puede registrar entrega
          verificada.
        </div>
      ) : null}

      <div className="stack-gap">
        {(registrable ? lines : []).map((ln) => {
          const diff = Math.max(0, ln.planned - ln.delivered)
          return (
            <div key={ln.key} className="card" style={{ padding: 12 }}>
              <div className="product-name">{ln.product_name}</div>
              <div className="muted" style={{ fontSize: "0.8rem" }}>
                SKU {ln.sku_code || ln.variant_id || "—"} · detail {ln.source_detail_id}
              </div>
              <div className="meta-row">
                <span className="muted">Documentado</span>
                <span>{ln.planned}</span>
              </div>
              <label className="field">
                Entregado
                <input
                  inputMode="decimal"
                  value={String(ln.delivered)}
                  onChange={(e) => {
                    const v = Number(e.target.value)
                    setLines((all) =>
                      all.map((x) =>
                        x.key === ln.key
                          ? { ...x, delivered: Number.isFinite(v) ? v : 0 }
                          : x,
                      ),
                    )
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
                      value={ln.reason}
                      onChange={(e) =>
                        setLines((all) =>
                          all.map((x) =>
                            x.key === ln.key ? { ...x, reason: e.target.value } : x,
                          ),
                        )
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
                        setLines((all) =>
                          all.map((x) =>
                            x.key === ln.key ? { ...x, note: e.target.value } : x,
                          ),
                        )
                      }
                    />
                  </label>
                </>
              ) : null}
            </div>
          )
        })}

        {!registrable
          ? (doc.products || []).map((p, idx) => (
              <div key={`ro-${idx}`} className="card" style={{ padding: 12 }}>
                <div className="product-name">
                  {p.name_short || p.description || p.producto || "Producto"}
                </div>
                <div className="muted" style={{ fontSize: "0.8rem" }}>
                  {typeof p.unidades === "number" ? `${p.unidades} un.` : "—"} · sin
                  detail_id
                </div>
              </div>
            ))
          : null}
      </div>

      {err ? <div className="banner banner-error">{err}</div> : null}
      {msg ? <div className="banner banner-info">{msg}</div> : null}
    </DeliveryStepChrome>
  )
}
