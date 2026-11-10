"use client"

import Link from "next/link"
import { useRouter } from "next/navigation"
import { useEffect, useMemo, useState } from "react"
import { ApiError, declareVisitPaymentMethod } from "@/lib/api"
import { DeliveryStepChrome } from "@/components/delivery-step-chrome"
import {
  PAYMENT_METHODS,
  paymentMethodLabel,
  validatePaymentMethod,
  type PaymentMethod,
} from "@/lib/delivery-collection"
import { buildEntregaHref, docKeyFromDocument, moneyCLP } from "@/lib/delivery-flow"
import { useDeliveryParams } from "@/lib/use-delivery-params"
import { useVisitSession } from "@/lib/use-visit-session"

type DocPayState = {
  related_document_id: number
  document_result_id: number | null
  label: string
  total: number | null
  method: PaymentMethod | ""
  saved: boolean
  saving: boolean
}

export default function EntregaCobranzaPage() {
  const { planId, customerKey } = useDeliveryParams()
  const router = useRouter()
  const session = useVisitSession(planId, customerKey)
  const docsHref = buildEntregaHref(planId, customerKey, "documentos")
  const [docs, setDocs] = useState<DocPayState[]>([])
  const [err, setErr] = useState<string | null>(null)
  const [info, setInfo] = useState<string | null>(null)

  const visitDocs = session.visitDetail?.documents || []
  const collections = session.visitDetail?.collections || []

  useEffect(() => {
    if (!session.documents.length) return
    setDocs((prev) => {
      const prevByRel = new Map(prev.map((d) => [d.related_document_id, d]))
      return session.documents
        .filter((d) => d.related_document_id != null)
        .map((d) => {
          const key = docKeyFromDocument(d)
          const relatedId = Number(d.related_document_id)
          const existing = prevByRel.get(relatedId)
          const savedDoc = visitDocs.find(
            (vd) => String(vd.related_document_id ?? "") === key,
          )
          const docResId = savedDoc?.id != null ? Number(savedDoc.id) : null
          const cols = collections.filter(
            (c) => docResId != null && Number(c.document_result_id) === docResId,
          )
          const serverMethod = cols[0]?.method
            ? (String(cols[0].method) as PaymentMethod)
            : ""
          const serverSaved = cols.length > 0
          if (existing && existing.saving) return existing
          if (existing && !existing.saved && !serverSaved) {
            return {
              ...existing,
              document_result_id: docResId ?? existing.document_result_id,
              label: `${d.document_type || "Doc"} ${d.document_number ?? "—"}`,
              total: typeof d.document_total === "number" ? d.document_total : null,
            }
          }
          return {
            related_document_id: relatedId,
            document_result_id: docResId,
            label: `${d.document_type || "Doc"} ${d.document_number ?? "—"}`,
            total: typeof d.document_total === "number" ? d.document_total : null,
            method: serverSaved
              ? serverMethod
              : existing?.method || "",
            saved: serverSaved || Boolean(existing?.saved),
            saving: false,
          }
        })
    })
  }, [session.documents, visitDocs, collections])

  const allSaved = useMemo(
    () => docs.length > 0 && docs.every((d) => d.saved && d.method),
    [docs],
  )

  async function saveDoc(doc: DocPayState, method: PaymentMethod) {
    if (!session.activeVisit) return
    if (!doc.document_result_id) {
      setErr("Registre primero las cantidades del documento")
      return
    }
    const invalid = validatePaymentMethod(method)
    if (invalid) {
      setErr(invalid)
      return
    }
    setErr(null)
    setInfo(null)
    setDocs((all) =>
      all.map((d) =>
        d.related_document_id === doc.related_document_id
          ? { ...d, method, saving: true }
          : d,
      ),
    )
    try {
      await declareVisitPaymentMethod(planId, session.activeVisit.id, {
        document_result_id: doc.document_result_id,
        method,
        document_amount: doc.total,
        note: null,
      })
      setDocs((all) =>
        all.map((d) =>
          d.related_document_id === doc.related_document_id
            ? { ...d, method, saved: true, saving: false }
            : d,
        ),
      )
      setInfo(`${doc.label}: ${paymentMethodLabel(method)}`)
      void session.softReloadVisitDetail()
    } catch (e) {
      setDocs((all) =>
        all.map((d) =>
          d.related_document_id === doc.related_document_id
            ? { ...d, saving: false, saved: false }
            : d,
        ),
      )
      setErr(e instanceof ApiError ? e.message : "Error al guardar forma de pago")
    }
  }

  if (session.loading) return <div className="empty">Cargando…</div>
  if (session.error) return <div className="banner banner-error">{session.error}</div>
  if (!session.canOperateActive) {
    return (
      <div className="stack-gap">
        <div className="banner banner-info">
          {session.adminReviewPending
            ? "Existe una visita anterior pendiente de revisión administrativa."
            : "Visita no activa."}
        </div>
        <Link href={buildEntregaHref(planId, customerKey, "ficha")} className="btn btn-primary">
          Ir a ficha
        </Link>
      </div>
    )
  }

  return (
    <DeliveryStepChrome
      clientName={session.clientName}
      step={2}
      title="Paso 2 · Forma de pago"
      backHref={docsHref}
      footer={
        <button
          type="button"
          className="btn btn-primary"
          disabled={!allSaved}
          onClick={() =>
            router.push(buildEntregaHref(planId, customerKey, "firma"))
          }
        >
          Continuar a conformidad
        </button>
      }
    >
      <p className="muted" style={{ margin: 0, fontSize: "0.8rem" }}>
        ¿Cómo pagó el cliente? Sin montos. No es recibo ni verificación bancaria.
      </p>

      <div className="stack-gap">
        {docs.map((doc) => (
          <div key={doc.related_document_id} className="card">
            <div className="client-card-head">
              <h3 style={{ margin: 0, fontSize: "1rem" }}>{doc.label}</h3>
              <span className={`pill${doc.saved ? " pill-ok" : ""}`}>
                {doc.saving ? "Guardando…" : doc.saved ? "Guardado" : "Pendiente"}
              </span>
            </div>
            {doc.total != null ? (
              <div className="meta-row">
                <span className="muted">Total documento (referencia)</span>
                <span>{moneyCLP(doc.total)}</span>
              </div>
            ) : null}

            <fieldset
              style={{
                border: "none",
                margin: "10px 0 0",
                padding: 0,
                display: "grid",
                gap: 8,
              }}
            >
              <legend className="muted" style={{ fontSize: "0.78rem", marginBottom: 4 }}>
                Forma de pago
              </legend>
              {PAYMENT_METHODS.map((m) => {
                const selected = doc.method === m.value
                return (
                  <button
                    key={m.value}
                    type="button"
                    className={`btn${selected ? " btn-primary" : " btn-secondary"}`}
                    style={{ textAlign: "left", justifyContent: "flex-start" }}
                    disabled={doc.saving || !doc.document_result_id}
                    onClick={() => void saveDoc(doc, m.value)}
                  >
                    {m.label}
                    {selected && doc.saved ? " · ✓" : ""}
                  </button>
                )
              })}
            </fieldset>
            {!doc.document_result_id ? (
              <p className="muted" style={{ margin: "8px 0 0", fontSize: "0.78rem" }}>
                Guarde cantidades del documento antes de declarar el pago.
              </p>
            ) : null}
          </div>
        ))}
      </div>

      {err ? <div className="banner banner-error">{err}</div> : null}
      {info ? <div className="banner banner-info">{info}</div> : null}
    </DeliveryStepChrome>
  )
}
