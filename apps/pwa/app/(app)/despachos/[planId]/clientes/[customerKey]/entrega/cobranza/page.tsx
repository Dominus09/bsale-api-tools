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

function PaymentIcon({ method }: { method: PaymentMethod }) {
  const paths: Record<PaymentMethod, React.ReactNode> = {
    cash: <><rect x="3" y="6" width="18" height="12" rx="2"/><circle cx="12" cy="12" r="2.5"/><path d="M6 12h1m10 0h1"/></>,
    transfer_declared: <><path d="M3 8h17m-4-4 4 4-4 4M21 16H4m4-4-4 4 4 4"/></>,
    cash_and_transfer: <><rect x="3" y="4" width="14" height="9" rx="2"/><circle cx="10" cy="8.5" r="1.8"/><path d="M6 18h15m-4-4 4 4-4 4"/></>,
    check_declared: <><rect x="3" y="5" width="18" height="14" rx="2"/><path d="M6 9h7M6 15h3m3 0 2 2 4-4"/></>,
    pending: <><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></>,
    not_applicable: <><circle cx="12" cy="12" r="9"/><path d="m6 6 12 12"/></>,
  }
  return <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">{paths[method]}</svg>
}

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
  const [applyBusy, setApplyBusy] = useState(false)

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
  const pendingCount = docs.filter((d) => !d.saved || !d.method).length
  const firstMethod = docs.find((d) => d.method)?.method || ""

  async function saveDoc(doc: DocPayState, method: PaymentMethod) {
    if (!session.activeVisit) return
    if (!doc.document_result_id) {
      setErr("Confirme primero el resultado del documento")
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

  async function applyToAll() {
    if (!session.activeVisit || !firstMethod) return
    if (
      !window.confirm(
        `¿Aplicar «${paymentMethodLabel(firstMethod)}» a todos los documentos?`,
      )
    ) {
      return
    }
    setApplyBusy(true)
    setErr(null)
    setInfo(null)
    try {
      for (const doc of docs) {
        if (!doc.document_result_id) continue
        if (doc.saved && doc.method === firstMethod) continue
        await declareVisitPaymentMethod(planId, session.activeVisit.id, {
          document_result_id: doc.document_result_id,
          method: firstMethod,
          document_amount: doc.total,
          note: null,
        })
        setDocs((all) =>
          all.map((d) =>
            d.related_document_id === doc.related_document_id
              ? { ...d, method: firstMethod, saved: true, saving: false }
              : d,
          ),
        )
      }
      setInfo(`Forma de pago aplicada a ${docs.length} documento(s)`)
      void session.softReloadVisitDetail()
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : "Error al aplicar a todos")
    } finally {
      setApplyBusy(false)
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
            router.push(buildEntregaHref(planId, customerKey, "cierre"))
          }
        >
          Continuar a conformidad
        </button>
      }
    >
      <div className="driver-screen-heading">
        <h2 className="section-title">¿Cómo pagó el cliente?</h2>
        <p className="muted" style={{ margin: 0, fontSize: "0.82rem" }}>
          Selecciona una forma de pago por documento. No ingreses montos.
          Cheque es solo declaración del chofer, no validación ni crédito.
        </p>
      </div>

      {pendingCount > 0 ? (
        <div className="banner banner-info" style={{ fontSize: "0.82rem" }}>
          {pendingCount} documento{pendingCount === 1 ? "" : "s"} sin forma de pago
        </div>
      ) : null}

      {docs.length > 1 && firstMethod ? (
        <button
          type="button"
          className="btn btn-secondary"
          disabled={applyBusy}
          onClick={() => void applyToAll()}
        >
          {applyBusy ? "Aplicando…" : "Aplicar esta forma de pago a todos"}
        </button>
      ) : null}

      <div className="stack-gap">
        {docs.map((doc) => (
          <div key={doc.related_document_id} className="card" style={{ padding: "12px" }}>
            <div className="client-card-head">
              <h3 style={{ margin: 0, fontSize: "0.95rem" }}>{doc.label}</h3>
              <span className={`pill${doc.saved ? " pill-ok" : ""}`}>
                {doc.saving
                  ? "Guardando…"
                  : doc.saved
                    ? paymentMethodLabel(doc.method)
                    : "Pendiente"}
              </span>
            </div>
            {doc.total != null ? (
              <div className="meta-row">
                <span className="muted">Total (referencia)</span>
                <span>{moneyCLP(doc.total)}</span>
              </div>
            ) : null}

            <div className="pay-method-grid" role="group" aria-label={`Pago ${doc.label}`}>
              {PAYMENT_METHODS.map((m) => {
                const selected = doc.method === m.value
                return (
                  <button
                    key={m.value}
                    type="button"
                    className={`pay-method-chip${selected ? " is-selected" : ""}`}
                    aria-pressed={selected}
                    disabled={doc.saving || !doc.document_result_id || applyBusy}
                    onClick={() => void saveDoc(doc, m.value)}
                  >
                    <PaymentIcon method={m.value} />
                    <span>{m.label}</span>
                    {selected && doc.saved ? <span className="driver-pay-check" aria-label="Guardado">✓</span> : null}
                  </button>
                )
              })}
            </div>
            {!doc.document_result_id ? (
              <p className="muted" style={{ margin: "8px 0 0", fontSize: "0.78rem" }}>
                Confirme el resultado del documento antes de declarar el pago.
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
