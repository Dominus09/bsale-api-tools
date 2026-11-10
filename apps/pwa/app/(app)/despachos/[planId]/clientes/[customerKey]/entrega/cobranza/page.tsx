"use client"

import Link from "next/link"
import { useRouter } from "next/navigation"
import { useEffect, useMemo, useState } from "react"
import { ApiError, declareVisitCollectionMixed } from "@/lib/api"
import { DeliveryStepChrome } from "@/components/delivery-step-chrome"
import {
  newPaymentRow,
  PAYMENT_METHODS,
  parseAmount,
  pendingBalance,
  sumDeclared,
  validateMixedPayments,
  type PaymentRow,
} from "@/lib/delivery-collection"
import { formatClpDisplay } from "@/lib/clp-input"
import { buildEntregaHref, docKeyFromDocument, moneyCLP } from "@/lib/delivery-flow"
import { useDeliveryParams } from "@/lib/use-delivery-params"
import { useVisitSession } from "@/lib/use-visit-session"

type DocPayState = {
  related_document_id: number
  document_result_id: number | null
  label: string
  total: number
  rows: PaymentRow[]
  saved: boolean
}

export default function EntregaCobranzaPage() {
  const { planId, customerKey } = useDeliveryParams()
  const router = useRouter()
  const session = useVisitSession(planId, customerKey)
  const docsHref = buildEntregaHref(planId, customerKey, "documentos")
  const [docs, setDocs] = useState<DocPayState[]>([])
  const [busyKey, setBusyKey] = useState<string | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [info, setInfo] = useState<string | null>(null)

  const visitDocs = session.visitDetail?.documents || []
  const collections = session.visitDetail?.collections || []

  useEffect(() => {
    if (!session.documents.length) return
    const next: DocPayState[] = session.documents
      .filter((d) => d.related_document_id != null)
      .map((d) => {
        const key = docKeyFromDocument(d)
        const savedDoc = visitDocs.find(
          (vd) => String(vd.related_document_id ?? "") === key,
        )
        const docResId = savedDoc?.id != null ? Number(savedDoc.id) : null
        const cols = collections.filter(
          (c) => docResId != null && Number(c.document_result_id) === docResId,
        )
        const rows: PaymentRow[] =
          cols.length > 0
            ? cols.map((c, i) =>
                newPaymentRow({
                  key: `saved-${c.id ?? i}`,
                  method: (String(c.method || "cash") as PaymentRow["method"]),
                  amount:
                    c.declared_amount != null
                      ? formatClpDisplay(Number(c.declared_amount))
                      : "",
                  note: String(c.note || ""),
                }),
              )
            : [newPaymentRow({ method: "cash" })]
        return {
          related_document_id: Number(d.related_document_id),
          document_result_id: docResId,
          label: `${d.document_type || "Doc"} ${d.document_number ?? "—"}`,
          total: typeof d.document_total === "number" ? d.document_total : 0,
          rows,
          saved: cols.length > 0,
        }
      })
    setDocs(next)
  }, [session.documents, visitDocs, collections])

  const allSaved = useMemo(
    () => docs.length > 0 && docs.every((d) => d.saved),
    [docs],
  )

  async function saveDoc(doc: DocPayState) {
    if (!session.activeVisit) return
    if (!doc.document_result_id) {
      setErr("Guarde primero las cantidades del documento (paso 1)")
      return
    }
    const validation = validateMixedPayments(doc.total, doc.rows)
    if (validation) {
      setErr(validation)
      return
    }
    setBusyKey(String(doc.related_document_id))
    setErr(null)
    setInfo(null)
    try {
      await declareVisitCollectionMixed(planId, session.activeVisit.id, {
        document_result_id: doc.document_result_id,
        document_amount: doc.total,
        payments: doc.rows.map((r) => ({
          method: r.method,
          declared_amount: parseAmount(r.amount) ?? 0,
          note: r.note.trim() || null,
        })),
      })
      setDocs((all) =>
        all.map((d) =>
          d.related_document_id === doc.related_document_id
            ? { ...d, saved: true }
            : d,
        ),
      )
      setInfo(`Cobranza de ${doc.label} guardada (mixto auditado).`)
      session.reload()
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : "Error al guardar cobranza")
    } finally {
      setBusyKey(null)
    }
  }

  if (session.loading) return <div className="empty">Cargando cobranza…</div>
  if (session.error) return <div className="banner banner-error">{session.error}</div>
  if (!session.canOperateActive) {
    return (
      <div className="stack-gap">
        <div className="banner banner-info">
          {session.adminReviewPending
            ? "Existe una visita anterior pendiente de revisión administrativa. Consulte el resultado desde la ficha."
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
      title="Paso 2 · Cobranza"
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
          Continuar a firma
        </button>
      }
    >
      <p className="muted" style={{ margin: 0, fontSize: "0.8rem" }}>
        Declaración del chofer con pagos mixtos. No es recibo ni verificación Bsale.
        La transferencia informada no se marca como verificada.
      </p>

      <div className="stack-gap">
        {docs.map((doc) => {
          const declared = sumDeclared(doc.rows)
          const pending = pendingBalance(doc.total, doc.rows)
          return (
            <div key={doc.related_document_id} className="card">
              <div className="client-card-head">
                <h3 style={{ margin: 0, fontSize: "1rem" }}>{doc.label}</h3>
                <span className={`pill${doc.saved ? " pill-ok" : ""}`}>
                  {doc.saved ? "Guardado" : "Pendiente"}
                </span>
              </div>
              <div className="meta-row">
                <span className="muted">Total documento</span>
                <span>{moneyCLP(doc.total)}</span>
              </div>
              <div className="meta-row">
                <span className="muted">Total declarado</span>
                <span className={declared - doc.total > 0.009 ? "amt-danger" : ""}>
                  {moneyCLP(declared)}
                </span>
              </div>
              <div className="meta-row">
                <span className="muted">Saldo pendiente</span>
                <span className={pending > 0 ? "amt-pending" : ""}>
                  {moneyCLP(pending)}
                </span>
              </div>

              <strong style={{ display: "block", margin: "10px 0 6px", fontSize: "0.9rem" }}>
                Formas de pago declaradas
              </strong>
              <div className="stack-gap">
                {doc.rows.map((row) => (
                  <div
                    key={row.key}
                    className={`card pay-row pay-${row.method}`}
                    style={{ padding: 10 }}
                  >
                    <label className="field">
                      Forma de pago
                      <select
                        value={row.method}
                        onChange={(e) =>
                          setDocs((all) =>
                            all.map((d) =>
                              d.related_document_id === doc.related_document_id
                                ? {
                                    ...d,
                                    saved: false,
                                    rows: d.rows.map((r) =>
                                      r.key === row.key
                                        ? {
                                            ...r,
                                            method: e.target
                                              .value as PaymentRow["method"],
                                          }
                                        : r,
                                    ),
                                  }
                                : d,
                            ),
                          )
                        }
                      >
                        {PAYMENT_METHODS.map((m) => (
                          <option key={m.value} value={m.value}>
                            {m.label}
                          </option>
                        ))}
                      </select>
                    </label>
                    <label className="field">
                      Monto (CLP)
                      <input
                        inputMode="numeric"
                        pattern="[0-9.]*"
                        enterKeyHint="done"
                        value={row.amount}
                        onChange={(e) => {
                          const raw = e.target.value
                          setDocs((all) =>
                            all.map((d) =>
                              d.related_document_id === doc.related_document_id
                                ? {
                                    ...d,
                                    saved: false,
                                    rows: d.rows.map((r) =>
                                      r.key === row.key
                                        ? { ...r, amount: raw }
                                        : r,
                                    ),
                                  }
                                : d,
                            ),
                          )
                        }}
                        onBlur={() => {
                          const n = parseAmount(row.amount)
                          if (n == null) return
                          const pretty = formatClpDisplay(n)
                          setDocs((all) =>
                            all.map((d) =>
                              d.related_document_id === doc.related_document_id
                                ? {
                                    ...d,
                                    rows: d.rows.map((r) =>
                                      r.key === row.key
                                        ? { ...r, amount: pretty }
                                        : r,
                                    ),
                                  }
                                : d,
                            ),
                          )
                        }}
                      />
                    </label>
                    <label className="field">
                      Observación
                      <input
                        value={row.note}
                        onChange={(e) =>
                          setDocs((all) =>
                            all.map((d) =>
                              d.related_document_id === doc.related_document_id
                                ? {
                                    ...d,
                                    saved: false,
                                    rows: d.rows.map((r) =>
                                      r.key === row.key
                                        ? { ...r, note: e.target.value }
                                        : r,
                                    ),
                                  }
                                : d,
                            ),
                          )
                        }
                      />
                    </label>
                    {doc.rows.length > 1 ? (
                      <button
                        type="button"
                        className="btn btn-ghost"
                        onClick={() =>
                          setDocs((all) =>
                            all.map((d) =>
                              d.related_document_id === doc.related_document_id
                                ? {
                                    ...d,
                                    saved: false,
                                    rows: d.rows.filter((r) => r.key !== row.key),
                                  }
                                : d,
                            ),
                          )
                        }
                      >
                        Eliminar fila
                      </button>
                    ) : null}
                  </div>
                ))}
              </div>
              <button
                type="button"
                className="btn btn-secondary"
                style={{ marginTop: 8 }}
                onClick={() =>
                  setDocs((all) =>
                    all.map((d) =>
                      d.related_document_id === doc.related_document_id
                        ? {
                            ...d,
                            saved: false,
                            rows: [...d.rows, newPaymentRow()],
                          }
                        : d,
                    ),
                  )
                }
              >
                + Agregar forma de pago
              </button>
              <button
                type="button"
                className="btn btn-primary"
                style={{ marginTop: 8 }}
                disabled={busyKey === String(doc.related_document_id)}
                onClick={() => void saveDoc(doc)}
              >
                {busyKey === String(doc.related_document_id)
                  ? "Guardando…"
                  : "Guardar declaración"}
              </button>
            </div>
          )
        })}
      </div>

      {err ? <div className="banner banner-error">{err}</div> : null}
      {info ? <div className="banner banner-info">{info}</div> : null}
    </DeliveryStepChrome>
  )
}
