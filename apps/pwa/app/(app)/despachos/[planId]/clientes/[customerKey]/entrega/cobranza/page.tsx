"use client"

import Link from "next/link"
import { useRouter } from "next/navigation"
import { useEffect, useMemo, useState } from "react"
import { ApiError, declareVisitCollection } from "@/lib/api"
import { DeliveryStepChrome } from "@/components/delivery-step-chrome"
import { buildEntregaHref, docKeyFromDocument, moneyCLP } from "@/lib/delivery-flow"
import { useDeliveryParams } from "@/lib/use-delivery-params"
import { useVisitSession } from "@/lib/use-visit-session"

const METHODS = [
  { value: "cash", label: "Efectivo" },
  { value: "transfer_declared", label: "Transferencia informada" },
  { value: "credit", label: "Crédito autorizado" },
  { value: "partial", label: "Pago mixto / parcial" },
  { value: "pending", label: "Pago pendiente" },
  { value: "not_applicable", label: "No corresponde" },
]

type DocPay = {
  related_document_id: number
  document_result_id: number | null
  label: string
  total: number
  method: string
  declared: string
  note: string
  saved: boolean
}

export default function EntregaCobranzaPage() {
  const { planId, customerKey } = useDeliveryParams()
  const router = useRouter()
  const session = useVisitSession(planId, customerKey)
  const docsHref = buildEntregaHref(planId, customerKey, "documentos")
  const [rows, setRows] = useState<DocPay[]>([])
  const [busyKey, setBusyKey] = useState<string | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [info, setInfo] = useState<string | null>(null)

  const visitDocs = session.visitDetail?.documents || []
  const collections = session.visitDetail?.collections || []

  useEffect(() => {
    if (!session.documents.length) return
    const next: DocPay[] = session.documents
      .filter((d) => d.related_document_id != null)
      .map((d) => {
        const key = docKeyFromDocument(d)
        const savedDoc = visitDocs.find(
          (vd) => String(vd.related_document_id ?? "") === key,
        )
        const docResId = savedDoc?.id != null ? Number(savedDoc.id) : null
        const col = collections.find(
          (c) =>
            docResId != null &&
            Number(c.document_result_id) === docResId,
        ) || collections.find((c) => c.document_result_id == null)
        return {
          related_document_id: Number(d.related_document_id),
          document_result_id: docResId,
          label: `${d.document_type || "Doc"} ${d.document_number ?? "—"}`,
          total: typeof d.document_total === "number" ? d.document_total : 0,
          method: String(col?.method || "cash"),
          declared:
            col?.declared_amount != null ? String(col.declared_amount) : "",
          note: String(col?.note || ""),
          saved: Boolean(col),
        }
      })
    setRows(next)
  }, [session.documents, visitDocs, collections])

  const allSaved = useMemo(
    () => rows.length > 0 && rows.every((r) => r.saved),
    [rows],
  )

  async function saveRow(row: DocPay) {
    if (!session.activeVisit) return
    const amt = row.declared.trim() === "" ? null : Number(row.declared)
    if (amt != null && (!Number.isFinite(amt) || amt < 0)) {
      setErr("Monto inválido")
      return
    }
    if (row.method === "pending" && amt != null && amt > 0) {
      setErr("Pago pendiente no admite monto recibido positivo")
      return
    }
    if (!row.document_result_id) {
      setErr("Guarde primero las cantidades del documento (paso 1)")
      return
    }
    setBusyKey(String(row.related_document_id))
    setErr(null)
    setInfo(null)
    try {
      await declareVisitCollection(planId, session.activeVisit.id, {
        document_result_id: row.document_result_id,
        method: row.method,
        document_amount: row.total,
        declared_amount: row.method === "pending" ? 0 : amt,
        note: row.note.trim() || null,
      })
      setRows((all) =>
        all.map((r) =>
          r.related_document_id === row.related_document_id
            ? { ...r, saved: true }
            : r,
        ),
      )
      setInfo(`Cobranza de ${row.label} guardada (declaración auditada).`)
      session.reload()
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : "Error al guardar cobranza")
    } finally {
      setBusyKey(null)
    }
  }

  if (session.loading) return <div className="empty">Cargando cobranza…</div>
  if (session.error) return <div className="banner banner-error">{session.error}</div>
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
        Declaración del chofer. No es recibo oficial ni verificación Bsale. La
        transferencia informada no se marca como verificada.
      </p>

      <div className="stack-gap">
        {rows.map((row) => {
          const declaredN =
            row.declared.trim() === "" ? null : Number(row.declared)
          const pending =
            Number.isFinite(declaredN as number) && row.method !== "pending"
              ? Math.max(0, row.total - Number(declaredN))
              : row.method === "pending"
                ? row.total
                : null
          return (
            <div key={row.related_document_id} className="card">
              <div className="client-card-head">
                <h3 style={{ margin: 0, fontSize: "1rem" }}>{row.label}</h3>
                <span className={`pill${row.saved ? " pill-ok" : ""}`}>
                  {row.saved ? "Guardado" : "Pendiente"}
                </span>
              </div>
              <div className="meta-row">
                <span className="muted">Total documento</span>
                <span>{moneyCLP(row.total)}</span>
              </div>
              <label className="field">
                Método
                <select
                  value={row.method}
                  onChange={(e) =>
                    setRows((all) =>
                      all.map((r) =>
                        r.related_document_id === row.related_document_id
                          ? {
                              ...r,
                              method: e.target.value,
                              saved: false,
                              declared:
                                e.target.value === "pending" ? "0" : r.declared,
                            }
                          : r,
                      ),
                    )
                  }
                >
                  {METHODS.map((m) => (
                    <option key={m.value} value={m.value}>
                      {m.label}
                    </option>
                  ))}
                </select>
              </label>
              <label className="field">
                Monto recibido declarado
                <input
                  inputMode="decimal"
                  disabled={row.method === "pending" || row.method === "not_applicable"}
                  value={row.method === "pending" ? "0" : row.declared}
                  onChange={(e) =>
                    setRows((all) =>
                      all.map((r) =>
                        r.related_document_id === row.related_document_id
                          ? { ...r, declared: e.target.value, saved: false }
                          : r,
                      ),
                    )
                  }
                />
              </label>
              <div className="meta-row">
                <span className="muted">Saldo pendiente</span>
                <span>{pending == null ? "—" : moneyCLP(pending)}</span>
              </div>
              <label className="field">
                Observación
                <input
                  value={row.note}
                  onChange={(e) =>
                    setRows((all) =>
                      all.map((r) =>
                        r.related_document_id === row.related_document_id
                          ? { ...r, note: e.target.value, saved: false }
                          : r,
                      ),
                    )
                  }
                />
              </label>
              <button
                type="button"
                className="btn btn-secondary"
                disabled={busyKey === String(row.related_document_id)}
                onClick={() => void saveRow(row)}
              >
                {busyKey === String(row.related_document_id)
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
