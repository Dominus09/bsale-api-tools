"use client"

import Link from "next/link"
import { useRouter } from "next/navigation"
import { DeliveryStepChrome } from "@/components/delivery-step-chrome"
import {
  buildEntregaHref,
  docKeyFromDocument,
  moneyCLP,
} from "@/lib/delivery-flow"
import { useDeliveryParams } from "@/lib/use-delivery-params"
import { useVisitSession } from "@/lib/use-visit-session"

function docStatus(
  relatedId: string,
  visitDocs: Array<Record<string, unknown>> | null | undefined,
  registrable: boolean,
): { label: string; ok: boolean } {
  if (!registrable) return { label: "Solo lectura", ok: false }
  const saved = (visitDocs || []).find(
    (d) => String(d.related_document_id ?? d.document_number ?? "") === relatedId,
  )
  if (!saved) return { label: "Pendiente", ok: false }
  const st = String(saved.result_status || "")
  if (st === "full") return { label: "Completo", ok: true }
  if (st === "partial") return { label: "Parcial", ok: true }
  if (st === "none") return { label: "Sin entrega", ok: true }
  return { label: "En registro", ok: true }
}

export default function EntregaDocumentosPage() {
  const { planId, customerKey } = useDeliveryParams()
  const router = useRouter()
  const session = useVisitSession(planId, customerKey)
  const fichaHref = buildEntregaHref(planId, customerKey, "ficha")

  if (session.loading) return <div className="empty">Cargando documentos…</div>
  if (session.error) {
    return (
      <div className="stack-gap">
        <Link href={fichaHref} className="muted">
          ← Ficha
        </Link>
        <div className="banner banner-error">{session.error}</div>
      </div>
    )
  }

  if (!session.activeVisit || session.activeVisit.status !== "in_progress") {
    return (
      <div className="stack-gap">
        <div className="banner banner-info">
          No hay visita en atención. Inicie o retome desde la ficha.
        </div>
        <Link href={fichaHref} className="btn btn-primary">
          Ir a ficha
        </Link>
      </div>
    )
  }

  const savedDocs = session.visitDetail?.documents || []
  const allSaved =
    session.documents.length > 0 &&
    session.documents.every((d) => {
      const key = docKeyFromDocument(d)
      if (!key) return false
      const registrable =
        d.quantities_registrable !== false &&
        (d.products || []).some((p) => p.source_detail_id != null)
      if (!registrable) return true
      return savedDocs.some(
        (s) => String(s.related_document_id ?? s.document_number ?? "") === key,
      )
    })

  return (
    <DeliveryStepChrome
      clientName={session.clientName}
      step={1}
      title="Paso 1 · Documentos"
      backHref={fichaHref}
      footer={
        <div className="btn-row">
          <Link
            href={buildEntregaHref(planId, customerKey, "incidencia")}
            className="btn btn-ghost"
          >
            Incidencia
          </Link>
          <button
            type="button"
            className="btn btn-primary"
            disabled={!allSaved}
            onClick={() =>
              router.push(buildEntregaHref(planId, customerKey, "cobranza"))
            }
          >
            Continuar a cobranza
          </button>
        </div>
      }
    >
      {!allSaved ? (
        <p className="muted" style={{ margin: 0, fontSize: "0.82rem" }}>
          Guarde las cantidades de cada documento registrable para habilitar cobranza.
        </p>
      ) : null}

      <div className="stack-gap">
        {session.documents.map((doc) => {
          const key = docKeyFromDocument(doc) || String(Math.random())
          const lines = (doc.products || []).filter((p) => p.source_detail_id != null)
          const ambiguous =
            doc.line_identity === "ambiguous_display_only" ||
            (doc.products || []).some((p) => p.source_detail_id == null)
          const registrable =
            doc.quantities_registrable !== false && lines.length > 0 && !ambiguous
          const st = docStatus(key, savedDocs, registrable)
          const href = buildEntregaHref(planId, customerKey, "documentos", {
            docKey: key,
          })
          return (
            <div key={key} className="card">
              <div className="client-card-head">
                <h3 style={{ fontSize: "1rem", margin: 0 }}>
                  {doc.document_type || "Documento"} {doc.document_number ?? "—"}
                </h3>
                <span className={`pill${st.ok ? " pill-ok" : ""}`}>{st.label}</span>
              </div>
              <div className="muted" style={{ fontSize: "0.85rem", marginTop: 6 }}>
                {moneyCLP(doc.document_total)} ·{" "}
                {(doc.products || []).length} línea(s)
                {doc.group_assignment_conflict
                  ? ` · ${doc.group_assignment_conflict.message_es}`
                  : ""}
              </div>
              {registrable ? (
                <Link href={href} className="btn btn-secondary" style={{ marginTop: 10 }}>
                  Registrar entrega
                </Link>
              ) : (
                <p className="muted" style={{ fontSize: "0.82rem", marginTop: 8 }} role="status">
                  Sin document_details fiables: solo lectura. No se registra entrega
                  verificada de este documento.
                </p>
              )}
            </div>
          )
        })}
      </div>
    </DeliveryStepChrome>
  )
}
