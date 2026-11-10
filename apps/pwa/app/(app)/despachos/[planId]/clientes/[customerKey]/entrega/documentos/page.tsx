"use client"

import Link from "next/link"
import { useRouter } from "next/navigation"
import { useMemo, useState } from "react"
import { DeliveryStepChrome } from "@/components/delivery-step-chrome"
import {
  buildEntregaHref,
  docKeyFromDocument,
  moneyCLP,
} from "@/lib/delivery-flow"
import { useDeliveryParams } from "@/lib/use-delivery-params"
import { useVisitSession } from "@/lib/use-visit-session"

function docLineProgress(
  doc: {
    related_document_id?: number | null
    document_number?: number | null
    products?: Array<{ source_detail_id?: number | null }>
    quantities_registrable?: boolean
    line_identity?: string
  },
  visitDocs: Array<Record<string, unknown>>,
  visitLines: Array<Record<string, unknown>>,
): {
  registrable: boolean
  expected: number
  registered: number
  pending: number
  complete: boolean
  label: string
  ok: boolean
} {
  const key = docKeyFromDocument(doc)
  const expectedIds = (doc.products || [])
    .filter((p) => p.source_detail_id != null)
    .map((p) => Number(p.source_detail_id))
  const ambiguous =
    doc.line_identity === "ambiguous_display_only" ||
    (doc.products || []).some((p) => p.source_detail_id == null)
  const registrable =
    doc.quantities_registrable !== false && expectedIds.length > 0 && !ambiguous
  if (!registrable) {
    return {
      registrable: false,
      expected: 0,
      registered: 0,
      pending: 0,
      complete: true,
      label: "Solo lectura",
      ok: false,
    }
  }
  const docMeta = visitDocs.find(
    (d) => String(d.related_document_id ?? d.document_number ?? "") === key,
  )
  const docResId = docMeta?.id != null ? String(docMeta.id) : null
  const registeredIds = new Set(
    (visitLines || [])
      .filter((ln) => docResId && String(ln.document_result_id ?? "") === docResId)
      .map((ln) => Number(ln.source_detail_id))
      .filter((n) => Number.isFinite(n)),
  )
  const registered = expectedIds.filter((id) => registeredIds.has(id)).length
  const pending = Math.max(0, expectedIds.length - registered)
  const complete = pending === 0
  let label = "Pendiente"
  if (complete) {
    const st = String(docMeta?.result_status || "")
    if (st === "full") label = "Completo"
    else if (st === "partial") label = "Parcial"
    else if (st === "none") label = "Sin entrega"
    else label = "Registrado"
  } else if (registered > 0) {
    label = "En registro"
  }
  return {
    registrable: true,
    expected: expectedIds.length,
    registered,
    pending,
    complete,
    label,
    ok: complete,
  }
}

export default function EntregaDocumentosPage() {
  const { planId, customerKey } = useDeliveryParams()
  const router = useRouter()
  const session = useVisitSession(planId, customerKey)
  const fichaHref = buildEntregaHref(planId, customerKey, "ficha")
  const [gateErr, setGateErr] = useState<string | null>(null)

  const savedDocs = session.visitDetail?.documents || []
  const savedLines = session.visitDetail?.lines || []

  const perDoc = useMemo(
    () =>
      session.documents.map((doc) => ({
        doc,
        prog: docLineProgress(doc, savedDocs, savedLines),
      })),
    [session.documents, savedDocs, savedLines],
  )

  const missingTotal = perDoc.reduce(
    (acc, x) => acc + (x.prog.registrable ? x.prog.pending : 0),
    0,
  )
  const allSaved = perDoc.length > 0 && missingTotal === 0

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
            onClick={() => {
              if (!allSaved) {
                setGateErr(
                  `Faltan ${missingTotal} línea(s) por registrar antes de cobranza`,
                )
                return
              }
              setGateErr(null)
              router.push(buildEntregaHref(planId, customerKey, "cobranza"))
            }}
          >
            Continuar a cobranza
          </button>
        </div>
      }
    >
      {!allSaved ? (
        <p className="muted" style={{ margin: 0, fontSize: "0.82rem" }}>
          Registre todas las líneas de cada documento (confirmación en servidor)
          para habilitar cobranza. Precargar cantidades no cuenta como registro.
        </p>
      ) : null}

      {gateErr ? <div className="banner banner-error">{gateErr}</div> : null}

      <div className="stack-gap">
        {perDoc.map(({ doc, prog }) => {
          const key = docKeyFromDocument(doc) || String(Math.random())
          const href = buildEntregaHref(planId, customerKey, "documentos", {
            docKey: key,
          })
          return (
            <div key={key} className="card">
              <div className="client-card-head">
                <h3 style={{ fontSize: "1rem", margin: 0 }}>
                  {doc.document_type || "Documento"} {doc.document_number ?? "—"}
                </h3>
                <span className={`pill${prog.ok ? " pill-ok" : ""}`}>{prog.label}</span>
              </div>
              <div className="muted" style={{ fontSize: "0.85rem", marginTop: 6 }}>
                {moneyCLP(doc.document_total)} · {(doc.products || []).length} línea(s)
                {doc.group_assignment_conflict
                  ? ` · ${doc.group_assignment_conflict.message_es}`
                  : ""}
              </div>
              {prog.registrable ? (
                <>
                  <div className="line-progress" role="status">
                    Registrados {prog.registered} de {prog.expected} · Pendientes{" "}
                    {prog.pending}
                  </div>
                  <Link href={href} className="btn btn-secondary" style={{ marginTop: 10 }}>
                    {prog.complete ? "Revisar / editar entrega" : "Registrar entrega"}
                  </Link>
                </>
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
