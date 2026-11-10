"use client"

import Link from "next/link"
import { Suspense, useSearchParams } from "next/navigation"
import { useEffect, useState } from "react"
import {
  ApiError,
  generateVisitWhatsappReport,
  type WhatsappReportResponse,
} from "@/lib/api"
import { WhatsappSharePanel } from "@/components/whatsapp-share-panel"
import { buildEntregaHref, visitStatusLabel } from "@/lib/delivery-flow"
import { useDeliveryParams } from "@/lib/use-delivery-params"
import { useVisitSession } from "@/lib/use-visit-session"

function ResultadoInner() {
  const { planId, customerKey } = useDeliveryParams()
  const search = useSearchParams()
  const visitIdParam = Number(search.get("visitId") || 0)
  const session = useVisitSession(planId, customerKey)
  const [report, setReport] = useState<WhatsappReportResponse | null>(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)

  const visitId =
    visitIdParam ||
    session.visitDetail?.visit?.id ||
    session.activeVisit?.id ||
    0

  useEffect(() => {
    if (!visitId) return
    let cancelled = false
    async function load() {
      try {
        const wa = await generateVisitWhatsappReport(planId, visitId, {
          client_name: session.clientName,
        })
        if (!cancelled) setReport(wa)
      } catch {
        /* opcional */
      }
    }
    void load()
    return () => {
      cancelled = true
    }
  }, [planId, visitId, session.clientName])

  const clientsHref = `/despachos/${planId}/clientes`
  const fichaHref = buildEntregaHref(planId, customerKey, "ficha")

  return (
    <div className="stack-gap">
      <div className="card">
        <h2 style={{ marginTop: 0 }}>Entrega registrada</h2>
        <p style={{ margin: "0 0 8px" }}>
          <strong>{session.clientName}</strong>
        </p>
        <div className="ficha-summary">
          <span className="pill pill-ok">
            {visitStatusLabel(session.visitDetail?.visit?.status) || "Cerrada"}
          </span>
          {visitId ? <span className="pill">Visita #{visitId}</span> : null}
        </div>
        <p className="muted" style={{ fontSize: "0.85rem", marginTop: 10 }}>
          Puede compartir el reporte por WhatsApp manualmente. No se emitieron
          recibos ni se mutó Bsale.
        </p>
      </div>

      <WhatsappSharePanel
        reportText={report?.report_text || null}
        share={report?.share}
        header={report?.header}
        busy={busy}
        generateLabel="Generar / regenerar reporte"
        onGenerate={async () => {
          if (!visitId) throw new Error("Sin visita")
          setBusy(true)
          setErr(null)
          try {
            const res = await generateVisitWhatsappReport(planId, visitId, {
              client_name: session.clientName,
            })
            setReport(res)
          } catch (e) {
            setErr(e instanceof ApiError ? e.message : "Error reporte")
            throw e
          } finally {
            setBusy(false)
          }
        }}
      />

      <Link href={clientsHref} className="btn btn-primary">
        Volver a lista de clientes
      </Link>
      <Link href={fichaHref} className="btn btn-ghost">
        Volver a ficha
      </Link>
      {err ? <div className="banner banner-error">{err}</div> : null}
    </div>
  )
}

export default function EntregaResultadoPage() {
  return (
    <Suspense fallback={<div className="empty">Cargando resultado…</div>}>
      <ResultadoInner />
    </Suspense>
  )
}
