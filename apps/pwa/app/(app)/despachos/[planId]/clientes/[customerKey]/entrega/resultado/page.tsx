"use client"

import Link from "next/link"
import { Suspense, useEffect, useState } from "react"
import { useSearchParams } from "next/navigation"
import {
  ApiError,
  generateVisitWhatsappReport,
  getVisitDetail,
  type DeliveryVisit,
  type WhatsappReportResponse,
} from "@/lib/api"
import { WhatsappSharePanel } from "@/components/whatsapp-share-panel"
import { buildEntregaHref, visitStatusLabel } from "@/lib/delivery-flow"
import { isClosedVisitStatus } from "@/lib/finalize-visit-nav"
import { useDeliveryParams } from "@/lib/use-delivery-params"
import { useVisitSession } from "@/lib/use-visit-session"

function ResultadoInner() {
  const { planId, customerKey } = useDeliveryParams()
  const search = useSearchParams()
  const visitIdParam = Number(search.get("visitId") || 0)
  const session = useVisitSession(planId, customerKey)
  const [visit, setVisit] = useState<DeliveryVisit | null>(null)
  const [report, setReport] = useState<WhatsappReportResponse | null>(null)
  const [busy, setBusy] = useState(false)
  const [loadingVisit, setLoadingVisit] = useState(true)
  const [err, setErr] = useState<string | null>(null)

  const visitId =
    (Number.isFinite(visitIdParam) && visitIdParam > 0 ? visitIdParam : 0) ||
    session.latestVisit?.id ||
    session.visitDetail?.visit?.id ||
    0

  useEffect(() => {
    if (!visitId) {
      setLoadingVisit(false)
      return
    }
    let cancelled = false
    async function load() {
      setLoadingVisit(true)
      try {
        const detail = await getVisitDetail(planId, visitId)
        if (cancelled) return
        setVisit(detail.visit)
        setErr(null)
        if (isClosedVisitStatus(detail.visit?.status)) {
          try {
            const wa = await generateVisitWhatsappReport(planId, visitId, {
              client_name: session.clientName,
            })
            if (!cancelled) setReport(wa)
          } catch {
            /* reporte opcional */
          }
        }
      } catch (e) {
        if (cancelled) return
        setErr(
          e instanceof ApiError
            ? e.message
            : "No se pudo cargar el resultado de la visita",
        )
      } finally {
        if (!cancelled) setLoadingVisit(false)
      }
    }
    void load()
    return () => {
      cancelled = true
    }
  }, [planId, visitId, session.clientName])

  const clientsHref = `/despachos/${planId}/clientes`
  const fichaHref = buildEntregaHref(planId, customerKey, "ficha")
  const status = visit?.status || session.latestVisit?.status
  const closed = isClosedVisitStatus(status)
  const review = visit?.closure_review_status

  if (loadingVisit || session.loading) {
    return <div className="empty">Cargando resultado…</div>
  }

  if (!visitId) {
    return (
      <div className="stack-gap">
        <div className="banner banner-info">
          No hay visita para mostrar. Si acaba de cerrar, vuelva a la ficha.
        </div>
        <Link href={fichaHref} className="btn btn-primary">
          Ir a ficha
        </Link>
      </div>
    )
  }

  return (
    <div className="stack-gap">
      <div className="card">
        <h2 style={{ marginTop: 0 }}>
          {closed ? "Entrega finalizada" : "Estado de visita"}
        </h2>
        <p style={{ margin: "0 0 8px" }}>
          <strong>{session.clientName}</strong>
        </p>
        <div className="ficha-summary">
          <span className={`pill${closed ? " pill-ok" : ""}`}>
            {visitStatusLabel(status)}
          </span>
          <span className="pill">Visita #{visitId}</span>
          {review ? (
            <span className="pill">
              {review === "verified"
                ? "Cierre verificado"
                : review === "pending_review"
                  ? "Pendiente de revisión"
                  : String(review)}
            </span>
          ) : null}
        </div>
        <p className="muted" style={{ fontSize: "0.85rem", marginTop: 10 }}>
          Estado leído desde el servidor. Puede compartir el reporte por WhatsApp
          manualmente. No se emitieron recibos ni se mutó Bsale.
        </p>
      </div>

      {closed ? (
        <WhatsappSharePanel
          reportText={report?.report_text || null}
          share={report?.share}
          header={report?.header}
          busy={busy}
          showFamilyActions
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
            } finally {
              setBusy(false)
            }
          }}
        />
      ) : (
        <div className="banner banner-info">
          La visita aún no está cerrada. Continue el circuito o finalice desde
          Firma.
        </div>
      )}

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
