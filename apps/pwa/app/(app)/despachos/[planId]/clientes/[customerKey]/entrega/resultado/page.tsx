"use client"

import Link from "next/link"
import { Suspense, useEffect, useMemo, useState } from "react"
import { useSearchParams } from "next/navigation"
import {
  ApiError,
  generateVisitWhatsappReport,
  getVisitDetail,
  type DeliveryVisit,
  type WhatsappReportResponse,
} from "@/lib/api"
import { WhatsappSharePanel } from "@/components/whatsapp-share-panel"
import { buildEntregaHref, moneyCLP, visitStatusLabel } from "@/lib/delivery-flow"
import { isClosedVisitStatus } from "@/lib/finalize-visit-nav"
import { buildResultadoSummary } from "@/lib/resultado-summary"
import { resolveVisitRecovery } from "@/lib/visit-recovery"
import { useDeliveryParams } from "@/lib/use-delivery-params"
import { useVisitSession } from "@/lib/use-visit-session"

type VisitDetailPayload = Awaited<ReturnType<typeof getVisitDetail>>

function ResultadoInner() {
  const { planId, customerKey } = useDeliveryParams()
  const search = useSearchParams()
  const visitIdParam = Number(search.get("visitId") || 0)
  const session = useVisitSession(planId, customerKey)
  const [detail, setDetail] = useState<VisitDetailPayload | null>(null)
  const [report, setReport] = useState<WhatsappReportResponse | null>(null)
  const [busy, setBusy] = useState(false)
  const [loadingVisit, setLoadingVisit] = useState(true)
  const [err, setErr] = useState<string | null>(null)

  const recovery = useMemo(
    () =>
      resolveVisitRecovery({
        activeVisit: session.activeVisit,
        latestVisit: session.latestVisit,
        lastClosedVisit: session.lastClosedVisit,
      }),
    [session.activeVisit, session.latestVisit, session.lastClosedVisit],
  )

  const visitId =
    (Number.isFinite(visitIdParam) && visitIdParam > 0 ? visitIdParam : 0) ||
    recovery.preferredResultVisitId ||
    0

  useEffect(() => {
    if (!visitId) {
      setLoadingVisit(false)
      setDetail(null)
      return
    }
    let cancelled = false
    async function load() {
      setLoadingVisit(true)
      try {
        const d = await getVisitDetail(planId, visitId)
        if (cancelled) return
        setDetail(d)
        setErr(null)
        if (isClosedVisitStatus(d.visit?.status)) {
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

  const visit: DeliveryVisit | null = detail?.visit || null
  const summary = useMemo(
    () =>
      buildResultadoSummary({
        docs: detail?.documents || [],
        lines: detail?.lines || [],
        collections: detail?.collections || [],
        conformity: detail?.conformity || null,
      }),
    [detail],
  )

  const clientsHref = `/despachos/${planId}/clientes`
  const fichaHref = buildEntregaHref(planId, customerKey, "ficha")
  const status = visit?.status
  const closed = isClosedVisitStatus(status)
  const review = visit?.closure_review_status

  const statusTone =
    status === "completed"
      ? "ok"
      : status === "partial" || status === "not_delivered"
        ? "warn"
        : "neutral"

  if (loadingVisit || session.loading) {
    return <div className="empty">Cargando resultado…</div>
  }

  if (!visitId) {
    return (
      <div className="stack-gap">
        <div className="banner banner-info">
          No hay visita cerrada para mostrar. Si hay una en curso, retómela desde
          la ficha.
        </div>
        {recovery.canResumeActive ? (
          <Link
            href={buildEntregaHref(planId, customerKey, "documentos")}
            className="btn btn-primary"
          >
            Retomar visita activa
          </Link>
        ) : null}
        <Link href={fichaHref} className="btn btn-ghost">
          Ir a ficha
        </Link>
      </div>
    )
  }

  return (
    <div className="stack-gap">
      <div className="card" style={{ textAlign: "center", paddingTop: 22 }}>
        <div
          className={`result-badge result-badge-${statusTone}`}
          aria-hidden
        >
          {status === "completed" ? "✓" : status === "not_delivered" ? "✕" : "!"}
        </div>
        <h2 style={{ margin: "12px 0 4px", fontSize: "1.25rem" }}>
          {closed ? "Entrega registrada" : "Estado de visita"}
        </h2>
        <p style={{ margin: "0 0 10px", fontWeight: 650 }}>{session.clientName}</p>
        <div className="ficha-summary" style={{ justifyContent: "center" }}>
          <span
            className={`pill${
              statusTone === "ok" ? " pill-ok" : statusTone === "warn" ? " pill-warn" : ""
            }`}
          >
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
      </div>

      {closed ? (
        <div className="card">
          <ul className="result-summary-list">
            {summary.compactLines.map((line, i) => (
              <li key={`sum-${i}`}>{line}</li>
            ))}
          </ul>
          {(detail?.documents || []).map((d, i) => (
            <div key={`doc-${i}`} className="meta-row" style={{ marginTop: 6 }}>
              <span className="muted">
                {d.document_type || "Doc"} {d.document_number ?? "—"}
              </span>
              <span>{moneyCLP(d.document_total as number | null | undefined)}</span>
            </div>
          ))}
        </div>
      ) : null}

      {closed ? (
        <WhatsappSharePanel
          reportText={report?.report_text || null}
          share={report?.share}
          header={report?.header}
          busy={busy}
          generateLabel="Generar reporte"
          onGenerate={async () => {
            if (!visitId) return
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
          Esta visita aún no está cerrada. Use el circuito activo o la ficha.
        </div>
      )}

      {recovery.adminReviewPending ? (
        <div className="banner banner-info" style={{ fontSize: "0.82rem" }}>
          Existe una visita anterior pendiente de revisión administrativa.
        </div>
      ) : null}

      {recovery.canResumeActive && recovery.active?.id !== visitId ? (
        <Link
          href={buildEntregaHref(planId, customerKey, "documentos")}
          className="btn btn-secondary"
        >
          Retomar visita activa #{recovery.active!.id}
        </Link>
      ) : null}

      <Link href={clientsHref} className="btn btn-secondary">
        Volver a clientes
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
