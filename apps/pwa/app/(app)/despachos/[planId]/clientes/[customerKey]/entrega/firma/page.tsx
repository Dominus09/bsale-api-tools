"use client"

import Link from "next/link"
import { useRouter } from "next/navigation"
import { useMemo, useState } from "react"
import {
  ApiError,
  finalizeDeliveryVisit,
  generateVisitWhatsappReport,
  type WhatsappReportResponse,
} from "@/lib/api"
import { DeliverySignaturePad } from "@/components/delivery-signature-pad"
import { DeliveryStepChrome } from "@/components/delivery-step-chrome"
import { WhatsappSharePanel } from "@/components/whatsapp-share-panel"
import { buildEntregaHref, moneyCLP } from "@/lib/delivery-flow"
import { useDeliveryParams } from "@/lib/use-delivery-params"
import { useVisitSession } from "@/lib/use-visit-session"

export default function EntregaFirmaPage() {
  const { planId, customerKey } = useDeliveryParams()
  const router = useRouter()
  const session = useVisitSession(planId, customerKey)
  const cobranzaHref = buildEntregaHref(planId, customerKey, "cobranza")
  const [conformityOk, setConformityOk] = useState(false)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [confirmEnd, setConfirmEnd] = useState(false)
  const [report, setReport] = useState<WhatsappReportResponse | null>(null)
  const [closed, setClosed] = useState(false)

  const detail = session.visitDetail
  const lines = detail?.lines || []
  const docs = detail?.documents || []
  const cols = detail?.collections || []
  const existingConf = detail?.conformity

  const summaryLines = useMemo(() => {
    const out: string[] = []
    out.push(`Cliente: ${session.clientName}`)
    for (const d of docs) {
      out.push(
        `${d.document_type || "Doc"} ${d.document_number ?? "—"} · ${moneyCLP(
          typeof d.document_total === "number" ? d.document_total : null,
        )} · estado ${d.result_status || "—"}`,
      )
    }
    for (const ln of lines) {
      const planned = Number(ln.planned_units || 0)
      const delivered = Number(ln.delivered_units || 0)
      const undel = Math.max(0, planned - delivered)
      const name = String(ln.product_name || "Producto")
      let s = `${name}: ${delivered}/${planned} un.`
      if (undel > 0) {
        s += ` · faltante ${undel}`
        if (ln.undelivered_reason) s += ` (${ln.undelivered_reason})`
        if (ln.undelivered_note) s += ` · ${ln.undelivered_note}`
      }
      out.push(s)
    }
    for (const c of cols) {
      const method = String(c.method || "—")
      const declared = moneyCLP(
        typeof c.declared_amount === "number" ? c.declared_amount : null,
      )
      const pending = moneyCLP(
        typeof c.pending_balance === "number" ? c.pending_balance : null,
      )
      out.push(`Cobranza ${method}: recibido ${declared} · saldo ${pending}`)
    }
    if (!docs.length) out.push("Sin documentos registrados en la visita")
    if (!lines.length) out.push("Sin líneas de cantidad guardadas")
    return out
  }, [session.clientName, docs, lines, cols])

  async function onFinalize() {
    const visit = session.activeVisit
    if (!visit) return
    setBusy(true)
    setErr(null)
    try {
      const gps = await new Promise<{
        lat: number
        lng: number
        accuracy: number | null
        at: string
      } | null>((resolve) => {
        if (!navigator.geolocation) {
          resolve(null)
          return
        }
        navigator.geolocation.getCurrentPosition(
          (pos) =>
            resolve({
              lat: pos.coords.latitude,
              lng: pos.coords.longitude,
              accuracy:
                typeof pos.coords.accuracy === "number"
                  ? pos.coords.accuracy
                  : null,
              at: new Date(pos.timestamp || Date.now()).toISOString(),
            }),
          () => resolve(null),
          { enableHighAccuracy: true, timeout: 20000, maximumAge: 0 },
        )
      })
      const hasPartial = lines.some(
        (ln) => Number(ln.delivered_units || 0) < Number(ln.planned_units || 0),
      )
      const status = hasPartial ? "partial" : "completed"
      const res = await finalizeDeliveryVisit(planId, visit.id, {
        expected_version: visit.version,
        status,
        latitude: gps?.lat ?? null,
        longitude: gps?.lng ?? null,
        accuracy_m: gps?.accuracy ?? null,
        gps_captured_at: gps?.at,
        gps_source: gps ? "device" : undefined,
        gps_exception_code: gps ? undefined : "gps_unavailable",
        gps_exception_note: gps ? undefined : "GPS no disponible al finalizar",
      })
      if (!res.persisted) {
        setErr("El servidor no confirmó el cierre.")
        return
      }
      setClosed(true)
      setConfirmEnd(false)
      try {
        const wa = await generateVisitWhatsappReport(planId, res.visit.id, {
          client_name: session.clientName,
        })
        setReport(wa)
      } catch {
        /* cierre ok */
      }
      router.push(
        buildEntregaHref(planId, customerKey, "resultado", {
          query: { visitId: res.visit.id },
        }),
      )
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : "No se pudo finalizar")
    } finally {
      setBusy(false)
    }
  }

  if (session.loading) return <div className="empty">Cargando conformidad…</div>
  if (session.error) return <div className="banner banner-error">{session.error}</div>
  if (!session.activeVisit || session.activeVisit.status !== "in_progress") {
    if (closed) {
      return <div className="empty">Redirigiendo al resultado…</div>
    }
    return (
      <div className="stack-gap">
        <div className="banner banner-info">Visita no activa.</div>
        <Link href={buildEntregaHref(planId, customerKey, "ficha")} className="btn btn-primary">
          Ir a ficha
        </Link>
      </div>
    )
  }

  const confReady =
    conformityOk ||
    Boolean(existingConf && (existingConf.refused || existingConf.signature_object_key || existingConf.receptor_name))

  return (
    <DeliveryStepChrome
      clientName={session.clientName}
      step={3}
      title="Paso 3 · Firma y cierre"
      backHref={cobranzaHref}
      footer={
        confReady && !confirmEnd ? (
          <button
            type="button"
            className="btn btn-primary"
            disabled={busy}
            onClick={() => setConfirmEnd(true)}
          >
            Finalizar entrega…
          </button>
        ) : confirmEnd ? (
          <div className="btn-row">
            <button
              type="button"
              className="btn btn-primary"
              disabled={busy}
              onClick={() => void onFinalize()}
            >
              {busy ? "Confirmando…" : "Confirmar cierre"}
            </button>
            <button
              type="button"
              className="btn btn-ghost"
              disabled={busy}
              onClick={() => setConfirmEnd(false)}
            >
              Volver
            </button>
          </div>
        ) : null
      }
    >
      {confReady ? (
        <div className="banner banner-info">
          Conformidad registrada
          {existingConf?.receptor_name
            ? ` · ${String(existingConf.receptor_name)}`
            : ""}
          {existingConf?.refused ? " · negativa a firmar" : ""}.
        </div>
      ) : (
        <DeliverySignaturePad
          planId={planId}
          visitId={session.activeVisit.id}
          summaryLines={summaryLines}
          onDone={() => {
            setConformityOk(true)
            session.reload()
          }}
        />
      )}

      {confirmEnd ? (
        <p className="muted" style={{ fontSize: "0.8rem", margin: 0 }}>
          El servidor validará cantidades, evidencia/firma y GPS. No descuenta stock.
          Cierre idempotente (una sola vez efectiva).
        </p>
      ) : null}

      {report ? (
        <WhatsappSharePanel
          reportText={report.report_text}
          share={report.share}
          header={report.header}
          busy={busy}
          generateLabel="Regenerar reporte"
          onGenerate={async () => {
            if (!session.activeVisit) return
            const res = await generateVisitWhatsappReport(
              planId,
              session.activeVisit.id,
              { client_name: session.clientName },
            )
            setReport(res)
          }}
        />
      ) : null}

      {err ? <div className="banner banner-error">{err}</div> : null}
    </DeliveryStepChrome>
  )
}
