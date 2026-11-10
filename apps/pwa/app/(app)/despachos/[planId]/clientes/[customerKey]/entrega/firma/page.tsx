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
  const [detailOpen, setDetailOpen] = useState(false)

  const detail = session.visitDetail
  const lines = detail?.lines || []
  const docs = detail?.documents || []
  const cols = detail?.collections || []
  const existingConf = detail?.conformity

  const { compactLines, detailLines } = useMemo(() => {
    const compact: string[] = []
    const full: string[] = []
    compact.push(`Cliente: ${session.clientName}`)
    let totalAmt = 0
    for (const d of docs) {
      const tot = typeof d.document_total === "number" ? d.document_total : 0
      totalAmt += tot
      compact.push(
        `${d.document_type || "Doc"} ${d.document_number ?? "—"} · ${moneyCLP(tot)} · ${d.result_status || "—"}`,
      )
    }
    compact.push(`Totales documentos: ${moneyCLP(totalAmt)}`)
    let fullN = 0
    let partialN = 0
    let noneN = 0
    for (const ln of lines) {
      const planned = Number(ln.planned_units || 0)
      const delivered = Number(ln.delivered_units || 0)
      if (delivered >= planned) fullN += 1
      else if (delivered <= 0) noneN += 1
      else partialN += 1
      const undel = Math.max(0, planned - delivered)
      let s = `${ln.product_name || "Producto"}: ${delivered}/${planned}`
      if (undel > 0 && ln.undelivered_reason) s += ` · ${ln.undelivered_reason}`
      full.push(s)
    }
    compact.push(
      `Líneas: ${fullN} completas · ${partialN} parciales · ${noneN} no entregadas`,
    )
    const byMethod = new Map<string, number>()
    let pendingSum = 0
    for (const c of cols) {
      const method = String(c.method || "—")
      const declared =
        typeof c.declared_amount === "number" ? c.declared_amount : 0
      byMethod.set(method, (byMethod.get(method) || 0) + declared)
      if (method === "pending") pendingSum += declared
      else if (typeof c.pending_balance === "number") {
        pendingSum += Math.max(0, c.pending_balance)
      }
      full.push(
        `Cobranza ${method}: ${moneyCLP(declared)}${c.note ? ` · ${c.note}` : ""}`,
      )
    }
    for (const [method, amt] of byMethod) {
      compact.push(`Cobranza ${method}: ${moneyCLP(amt)}`)
    }
    if (byMethod.size === 0) compact.push("Sin cobranza declarada")
    compact.push(`Saldo pendiente (declarado): ${moneyCLP(pendingSum)}`)
    if (!docs.length) compact.push("Sin documentos registrados")
    if (!lines.length) compact.push("Sin líneas guardadas")
    return { compactLines: compact, detailLines: full }
  }, [session.clientName, docs, lines, cols])

  const summaryLines = detailOpen
    ? [...compactLines, "— Detalle —", ...detailLines]
    : compactLines

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
                  detailOpen={detailOpen}
                  onToggleDetail={() => setDetailOpen((v) => !v)}
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
