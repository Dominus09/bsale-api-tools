"use client"

import Link from "next/link"
import { useMemo, useRef, useState } from "react"
import { ApiError, finalizeDeliveryVisit } from "@/lib/api"
import { DeliveryStepChrome } from "@/components/delivery-step-chrome"
import {
  filterActiveCollections,
  summarizeCollections,
} from "@/lib/active-collections"
import { docStatusLabel, paymentMethodLabel } from "@/lib/delivery-labels"
import { resolveSemaphore } from "@/lib/semaphore"
import { buildEntregaHref, moneyCLP } from "@/lib/delivery-flow"
import {
  isClosedVisitStatus,
  navigateToResultado,
  resultadoHref,
} from "@/lib/finalize-visit-nav"
import { useDeliveryParams } from "@/lib/use-delivery-params"
import { useVisitSession } from "@/lib/use-visit-session"

export default function EntregaCierrePage() {
  const { planId, customerKey } = useDeliveryParams()
  const session = useVisitSession(planId, customerKey)
  const cobranzaHref = buildEntregaHref(planId, customerKey, "cobranza")
  const [obs, setObs] = useState("")
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [closingMsg, setClosingMsg] = useState<string | null>(null)
  const finalizeLock = useRef(false)

  const detail = session.visitDetail
  const lines = detail?.lines || []
  const docs = detail?.documents || []
  const allCols = detail?.collections || []

  const docResIds = useMemo(
    () =>
      docs
        .map((d) => (d.id != null ? Number(d.id) : null))
        .filter((n): n is number => n != null && Number.isFinite(n)),
    [docs],
  )

  const cols = useMemo(
    () => filterActiveCollections(allCols, docResIds),
    [allCols, docResIds],
  )

  const hasPartial = useMemo(
    () =>
      lines.some(
        (ln) => Number(ln.delivered_units || 0) < Number(ln.planned_units || 0),
      ),
    [lines],
  )
  const status = hasPartial ? "partial" : "completed"
  const obsRequired = hasPartial
  const obsTrim = obs.trim()

  const { payLabel, blockReason, sem } = useMemo(() => {
    let totalAmt = 0
    for (const d of docs) {
      const tot = typeof d.document_total === "number" ? d.document_total : 0
      totalAmt += tot
    }
    const summary = summarizeCollections(cols, totalAmt)
    const declarative = cols.length > 0 && cols.every((c) => c.declared_amount == null)
    const methods = Array.from(summary.byMethod.keys())
    const pay =
      methods.length > 0
        ? methods.map((m) => paymentMethodLabel(m)).join(" · ")
        : "Sin forma de pago declarada"

    let block: string | null = null
    const orphans = allCols.filter((c) => c.document_result_id == null)
    if (orphans.length > 0) {
      block =
        "Hay declaraciones antiguas sin documento. Vuelva a forma de pago y guarde de nuevo."
    }
    if (!declarative && summary.declaredAll - totalAmt > 0.009) {
      block =
        "La cobranza vigente no cuadra con el total. Corrija antes de finalizar."
    }
    if (docs.length > 0 && cols.length === 0) {
      block = "Declare la forma de pago de cada documento antes de finalizar."
    }

    const semaphore = resolveSemaphore({
      status,
      resultReason: null,
    })
    return { payLabel: pay, blockReason: block, sem: semaphore }
  }, [docs, cols, allCols, status])

  async function onFinalize() {
    if (finalizeLock.current || busy) return
    const visit = session.activeVisit
    if (!visit) return
    if (obsRequired && obsTrim.length < 3) {
      setErr("La entrega parcial requiere observaciones de entrega.")
      return
    }
    finalizeLock.current = true
    setBusy(true)
    setErr(null)
    setClosingMsg("Guardando cierre…")
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
      const res = await finalizeDeliveryVisit(planId, visit.id, {
        expected_version: visit.version,
        status,
        delivery_observations: obsTrim || null,
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
        setClosingMsg(null)
        return
      }
      const closedId = Number(res.visit?.id || visit.id)
      if (!Number.isFinite(closedId) || closedId < 1) {
        setErr("Cierre guardado, pero falta el id de visita. Abra la ficha.")
        setClosingMsg(null)
        return
      }
      setClosingMsg("Cierre confirmado. Abriendo resultado…")
      navigateToResultado(planId, customerKey, closedId)
    } catch (e) {
      if (e instanceof ApiError && e.status === 409) {
        const closed = session.lastClosedVisit || session.latestVisit
        if (closed && isClosedVisitStatus(closed.status)) {
          setClosingMsg("La visita ya estaba cerrada. Abriendo resultado…")
          navigateToResultado(planId, customerKey, closed.id)
          return
        }
      }
      setErr(e instanceof ApiError ? e.message : "No se pudo finalizar")
      setClosingMsg(null)
    } finally {
      finalizeLock.current = false
      setBusy(false)
    }
  }

  if (session.loading) return <div className="empty">Cargando cierre…</div>
  if (session.error) return <div className="banner banner-error">{session.error}</div>

  const latest = session.latestVisit
  if (latest && isClosedVisitStatus(latest.status) && !session.activeVisit) {
    return (
      <div className="stack-gap">
        <div className="banner banner-info">
          Esta visita ya fue cerrada. No se inicia otra automáticamente.
        </div>
        <Link
          href={resultadoHref(planId, customerKey, latest.id)}
          className="btn btn-primary"
        >
          Ver resultado
        </Link>
        <Link href={buildEntregaHref(planId, customerKey, "ficha")} className="btn btn-ghost">
          Ir a ficha
        </Link>
      </div>
    )
  }

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

  const canFinalize = !blockReason && (!obsRequired || obsTrim.length >= 3)

  return (
    <DeliveryStepChrome
      clientName={session.clientName}
      step={3}
      title="Paso 3 · Cierre"
      backHref={cobranzaHref}
      footer={
        <button
          type="button"
          className="btn btn-primary"
          disabled={busy || !canFinalize}
          onClick={() => void onFinalize()}
        >
          {busy ? "Guardando cierre…" : "Finalizar entrega"}
        </button>
      }
    >
      <div className="card">
        <div className="ficha-summary" style={{ marginBottom: 10 }}>
          <span className={`sem-pill ${sem.cssClass}`}>{sem.labelEs}</span>
          <span className="pill">{docs.length} doc{docs.length === 1 ? "" : "s"}</span>
        </div>
        <h2 style={{ margin: "0 0 8px", fontSize: "1.05rem" }}>{session.clientName}</h2>
        <ul className="muted" style={{ margin: "0 0 10px", paddingLeft: 18, fontSize: "0.85rem" }}>
          {docs.map((d) => (
            <li key={String(d.id ?? d.document_number)}>
              {d.document_type || "Doc"} {d.document_number ?? "—"} ·{" "}
              {moneyCLP(typeof d.document_total === "number" ? d.document_total : null)} ·{" "}
              {docStatusLabel(String(d.result_status || ""))}
            </li>
          ))}
        </ul>
        <p style={{ margin: 0, fontSize: "0.9rem" }}>
          Forma de pago: <strong>{payLabel}</strong>
        </p>
      </div>

      <div className="card">
        <label className="field" htmlFor="delivery-obs">
          Observaciones de entrega
          {obsRequired ? " (obligatorias)" : " (opcionales)"}
        </label>
        <textarea
          id="delivery-obs"
          className="input delivery-obs-input"
          rows={5}
          value={obs}
          onChange={(e) => setObs(e.target.value)}
          placeholder="Ej.: Cliente recibió parte del pedido. Quedaron 2 cajas pendientes."
          disabled={busy}
          maxLength={1000}
        />
        <p className="muted" style={{ margin: "6px 0 0", fontSize: "0.75rem" }}>
          Distinto de las observaciones comerciales del cliente en la ficha. No
          reemplaza cantidades ni formas de pago registradas.
        </p>
      </div>

      {blockReason ? <div className="banner banner-error">{blockReason}</div> : null}
      {closingMsg ? <div className="banner banner-info">{closingMsg}</div> : null}
      {err ? <div className="banner banner-error">{err}</div> : null}
    </DeliveryStepChrome>
  )
}
