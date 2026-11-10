"use client"

import { useMemo, useState } from "react"
import {
  ApiError,
  type DeliveryVisit,
  type WhatsappReportResponse,
  declareVisitCollection,
  finalizeDeliveryVisit,
  generateVisitWhatsappReport,
  startDeliveryVisit,
} from "@/lib/api"
import { WhatsappSharePanel } from "@/components/whatsapp-share-panel"

type Props = {
  planId: number
  customerKey: string
  clientName: string
  documentCount: number
  documentTotalLabel: string
  documentTotalAmount?: number
  p4Ready: boolean
  p4Note?: string | null
  activeVisit: DeliveryVisit | null
  onChanged: () => void
}

function captureGps(): Promise<{
  lat: number
  lng: number
  accuracy: number | null
  at: string
} | null> {
  return new Promise((resolve) => {
    if (!navigator.geolocation) {
      resolve(null)
      return
    }
    navigator.geolocation.getCurrentPosition(
      (pos) => {
        resolve({
          lat: pos.coords.latitude,
          lng: pos.coords.longitude,
          accuracy:
            typeof pos.coords.accuracy === "number" ? pos.coords.accuracy : null,
          at: new Date(pos.timestamp || Date.now()).toISOString(),
        })
      },
      () => resolve(null),
      { enableHighAccuracy: true, timeout: 20000, maximumAge: 0 },
    )
  })
}

export function DeliveryVisitPanel(props: Props) {
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [info, setInfo] = useState<string | null>(null)
  const [confirmStart, setConfirmStart] = useState(false)
  const [confirmEnd, setConfirmEnd] = useState(false)
  const [endStatus, setEndStatus] = useState<"completed" | "partial" | "not_delivered">(
    "completed",
  )
  const [endReason, setEndReason] = useState("closed_store")
  const [endNote, setEndNote] = useState("")
  const [payMethod, setPayMethod] = useState("cash")
  const [payAmount, setPayAmount] = useState("")
  const [lastClosedId, setLastClosedId] = useState<number | null>(null)
  const [report, setReport] = useState<WhatsappReportResponse | null>(null)

  const visit = props.activeVisit
  const inProgress = visit?.status === "in_progress"

  const startLabel = useMemo(() => {
    if (inProgress) return "Retomar entrega en curso"
    return "Comenzar entrega"
  }, [inProgress])

  async function onStart() {
    setBusy(true)
    setErr(null)
    setInfo(null)
    try {
      const gps = await captureGps()
      const idem =
        typeof crypto !== "undefined" && "randomUUID" in crypto
          ? crypto.randomUUID()
          : `start-${Date.now()}`
      const body = gps
        ? {
            latitude: gps.lat,
            longitude: gps.lng,
            accuracy_m: gps.accuracy,
            gps_captured_at: gps.at,
            gps_source: "device",
            idempotency_key: idem,
          }
        : {
            gps_exception_code: "gps_unavailable",
            gps_exception_note:
              "GPS no disponible o permiso denegado en el dispositivo",
            idempotency_key: idem,
          }
      const res = await startDeliveryVisit(props.planId, props.customerKey, body)
      setConfirmStart(false)
      setInfo(
        res.resumed
          ? "Visita en atención reanudada."
          : res.created
            ? "Visita iniciada · En atención (online)."
            : "Visita recuperada.",
      )
      props.onChanged()
    } catch (e) {
      if (!navigator.onLine) {
        setErr(
          "Sin conexión. P4 es online-only: no se confirma entrega sin servidor.",
        )
      } else if (e instanceof ApiError) {
        setErr(e.message)
      } else {
        setErr("No se pudo iniciar la visita")
      }
    } finally {
      setBusy(false)
    }
  }

  async function onFinalize() {
    if (!visit) return
    setBusy(true)
    setErr(null)
    setInfo(null)
    try {
      const gps = await captureGps()
      const res = await finalizeDeliveryVisit(props.planId, visit.id, {
        expected_version: visit.version,
        status: endStatus,
        result_reason: endStatus === "not_delivered" ? endReason : null,
        result_note:
          endStatus === "not_delivered" && endReason === "other"
            ? endNote
            : endNote || null,
        latitude: gps?.lat ?? null,
        longitude: gps?.lng ?? null,
        accuracy_m: gps?.accuracy ?? null,
        gps_captured_at: gps?.at,
        gps_source: gps ? "device" : undefined,
      })
      if (!res.persisted) {
        setErr("El servidor no confirmó el cierre.")
        return
      }
      setConfirmEnd(false)
      setLastClosedId(res.visit.id)
      setReport(null)
      setInfo(res.note_es || "Entrega finalizada. Puede generar el reporte WhatsApp.")
      props.onChanged()
      // Auto-generar texto del reporte (no envía). Fallo no bloquea el cierre.
      try {
        const wa = await generateVisitWhatsappReport(props.planId, res.visit.id, {
          client_name: props.clientName,
        })
        setReport(wa)
      } catch {
        // El cierre ya persistió; el chofer puede regenerar manualmente.
      }
    } catch (e) {
      if (!navigator.onLine) {
        setErr("Sin conexión. No se finaliza hasta confirmar con el servidor.")
      } else if (e instanceof ApiError) {
        setErr(e.message)
      } else {
        setErr("No se pudo finalizar")
      }
    } finally {
      setBusy(false)
    }
  }

  if (!props.p4Ready) {
    return (
      <div className="banner banner-info" style={{ fontSize: "0.85rem" }}>
        Visitas de entrega no disponibles aún
        {props.p4Note ? `: ${props.p4Note}` : " (migración 056 pendiente)."}
        <div className="muted" style={{ marginTop: 6 }}>
          Modo online-only: no se simula entrega confirmada sin red.
        </div>
      </div>
    )
  }

  return (
    <div className="card">
      <div className="ficha-summary" style={{ marginBottom: 10 }}>
        <span className={`pill${inProgress ? " pill-ok" : ""}`}>
          {inProgress ? "En atención" : "Sin iniciar"}
        </span>
        {visit?.started_at_local ? (
          <span className="pill">Inicio {visit.started_at_local.slice(11, 16)}</span>
        ) : null}
      </div>

      {!confirmStart && !confirmEnd ? (
        <button
          type="button"
          className="btn btn-primary"
          disabled={busy}
          onClick={() => setConfirmStart(true)}
        >
          {startLabel}
        </button>
      ) : null}

      {confirmStart ? (
        <div className="stack-gap" style={{ marginTop: 8 }}>
          <p style={{ margin: 0, fontSize: "0.9rem" }}>
            <strong>{props.clientName}</strong>
            <br />
            {props.documentCount} documento(s) · {props.documentTotalLabel}
            <br />
            <span className="muted">
              Se capturará GPS (o excepción auditable). Online-only.
            </span>
          </p>
          <div className="btn-row">
            <button
              type="button"
              className="btn btn-primary"
              disabled={busy}
              onClick={() => void onStart()}
            >
              {busy ? "Guardando…" : "Confirmar y comenzar"}
            </button>
            <button
              type="button"
              className="btn btn-ghost"
              disabled={busy}
              onClick={() => setConfirmStart(false)}
            >
              Cancelar
            </button>
          </div>
        </div>
      ) : null}

      {inProgress && !confirmStart ? (
        <div style={{ marginTop: 10 }} className="stack-gap">
          <div className="card" style={{ padding: 12 }}>
            <strong style={{ fontSize: "0.9rem" }}>Cobranza declarada</strong>
            <p className="muted" style={{ margin: "4px 0 8px", fontSize: "0.78rem" }}>
              No es recibo oficial ni verificación contable.
            </p>
            <label className="field">
              Método
              <select value={payMethod} onChange={(e) => setPayMethod(e.target.value)}>
                <option value="cash">Efectivo recibido</option>
                <option value="transfer_declared">Transferencia informada</option>
                <option value="credit">Crédito autorizado</option>
                <option value="partial">Cobranza parcial</option>
                <option value="pending">Pago pendiente</option>
                <option value="not_applicable">No corresponde cobranza</option>
              </select>
            </label>
            <label className="field">
              Monto declarado
              <input
                inputMode="decimal"
                value={payAmount}
                onChange={(e) => setPayAmount(e.target.value)}
                placeholder={
                  props.documentTotalAmount != null
                    ? String(props.documentTotalAmount)
                    : "0"
                }
              />
            </label>
            <button
              type="button"
              className="btn btn-secondary"
              disabled={busy}
              onClick={() => {
                void (async () => {
                  if (!visit) return
                  setBusy(true)
                  setErr(null)
                  try {
                    const amt = payAmount.trim() ? Number(payAmount) : null
                    await declareVisitCollection(props.planId, visit.id, {
                      method: payMethod,
                      document_amount: props.documentTotalAmount ?? null,
                      declared_amount: amt,
                    })
                    setInfo("Cobranza declarada (auditada).")
                  } catch (e) {
                    setErr(e instanceof ApiError ? e.message : "Error cobranza")
                  } finally {
                    setBusy(false)
                  }
                })()
              }}
            >
              Guardar cobranza
            </button>
          </div>

          {!confirmEnd ? (
            <button
              type="button"
              className="btn btn-secondary"
              disabled={busy}
              onClick={() => setConfirmEnd(true)}
            >
              Finalizar entrega…
            </button>
          ) : (
            <div className="stack-gap">
              <label className="field">
                Resultado
                <select
                  value={endStatus}
                  onChange={(e) =>
                    setEndStatus(e.target.value as typeof endStatus)
                  }
                >
                  <option value="completed">Entrega completa</option>
                  <option value="partial">Entrega parcial</option>
                  <option value="not_delivered">No entregada</option>
                </select>
              </label>
              {endStatus === "not_delivered" ? (
                <>
                  <label className="field">
                    Motivo
                    <select
                      value={endReason}
                      onChange={(e) => setEndReason(e.target.value)}
                    >
                      <option value="closed_store">Local cerrado</option>
                      <option value="not_found">Local no encontrado</option>
                      <option value="rejected_order">Pedido rechazado</option>
                      <option value="absent_customer">Cliente ausente</option>
                      <option value="other">Otro</option>
                    </select>
                  </label>
                  <label className="field">
                    Observación{endReason === "other" ? " (obligatoria)" : ""}
                    <textarea
                      value={endNote}
                      onChange={(e) => setEndNote(e.target.value)}
                      rows={2}
                    />
                  </label>
                </>
              ) : null}
              <p className="muted" style={{ margin: 0, fontSize: "0.8rem" }}>
                Evidencia fotográfica y firma se habilitan en módulos siguientes.
                El cierre no descuenta stock ni modifica Bsale.
              </p>
              <div className="btn-row">
                <button
                  type="button"
                  className="btn btn-primary"
                  disabled={busy}
                  onClick={() => void onFinalize()}
                >
                  {busy ? "Confirmando…" : "Finalizar entrega"}
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
            </div>
          )}
        </div>
      ) : null}

      {lastClosedId && !inProgress ? (
        <div style={{ marginTop: 10 }} className="card">
          <strong style={{ fontSize: "0.95rem" }}>Resumen y WhatsApp</strong>
          <p className="muted" style={{ margin: "6px 0 10px", fontSize: "0.8rem" }}>
            Cierre persistido. Compartir no reabre ni duplica la visita.
          </p>
          <WhatsappSharePanel
            reportText={report?.report_text || null}
            share={report?.share}
            header={report?.header}
            busy={busy}
            generateLabel="Generar / regenerar reporte"
            onGenerate={async () => {
              setBusy(true)
              setErr(null)
              try {
                const res = await generateVisitWhatsappReport(
                  props.planId,
                  lastClosedId,
                  { client_name: props.clientName },
                )
                setReport(res)
                setInfo(
                  res.share.note_es ||
                    "Reporte listo. Confirme el envío manualmente en WhatsApp.",
                )
              } catch (e) {
                setErr(e instanceof ApiError ? e.message : "Error reporte")
                throw e
              } finally {
                setBusy(false)
              }
            }}
          />
        </div>
      ) : null}

      {err ? <div className="banner banner-error" style={{ marginTop: 10 }}>{err}</div> : null}
      {info ? <div className="banner banner-info" style={{ marginTop: 10 }}>{info}</div> : null}
    </div>
  )
}
