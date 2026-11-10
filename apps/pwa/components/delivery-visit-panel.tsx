"use client"

import { useMemo, useState } from "react"
import {
  ApiError,
  type DeliveryVisit,
  finalizeDeliveryVisit,
  startDeliveryVisit,
} from "@/lib/api"

type Props = {
  planId: number
  customerKey: string
  clientName: string
  documentCount: number
  documentTotalLabel: string
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
      setInfo(res.note_es || "Entrega finalizada.")
      props.onChanged()
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
        <div style={{ marginTop: 10 }}>
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

      {err ? <div className="banner banner-error" style={{ marginTop: 10 }}>{err}</div> : null}
      {info ? <div className="banner banner-info" style={{ marginTop: 10 }}>{info}</div> : null}
    </div>
  )
}
