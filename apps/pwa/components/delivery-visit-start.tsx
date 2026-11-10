"use client"

import { useRouter } from "next/navigation"
import { useMemo, useState } from "react"
import {
  ApiError,
  type DeliveryVisit,
  startDeliveryVisit,
} from "@/lib/api"
import { buildEntregaHref, visitStatusLabel } from "@/lib/delivery-flow"
import {
  isClosedVisitStatus,
  resultadoHref,
} from "@/lib/finalize-visit-nav"

type Props = {
  planId: number
  customerKey: string
  clientName: string
  documentCount: number
  documentTotalLabel: string
  p4Ready: boolean
  p4Note?: string | null
  activeVisit: DeliveryVisit | null
  latestVisit?: DeliveryVisit | null
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

/** Ficha: solo estado + Comenzar/Retomar. El circuito vive en /entrega/*. */
export function DeliveryVisitStart(props: Props) {
  const router = useRouter()
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [confirmStart, setConfirmStart] = useState(false)
  const [confirmNewAfterClosed, setConfirmNewAfterClosed] = useState(false)

  const visit = props.activeVisit
  const latest = props.latestVisit || props.activeVisit
  const inProgress = visit?.status === "in_progress"
  const closedLatest =
    !inProgress && latest != null && isClosedVisitStatus(latest.status)

  const startLabel = useMemo(
    () => (inProgress ? "Retomar entrega" : "Comenzar entrega"),
    [inProgress],
  )

  async function onStart() {
    setBusy(true)
    setErr(null)
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
      setConfirmNewAfterClosed(false)
      props.onChanged()
      const href = buildEntregaHref(props.planId, props.customerKey, "documentos")
      router.push(href)
      if (res.resumed || res.created || res.idempotent_replay) {
        /* navegación es la confirmación visual */
      }
    } catch (e) {
      if (!navigator.onLine) {
        setErr("Sin conexión. P4 es online-only: no se confirma entrega sin servidor.")
      } else if (e instanceof ApiError) {
        setErr(e.message)
      } else {
        setErr("No se pudo iniciar la visita")
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
      </div>
    )
  }

  if (closedLatest && latest) {
    return (
      <div className="card">
        <div className="ficha-summary" style={{ marginBottom: 10 }}>
          <span className="pill pill-ok">{visitStatusLabel(latest.status)}</span>
          <span className="pill">Visita #{latest.id}</span>
          {latest.ended_at_local ? (
            <span className="pill">
              Cierre {String(latest.ended_at_local).slice(11, 16)}
            </span>
          ) : null}
        </div>
        <p className="muted" style={{ fontSize: "0.82rem", marginTop: 0 }}>
          Entrega ya cerrada en servidor. No se crea otra visita al abrir el
          resultado.
        </p>
        <button
          type="button"
          className="btn btn-primary"
          disabled={busy}
          onClick={() =>
            router.push(resultadoHref(props.planId, props.customerKey, latest.id))
          }
        >
          Ver resultado
        </button>
        {!confirmNewAfterClosed ? (
          <button
            type="button"
            className="btn btn-ghost"
            style={{ marginTop: 8 }}
            disabled={busy}
            onClick={() => setConfirmNewAfterClosed(true)}
          >
            Iniciar nueva visita
          </button>
        ) : (
          <div className="stack-gap" style={{ marginTop: 8 }}>
            <p style={{ margin: 0, fontSize: "0.85rem" }}>
              Esto creará una visita nueva. Use solo si debe atender de nuevo al
              mismo cliente.
            </p>
            <div className="btn-row">
              <button
                type="button"
                className="btn btn-primary"
                disabled={busy}
                onClick={() => void onStart()}
              >
                {busy ? "Guardando…" : "Confirmar nueva visita"}
              </button>
              <button
                type="button"
                className="btn btn-ghost"
                disabled={busy}
                onClick={() => setConfirmNewAfterClosed(false)}
              >
                Cancelar
              </button>
            </div>
          </div>
        )}
        {err ? (
          <div className="banner banner-error" style={{ marginTop: 10 }}>
            {err}
          </div>
        ) : null}
      </div>
    )
  }

  return (
    <div className="card">
      <div className="ficha-summary" style={{ marginBottom: 10 }}>
        <span className={`pill${inProgress ? " pill-ok" : ""}`}>
          {inProgress ? "En atención" : visitStatusLabel(visit?.status)}
        </span>
        {visit?.started_at_local ? (
          <span className="pill">Inicio {visit.started_at_local.slice(11, 16)}</span>
        ) : null}
      </div>

      {!confirmStart ? (
        <button
          type="button"
          className="btn btn-primary"
          disabled={busy}
          onClick={() => setConfirmStart(true)}
        >
          {startLabel}
        </button>
      ) : (
        <div className="stack-gap" style={{ marginTop: 8 }}>
          <p style={{ margin: 0, fontSize: "0.9rem" }}>
            <strong>{props.clientName}</strong>
            <br />
            {props.documentCount} documento(s) · {props.documentTotalLabel}
            <br />
            <span className="muted" style={{ fontSize: "0.8rem" }}>
              Se registrará inicio en servidor (GPS o excepción auditable). No se crea
              otra visita si ya hay una en curso.
            </span>
          </p>
          <div className="btn-row">
            <button
              type="button"
              className="btn btn-primary"
              disabled={busy}
              onClick={() => void onStart()}
            >
              {busy ? "Guardando…" : inProgress ? "Retomar circuito" : "Confirmar y comenzar"}
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
      )}

      {inProgress && !confirmStart ? (
        <button
          type="button"
          className="btn btn-ghost"
          style={{ marginTop: 8 }}
          disabled={busy}
          onClick={() =>
            router.push(buildEntregaHref(props.planId, props.customerKey, "documentos"))
          }
        >
          Ir al circuito de entrega
        </button>
      ) : null}

      {err ? (
        <div className="banner banner-error" style={{ marginTop: 10 }}>
          {err}
        </div>
      ) : null}
    </div>
  )
}
