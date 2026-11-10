"use client"

import { useRouter } from "next/navigation"
import { useMemo, useState } from "react"
import {
  ApiError,
  type DeliveryVisit,
  startDeliveryVisit,
} from "@/lib/api"
import { buildEntregaHref, visitStatusLabel } from "@/lib/delivery-flow"

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

/** Ficha: solo estado + Comenzar/Retomar. El circuito vive en /entrega/*. */
export function DeliveryVisitStart(props: Props) {
  const router = useRouter()
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [confirmStart, setConfirmStart] = useState(false)

  const visit = props.activeVisit
  const inProgress = visit?.status === "in_progress"
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
