"use client"

import { useRouter } from "next/navigation"
import { useMemo, useState } from "react"
import {
  ApiError,
  type DeliveryVisit,
  startDeliveryVisit,
} from "@/lib/api"
import { buildEntregaHref, visitStatusLabel } from "@/lib/delivery-flow"
import { resultadoHref } from "@/lib/finalize-visit-nav"
import { resolveVisitRecovery } from "@/lib/visit-recovery"

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
  lastClosedVisit?: DeliveryVisit | null
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

/** Ficha: estado + Retomar / Ver resultado histórico / Nueva visita explícita. */
export function DeliveryVisitStart(props: Props) {
  const router = useRouter()
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [confirmStart, setConfirmStart] = useState(false)
  const [confirmNewAfterClosed, setConfirmNewAfterClosed] = useState(false)

  const recovery = useMemo(
    () =>
      resolveVisitRecovery({
        activeVisit: props.activeVisit,
        latestVisit: props.latestVisit,
        lastClosedVisit: props.lastClosedVisit,
      }),
    [props.activeVisit, props.latestVisit, props.lastClosedVisit],
  )

  const inProgress = recovery.canResumeActive
  const closedOnly = !inProgress && recovery.canViewClosedResult

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
      await startDeliveryVisit(props.planId, props.customerKey, body)
      setConfirmStart(false)
      setConfirmNewAfterClosed(false)
      props.onChanged()
      router.push(buildEntregaHref(props.planId, props.customerKey, "documentos"))
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
        {inProgress && recovery.active ? (
          <>
            <span className="pill pill-ok">En atención #{recovery.active.id}</span>
            {recovery.active.started_at_local ? (
              <span className="pill">
                Inicio {recovery.active.started_at_local.slice(11, 16)}
              </span>
            ) : null}
          </>
        ) : recovery.lastClosed ? (
          <>
            <span className="pill pill-ok">
              {visitStatusLabel(recovery.lastClosed.status)} #{recovery.lastClosed.id}
            </span>
            {recovery.lastClosed.ended_at_local ? (
              <span className="pill">
                Cierre {String(recovery.lastClosed.ended_at_local).slice(11, 16)}
              </span>
            ) : null}
          </>
        ) : (
          <span className="pill">{visitStatusLabel(null)}</span>
        )}
      </div>

      {/* Caso dual: activa + histórica — ambos accesibles. */}
      {inProgress && recovery.active ? (
        <div className="stack-gap">
          {!confirmStart ? (
            <button
              type="button"
              className="btn btn-primary"
              disabled={busy}
              onClick={() => setConfirmStart(true)}
            >
              Retomar entrega
            </button>
          ) : (
            <div className="stack-gap">
              <p style={{ margin: 0, fontSize: "0.85rem" }}>
                Continúa la visita #{recovery.active.id} en curso. No crea otra.
              </p>
              <div className="btn-row">
                <button
                  type="button"
                  className="btn btn-primary"
                  disabled={busy}
                  onClick={() => void onStart()}
                >
                  {busy ? "Guardando…" : "Retomar circuito"}
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
          {!confirmStart ? (
            <button
              type="button"
              className="btn btn-ghost"
              disabled={busy}
              onClick={() =>
                router.push(
                  buildEntregaHref(props.planId, props.customerKey, "documentos"),
                )
              }
            >
              Ir al circuito de entrega
            </button>
          ) : null}
          {recovery.canViewClosedResult && recovery.lastClosed ? (
            <button
              type="button"
              className="btn btn-secondary"
              disabled={busy}
              onClick={() =>
                router.push(
                  resultadoHref(
                    props.planId,
                    props.customerKey,
                    recovery.lastClosed!.id,
                  ),
                )
              }
            >
              Ver resultado visita #{recovery.lastClosed.id}
            </button>
          ) : null}
        </div>
      ) : null}

      {closedOnly && recovery.lastClosed ? (
        <div className="stack-gap">
          <p className="muted" style={{ fontSize: "0.82rem", marginTop: 0 }}>
            Entrega cerrada en servidor. Recargar la ficha no crea visitas.
          </p>
          <button
            type="button"
            className="btn btn-primary"
            disabled={busy}
            onClick={() =>
              router.push(
                resultadoHref(
                  props.planId,
                  props.customerKey,
                  recovery.lastClosed!.id,
                ),
              )
            }
          >
            Ver resultado
          </button>
          {!confirmNewAfterClosed ? (
            <button
              type="button"
              className="btn btn-ghost"
              disabled={busy}
              onClick={() => setConfirmNewAfterClosed(true)}
            >
              Iniciar nueva visita
            </button>
          ) : (
            <div className="stack-gap">
              <p style={{ margin: 0, fontSize: "0.85rem" }}>
                Esto creará una visita nueva. Use solo si debe atender de nuevo.
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
        </div>
      ) : null}

      {!inProgress && !closedOnly ? (
        <div className="stack-gap">
          {!confirmStart ? (
            <button
              type="button"
              className="btn btn-primary"
              disabled={busy}
              onClick={() => setConfirmStart(true)}
            >
              Comenzar entrega
            </button>
          ) : (
            <div className="stack-gap">
              <p style={{ margin: 0, fontSize: "0.9rem" }}>
                <strong>{props.clientName}</strong>
                <br />
                {props.documentCount} documento(s) · {props.documentTotalLabel}
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
          )}
        </div>
      ) : null}

      {err ? (
        <div className="banner banner-error" style={{ marginTop: 10 }}>
          {err}
        </div>
      ) : null}
    </div>
  )
}
