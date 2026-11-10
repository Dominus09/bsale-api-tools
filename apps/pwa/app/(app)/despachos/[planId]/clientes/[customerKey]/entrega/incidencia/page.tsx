"use client"

import Link from "next/link"
import { Suspense, useMemo, useRef, useState } from "react"
import { useSearchParams } from "next/navigation"
import {
  ApiError,
  finalizeDeliveryVisit,
  startDeliveryVisit,
  uploadVisitEvidence,
} from "@/lib/api"
import { buildEntregaHref } from "@/lib/delivery-flow"
import { navigateToResultado } from "@/lib/finalize-visit-nav"
import { useDeliveryParams } from "@/lib/use-delivery-params"
import { useVisitSession } from "@/lib/use-visit-session"

const REASONS = [
  { value: "closed_store", label: "Local cerrado" },
  { value: "rejected_order", label: "Pedido rechazado" },
  { value: "absent_customer", label: "Cliente ausente" },
  { value: "not_found", label: "Local no encontrado" },
  { value: "other", label: "Otro" },
] as const

type GpsFix = {
  lat: number
  lng: number
  accuracy: number | null
  at: string
}

function IncidenciaInner() {
  const { planId, customerKey } = useDeliveryParams()
  const search = useSearchParams()
  const preset = search.get("reason")
  const session = useVisitSession(planId, customerKey)
  const fileRef = useRef<HTMLInputElement | null>(null)

  const [reason, setReason] = useState(
    REASONS.some((r) => r.value === preset) ? String(preset) : "closed_store",
  )
  const [note, setNote] = useState("")
  const [gps, setGps] = useState<GpsFix | null>(null)
  const [gpsErr, setGpsErr] = useState<string | null>(null)
  const [proxExc, setProxExc] = useState(false)
  const [proxExcCode, setProxExcCode] = useState("gps_out_of_range")
  const [proxExcNote, setProxExcNote] = useState("")
  const [evidenceOk, setEvidenceOk] = useState(false)
  const [gpsMeta, setGpsMeta] = useState<Record<string, unknown> | null>(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [info, setInfo] = useState<string | null>(null)
  const [step, setStep] = useState<"gps" | "photo" | "confirm">("gps")

  const fichaHref = buildEntregaHref(planId, customerKey, "ficha")
  const needsProximity = reason === "closed_store" || reason === "rejected_order"
  const reasonLabel = useMemo(
    () => REASONS.find((r) => r.value === reason)?.label || reason,
    [reason],
  )

  async function captureGps(): Promise<GpsFix | null> {
    return new Promise((resolve) => {
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
              typeof pos.coords.accuracy === "number" ? pos.coords.accuracy : null,
            at: new Date(pos.timestamp || Date.now()).toISOString(),
          }),
        () => resolve(null),
        { enableHighAccuracy: true, timeout: 20000, maximumAge: 0 },
      )
    })
  }

  async function ensureVisit() {
    if (session.activeVisit) return session.activeVisit
    const fix = gps || (await captureGps())
    const idem =
      typeof crypto !== "undefined" && "randomUUID" in crypto
        ? crypto.randomUUID()
        : `start-${Date.now()}`
    const body = fix
      ? {
          latitude: fix.lat,
          longitude: fix.lng,
          accuracy_m: fix.accuracy,
          gps_captured_at: fix.at,
          gps_source: "device",
          idempotency_key: idem,
        }
      : {
          gps_exception_code: "gps_unavailable",
          gps_exception_note: "GPS no disponible al iniciar incidencia",
          idempotency_key: idem,
        }
    const res = await startDeliveryVisit(planId, customerKey, body)
    session.reload()
    return res.visit
  }

  async function onCaptureGps() {
    setBusy(true)
    setErr(null)
    setGpsErr(null)
    try {
      const fix = await captureGps()
      if (!fix) {
        setGpsErr("No se pudo obtener GPS. Active ubicación o registre excepción.")
        setProxExc(true)
        setProxExcCode("gps_unavailable")
        return
      }
      setGps(fix)
      setInfo(
        `GPS ±${fix.accuracy != null ? Math.round(fix.accuracy) : "—"} m · listo`,
      )
      setStep("photo")
    } finally {
      setBusy(false)
    }
  }

  async function onPhoto(file: File) {
    setBusy(true)
    setErr(null)
    try {
      const visit = await ensureVisit()
      if (!visit?.id) throw new Error("Sin visita activa")
      const fix = gps || (await captureGps())
      if (fix) setGps(fix)
      const idem =
        typeof crypto !== "undefined" && "randomUUID" in crypto
          ? crypto.randomUUID()
          : `ev-${Date.now()}`
      const res = await uploadVisitEvidence(planId, Number(visit.id), file, {
        evidence_kind: reason,
        latitude: fix?.lat,
        longitude: fix?.lng,
        accuracy_m: fix?.accuracy,
        gps_captured_at: fix?.at,
        proximity_exception_code: proxExc ? proxExcCode : null,
        proximity_exception_note: proxExc ? proxExcNote : null,
        idempotency_key: idem,
      })
      setEvidenceOk(true)
      setGpsMeta((res.gps_meta as Record<string, unknown>) || null)
      setInfo("Fotografía confirmada en servidor (R2 privado).")
      setStep("confirm")
    } catch (e) {
      if (e instanceof ApiError) {
        const code = e.code || ""
        if (code === "GPS_OUT_OF_RANGE" || e.message.includes("300")) {
          setProxExc(true)
          setProxExcCode("gps_out_of_range")
          setErr(e.message)
          setStep("gps")
        } else if (code === "CLIENT_GPS_MISSING") {
          setProxExc(true)
          setProxExcCode("client_gps_missing")
          setErr(e.message)
        } else if (code === "GPS_REQUIRED" || code === "GPS_STALE") {
          setErr(e.message)
          setStep("gps")
        } else {
          setErr(e.message)
        }
      } else {
        setErr("Error al subir evidencia")
      }
    } finally {
      setBusy(false)
    }
  }

  async function onFinalize() {
    setBusy(true)
    setErr(null)
    try {
      const visit = session.activeVisit || (await ensureVisit())
      if (!visit?.id) {
        setErr("No hay visita activa")
        return
      }
      if (!evidenceOk) {
        setErr("La fotografía confirmada es obligatoria.")
        return
      }
      if (proxExc && proxExcNote.trim().length < 8) {
        setErr("Detalle de excepción GPS obligatorio (mín. 8 caracteres).")
        return
      }
      if (reason === "other" && !note.trim()) {
        setErr("Observación obligatoria para 'Otro'.")
        return
      }
      const fix = gps || (await captureGps())
      const res = await finalizeDeliveryVisit(planId, Number(visit.id), {
        expected_version: Number(visit.version || 1),
        status: "not_delivered",
        result_reason: reason,
        result_note: note || null,
        latitude: fix?.lat ?? null,
        longitude: fix?.lng ?? null,
        accuracy_m: fix?.accuracy ?? null,
        gps_captured_at: fix?.at,
        gps_source: fix ? "device" : undefined,
        gps_exception_code: proxExc ? proxExcCode : fix ? undefined : "gps_unavailable",
        gps_exception_note: proxExc
          ? proxExcNote
          : fix
            ? undefined
            : "GPS no disponible al finalizar",
      })
      if (!res.persisted) {
        setErr("El servidor no confirmó el cierre.")
        return
      }
      navigateToResultado(planId, customerKey, Number(res.visit?.id || visit.id))
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : "No se pudo finalizar")
    } finally {
      setBusy(false)
    }
  }

  if (session.loading) return <div className="empty">Cargando…</div>
  if (session.error) return <div className="banner banner-error">{session.error}</div>
  if (!session.canOperateActive && !session.activeVisit) {
    // Permitir iniciar desde incidencia si no hay bloqueo de cierre.
    if (session.lastClosedVisit && !session.adminReviewPending) {
      /* continue if allow — session.canOperateActive false when closed */
    }
  }
  if (session.adminReviewPending || (!session.canOperateActive && session.lastClosedVisit)) {
    return (
      <div className="stack-gap">
        <div className="banner banner-info">
          {session.adminReviewPending
            ? "Existe una visita anterior pendiente de revisión administrativa."
            : "Visita ya finalizada. Consulte el resultado desde la ficha."}
        </div>
        <Link href={fichaHref} className="btn btn-primary">
          Ir a ficha
        </Link>
      </div>
    )
  }

  return (
    <div className="stack-gap">
      <Link href={fichaHref} className="muted" style={{ fontSize: "0.85rem" }}>
        ← Ficha
      </Link>

      <div className="card">
        <div className="ficha-summary" style={{ marginBottom: 10 }}>
          <span className="sem-pill sem-red">{reasonLabel}</span>
        </div>
        <h2 style={{ margin: "0 0 6px", fontSize: "1.1rem" }}>{session.clientName}</h2>
        <p className="muted" style={{ margin: 0, fontSize: "0.82rem" }}>
          Foto obligatoria · GPS ≤ 300 m del cliente · sin firma ni cobranza
        </p>

        <label className="field" style={{ marginTop: 12 }}>
          Motivo
          <select
            value={reason}
            onChange={(e) => {
              setReason(e.target.value)
              setEvidenceOk(false)
              setStep("gps")
            }}
            disabled={busy || evidenceOk}
          >
            {REASONS.map((r) => (
              <option key={r.value} value={r.value}>
                {r.label}
              </option>
            ))}
          </select>
        </label>

        {reason === "other" || note ? (
          <label className="field">
            Observación
            <textarea rows={2} value={note} onChange={(e) => setNote(e.target.value)} />
          </label>
        ) : (
          <button
            type="button"
            className="btn btn-ghost btn-compact"
            onClick={() => setNote(" ")}
          >
            Agregar observación
          </button>
        )}
      </div>

      {step === "gps" || !gps ? (
        <div className="card stack-gap">
          <strong>1. Ubicación</strong>
          <button
            type="button"
            className="btn btn-primary"
            disabled={busy}
            onClick={() => void onCaptureGps()}
          >
            {busy ? "Obteniendo…" : "Capturar GPS"}
          </button>
          {gpsErr ? <div className="banner banner-error">{gpsErr}</div> : null}
          {needsProximity && (proxExc || gpsErr) ? (
            <>
              <label className="field">
                Excepción GPS (queda pendiente de validación)
                <select
                  value={proxExcCode}
                  onChange={(e) => setProxExcCode(e.target.value)}
                >
                  <option value="gps_out_of_range">Fuera de 300 m</option>
                  <option value="client_gps_missing">Cliente sin coordenadas</option>
                  <option value="gps_denied">Permiso denegado</option>
                  <option value="gps_unavailable">Sin señal GPS</option>
                  <option value="gps_inaccurate">Precisión insuficiente</option>
                  <option value="access_blocked">Acceso físicamente impedido</option>
                </select>
              </label>
              <label className="field">
                Motivo de excepción
                <textarea
                  rows={2}
                  value={proxExcNote}
                  onChange={(e) => {
                    setProxExc(true)
                    setProxExcNote(e.target.value)
                  }}
                  placeholder="Detalle obligatorio (≥ 8 caracteres)"
                />
              </label>
              <button
                type="button"
                className="btn btn-secondary"
                disabled={busy || proxExcNote.trim().length < 8}
                onClick={() => setStep("photo")}
              >
                Continuar con excepción GPS
              </button>
            </>
          ) : null}
        </div>
      ) : null}

      {step === "photo" || step === "confirm" ? (
        <div className="card stack-gap">
          <strong>2. Fotografía del establecimiento</strong>
          <input
            ref={fileRef}
            type="file"
            accept="image/*"
            capture="environment"
            hidden
            onChange={(e) => {
              const f = e.target.files?.[0]
              if (f) void onPhoto(f)
              e.target.value = ""
            }}
          />
          <button
            type="button"
            className="btn btn-secondary"
            disabled={busy}
            onClick={() => fileRef.current?.click()}
          >
            {evidenceOk ? "Evidencia OK · Otra foto" : "Abrir cámara"}
          </button>
          {typeof gpsMeta?.distance_m === "number" ? (
            <p className="muted" style={{ margin: 0, fontSize: "0.8rem" }}>
              Distancia validada: {Number(gpsMeta.distance_m).toFixed(0)} m
            </p>
          ) : null}
        </div>
      ) : null}

      {step === "confirm" && evidenceOk ? (
        <div className="card stack-gap">
          <strong>3. Confirmar incidencia</strong>
          <button
            type="button"
            className="btn btn-danger"
            disabled={busy}
            onClick={() => void onFinalize()}
          >
            {busy ? "Guardando…" : `Registrar · ${reasonLabel}`}
          </button>
        </div>
      ) : null}

      {err ? <div className="banner banner-error">{err}</div> : null}
      {info ? <div className="banner banner-info">{info}</div> : null}
    </div>
  )
}

export default function EntregaIncidenciaPage() {
  return (
    <Suspense fallback={<div className="empty">Cargando…</div>}>
      <IncidenciaInner />
    </Suspense>
  )
}
