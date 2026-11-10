"use client"

import Link from "next/link"
import { useRouter } from "next/navigation"
import { useRef, useState } from "react"
import {
  ApiError,
  finalizeDeliveryVisit,
  registerVisitEvidenceException,
  uploadVisitEvidence,
} from "@/lib/api"
import { buildEntregaHref } from "@/lib/delivery-flow"
import { useDeliveryParams } from "@/lib/use-delivery-params"
import { useVisitSession } from "@/lib/use-visit-session"

const REASONS = [
  { value: "closed_store", label: "Local cerrado" },
  { value: "not_found", label: "Local no encontrado" },
  { value: "absent_customer", label: "Cliente ausente" },
  { value: "rejected_order", label: "Pedido rechazado" },
  { value: "other", label: "Otro" },
]

export default function EntregaIncidenciaPage() {
  const { planId, customerKey } = useDeliveryParams()
  const router = useRouter()
  const session = useVisitSession(planId, customerKey)
  const fileRef = useRef<HTMLInputElement | null>(null)
  const [reason, setReason] = useState("closed_store")
  const [note, setNote] = useState("")
  const [exceptionNote, setExceptionNote] = useState("")
  const [evidenceOk, setEvidenceOk] = useState(false)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [info, setInfo] = useState<string | null>(null)

  const fichaHref = buildEntregaHref(planId, customerKey, "ficha")
  const docsHref = buildEntregaHref(planId, customerKey, "documentos")

  async function captureGps() {
    return new Promise<{
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
  }

  async function onPhoto(file: File) {
    if (!session.activeVisit) return
    setBusy(true)
    setErr(null)
    try {
      const gps = await captureGps()
      const idem =
        typeof crypto !== "undefined" && "randomUUID" in crypto
          ? crypto.randomUUID()
          : `ev-${Date.now()}`
      await uploadVisitEvidence(planId, session.activeVisit.id, file, {
        evidence_kind: reason,
        latitude: gps?.lat,
        longitude: gps?.lng,
        accuracy_m: gps?.accuracy,
        idempotency_key: idem,
      })
      setEvidenceOk(true)
      setInfo("Fotografía de incidencia guardada (R2 privado).")
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : "Error al subir evidencia")
    } finally {
      setBusy(false)
    }
  }

  async function onException() {
    if (!session.activeVisit) return
    setBusy(true)
    setErr(null)
    try {
      const gps = await captureGps()
      await registerVisitEvidenceException(planId, session.activeVisit.id, {
        evidence_kind: reason,
        exception_note: exceptionNote,
        latitude: gps?.lat,
        longitude: gps?.lng,
      })
      setEvidenceOk(true)
      setInfo(
        "Excepción auditada sin foto. El cierre quedará pendiente de revisión (no verificado).",
      )
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : "Error excepción")
    } finally {
      setBusy(false)
    }
  }

  async function onFinalize() {
    const visit = session.activeVisit
    if (!visit) return
    if (!evidenceOk) {
      setErr("Registre foto de evidencia o excepción auditable antes de cerrar.")
      return
    }
    setBusy(true)
    setErr(null)
    try {
      const gps = await captureGps()
      const res = await finalizeDeliveryVisit(planId, visit.id, {
        expected_version: visit.version,
        status: "not_delivered",
        result_reason: reason,
        result_note: note || null,
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

  if (session.loading) return <div className="empty">Cargando…</div>
  if (session.error) return <div className="banner banner-error">{session.error}</div>
  if (!session.activeVisit || session.activeVisit.status !== "in_progress") {
    return (
      <div className="stack-gap">
        <div className="banner banner-info">Visita no activa.</div>
        <Link href={fichaHref} className="btn btn-primary">
          Ir a ficha
        </Link>
      </div>
    )
  }

  return (
    <div className="stack-gap">
      <Link href={docsHref} className="btn btn-ghost btn-compact">
        ← Volver al circuito
      </Link>
      <div className="card">
        <h2 style={{ marginTop: 0 }}>Incidencia · {session.clientName}</h2>
        <p className="muted" style={{ fontSize: "0.85rem" }}>
          No obliga a registrar cantidades ni cobranza. Sin evidencia verificable el
          cierre queda pendiente de revisión.
        </p>
        <label className="field">
          Motivo
          <select value={reason} onChange={(e) => setReason(e.target.value)}>
            {REASONS.map((r) => (
              <option key={r.value} value={r.value}>
                {r.label}
              </option>
            ))}
          </select>
        </label>
        <label className="field">
          Observación
          <textarea rows={2} value={note} onChange={(e) => setNote(e.target.value)} />
        </label>
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
        <div className="stack-gap">
          <button
            type="button"
            className="btn btn-secondary"
            disabled={busy}
            onClick={() => fileRef.current?.click()}
          >
            {evidenceOk ? "Evidencia OK · Tomar otra" : "Tomar fotografía actual"}
          </button>
          <label className="field">
            Si falla cámara: excepción auditable
            <textarea
              rows={2}
              value={exceptionNote}
              onChange={(e) => setExceptionNote(e.target.value)}
              placeholder="Motivo técnico (mín. 8 caracteres)"
            />
          </label>
          <button
            type="button"
            className="btn btn-ghost"
            disabled={busy || exceptionNote.trim().length < 8}
            onClick={() => void onException()}
          >
            Registrar excepción sin foto
          </button>
          <button
            type="button"
            className="btn btn-primary"
            disabled={busy || !evidenceOk}
            onClick={() => void onFinalize()}
          >
            {busy ? "Guardando…" : "Guardar resultado de incidencia"}
          </button>
        </div>
      </div>
      {err ? <div className="banner banner-error">{err}</div> : null}
      {info ? <div className="banner banner-info">{info}</div> : null}
    </div>
  )
}
