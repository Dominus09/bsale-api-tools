"use client"

import Link from "next/link"
import { useEffect, useRef, useState } from "react"
import { notifyOutboxChanged } from "@/components/offline-outbox-pending-badge"
import { getSessionUserId } from "@/lib/auth"
import { buildEntregaHref } from "@/lib/delivery-flow"
import {
  extractPickingFromReady,
  findActiveOutboxForCustomer,
  saveClosedStoreLocal,
  type OutboxOperation,
} from "@/lib/offline-outbox"
import { isOfflineV2UiEnabledForPlan } from "@/lib/offline-outbox-policy"
import { readOfflineClientDetail } from "@/lib/offline-load"
import { getOfflinePlanReady } from "@/lib/offline-store"
import { refreshOfflineV2PilotAuthorization } from "@/lib/offline-v2-pilot-auth"
import { useDeliveryParams } from "@/lib/use-delivery-params"

type GpsFix = {
  lat: number
  lng: number
  accuracy: number | null
  at: string
}

function captureGps(): Promise<GpsFix | null> {
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

export default function OfflineCerradoPage() {
  const { planId, customerKey } = useDeliveryParams()
  const fileRef = useRef<HTMLInputElement | null>(null)
  const lock = useRef(false)
  const [clientName, setClientName] = useState<string | null>(null)
  const [clientId, setClientId] = useState<number | null>(null)
  const [picking, setPicking] = useState<{
    pickingId: number | null
    pickingVersion: string | null
  }>({ pickingId: null, pickingVersion: null })
  const [existing, setExisting] = useState<OutboxOperation | null>(null)
  const [note, setNote] = useState("")
  const [gps, setGps] = useState<GpsFix | null>(null)
  const [gpsTried, setGpsTried] = useState(false)
  const [photoBuf, setPhotoBuf] = useState<ArrayBuffer | null>(null)
  const [photoMeta, setPhotoMeta] = useState<{
    type: string
    at: string
    name: string
  } | null>(null)
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [okMsg, setOkMsg] = useState<string | null>(null)
  const [loadErr, setLoadErr] = useState<string | null>(null)

  const fichaHref = buildEntregaHref(planId, customerKey, "ficha")

  useEffect(() => {
    let cancelled = false
    async function load() {
      const uid = getSessionUserId()
      if (!uid) {
        setLoadErr("Sin sesión local.")
        return
      }
      await refreshOfflineV2PilotAuthorization(planId)
      if (!isOfflineV2UiEnabledForPlan(planId)) {
        setLoadErr(
          "Offline V2 no autorizado para este usuario o despacho. Use el flujo online / Offline V1.",
        )
        return
      }
      const detail = await readOfflineClientDetail(planId, customerKey)
      if (cancelled) return
      if (detail.status !== "success") {
        setLoadErr(detail.message)
        return
      }
      const p = detail.payload as {
        client?: { client_name?: string; client_id?: number | null }
      }
      setClientName(p.client?.client_name || null)
      setClientId(
        p.client?.client_id != null ? Number(p.client.client_id) : null,
      )
      const ready = await getOfflinePlanReady(uid, planId)
      setPicking(extractPickingFromReady(ready))
      const act = await findActiveOutboxForCustomer(uid, planId, customerKey)
      if (act) setExisting(act)
    }
    void load()
    return () => {
      cancelled = true
    }
  }, [planId, customerKey])

  async function onCaptureGps() {
    setBusy(true)
    setErr(null)
    try {
      const fix = await captureGps()
      setGpsTried(true)
      setGps(fix)
      if (!fix) {
        setErr("GPS no disponible. Puede continuar con excepción auditable.")
      }
    } finally {
      setBusy(false)
    }
  }

  async function onPickPhoto(file: File | null) {
    setErr(null)
    setOkMsg(null)
    setPhotoBuf(null)
    setPhotoMeta(null)
    if (!file) return
    try {
      const buf = await file.arrayBuffer()
      if (buf.byteLength < 32) {
        setErr("Archivo de foto inválido o vacío.")
        return
      }
      setPhotoBuf(buf)
      setPhotoMeta({
        type: file.type || "image/jpeg",
        at: new Date().toISOString(),
        name: file.name || "foto.jpg",
      })
    } catch {
      setErr("No se pudo leer la fotografía.")
    }
  }

  async function onSave() {
    if (lock.current || busy) return
    const uid = getSessionUserId()
    if (!uid) {
      setErr("Sin sesión.")
      return
    }
    if (!photoBuf || !photoMeta) {
      setErr("Capture y verifique la fotografía antes de guardar.")
      return
    }
    lock.current = true
    setBusy(true)
    setErr(null)
    setOkMsg(null)
    try {
      let fix = gps
      if (!gpsTried) {
        fix = await captureGps()
        setGpsTried(true)
        setGps(fix)
      }
      const res = await saveClosedStoreLocal({
        userId: uid,
        planId,
        customerKey,
        clientId,
        clientName,
        picking,
        note,
        gps: fix
          ? {
              lat: fix.lat,
              lng: fix.lng,
              accuracy_m: fix.accuracy,
              captured_at: fix.at,
              source: "deferred_offline",
            }
          : {
              lat: null,
              lng: null,
              accuracy_m: null,
              captured_at: null,
              source: "exception",
              exception_code: "gps_unavailable",
              exception_note: "GPS no disponible en local cerrado offline",
            },
        photo: {
          bytes: photoBuf,
          contentType: photoMeta.type,
          capturedAt: photoMeta.at,
        },
        resumeLocalUuid:
          existing?.op_type === "closed_store" ? existing.localUuid : null,
      })
      if (!res.ok) {
        setErr(res.message)
        if (res.existing) setExisting(res.existing)
        return
      }
      setExisting(res.op)
      setOkMsg("Guardado en este dispositivo · Pendiente de sincronización")
      notifyOutboxChanged()
    } finally {
      lock.current = false
      setBusy(false)
    }
  }

  if (loadErr) {
    return (
      <div className="stack-gap">
        <div className="banner banner-error">{loadErr}</div>
        <Link href={fichaHref} className="btn btn-ghost">
          Volver a ficha
        </Link>
      </div>
    )
  }

  if (existing && existing.op_type !== "closed_store") {
    return (
      <div className="stack-gap">
        <div className="banner banner-info">
          Ya hay una operación pendiente incompatible para este cliente.
        </div>
        <Link href={fichaHref} className="btn btn-primary">
          Volver a ficha
        </Link>
      </div>
    )
  }

  return (
    <div className="stack-gap">
      <div className="banner banner-info">
        <strong>Local cerrado local</strong>
        <div style={{ fontSize: "0.8rem", marginTop: 4 }}>
          Foto y GPS se guardan en el dispositivo. No se sube a R2. GPS no
          queda verificado. Sin productos entregados ni cobros.
        </div>
      </div>

      <div className="card">
        <div>{clientName || customerKey}</div>
        <div className="muted" style={{ fontSize: "0.78rem" }}>
          picking{" "}
          {picking.pickingId != null
            ? `#${picking.pickingId} v${picking.pickingVersion || "—"}`
            : "no válido"}
        </div>
      </div>

      <button
        type="button"
        className="btn btn-secondary"
        disabled={busy}
        onClick={() => void onCaptureGps()}
      >
        {gps
          ? `GPS ±${gps.accuracy != null ? Math.round(gps.accuracy) : "—"} m`
          : "Capturar GPS"}
      </button>

      <div className="stack-gap">
        <input
          ref={fileRef}
          type="file"
          accept="image/*"
          capture="environment"
          hidden
          onChange={(e) => {
            const f = e.target.files?.[0] || null
            void onPickPhoto(f)
          }}
        />
        <button
          type="button"
          className="btn btn-secondary"
          disabled={busy}
          onClick={() => fileRef.current?.click()}
        >
          {photoMeta ? "Cambiar fotografía" : "Tomar / elegir fotografía"}
        </button>
        {photoMeta ? (
          <div className="pill pill-ok">
            Foto lista · {(photoBuf?.byteLength || 0)} bytes · no subida a R2
          </div>
        ) : (
          <div className="muted" style={{ fontSize: "0.78rem" }}>
            Debe quedar guardada localmente antes de finalizar.
          </div>
        )}
      </div>

      <label className="field">
        <span>Observación (obligatoria)</span>
        <textarea
          rows={3}
          value={note}
          onChange={(e) => setNote(e.target.value)}
          disabled={busy}
        />
      </label>

      {err ? <div className="banner banner-error">{err}</div> : null}
      {okMsg ? <div className="banner banner-info">{okMsg}</div> : null}

      <button
        type="button"
        className="btn btn-danger"
        disabled={busy || !photoBuf || picking.pickingId == null}
        onClick={() => void onSave()}
      >
        {busy ? "Guardando…" : "Guardar en este dispositivo"}
      </button>
      <Link href={fichaHref} className="btn btn-ghost">
        Volver a ficha
      </Link>
    </div>
  )
}
