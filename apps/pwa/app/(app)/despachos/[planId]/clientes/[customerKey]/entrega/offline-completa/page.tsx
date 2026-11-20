"use client"

import Link from "next/link"
import { useEffect, useMemo, useRef, useState } from "react"
import { notifyOutboxChanged } from "@/components/offline-outbox-pending-badge"
import { getSessionUserId } from "@/lib/auth"
import { PAYMENT_METHODS, type PaymentMethod } from "@/lib/delivery-collection"
import { buildEntregaHref } from "@/lib/delivery-flow"
import {
  extractPickingFromReady,
  findActiveOutboxForCustomer,
  saveCompleteDeliveryLocal,
  type OutboxOperation,
} from "@/lib/offline-outbox"
import { readOfflineClientDetail } from "@/lib/offline-load"
import { getOfflinePlanReady } from "@/lib/offline-store"
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

export default function OfflineCompletaPage() {
  const { planId, customerKey } = useDeliveryParams()
  const lock = useRef(false)
  const [payload, setPayload] = useState<unknown>(null)
  const [clientName, setClientName] = useState<string | null>(null)
  const [clientId, setClientId] = useState<number | null>(null)
  const [picking, setPicking] = useState<{
    pickingId: number | null
    pickingVersion: string | null
  }>({ pickingId: null, pickingVersion: null })
  const [existing, setExisting] = useState<OutboxOperation | null>(null)
  const [method, setMethod] = useState<PaymentMethod | "">("")
  const [note, setNote] = useState("")
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
      const detail = await readOfflineClientDetail(planId, customerKey)
      if (cancelled) return
      if (detail.status !== "success") {
        setLoadErr(detail.message)
        return
      }
      setPayload(detail.payload)
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

  const docCount = useMemo(() => {
    const p = payload as { documents?: unknown[] } | null
    return Array.isArray(p?.documents) ? p!.documents!.length : 0
  }, [payload])

  async function onSave() {
    if (lock.current || busy) return
    const uid = getSessionUserId()
    if (!uid) {
      setErr("Sin sesión.")
      return
    }
    if (!method) {
      setErr("Seleccione la forma de pago.")
      return
    }
    lock.current = true
    setBusy(true)
    setErr(null)
    setOkMsg(null)
    try {
      const gps = await captureGps()
      const res = await saveCompleteDeliveryLocal({
        userId: uid,
        planId,
        customerKey,
        clientId,
        clientName,
        fichaPayload: payload,
        picking,
        payment: { method },
        note: note.trim() || null,
        gps: gps
          ? {
              lat: gps.lat,
              lng: gps.lng,
              accuracy_m: gps.accuracy,
              captured_at: gps.at,
              source: "deferred_offline",
            }
          : {
              lat: null,
              lng: null,
              accuracy_m: null,
              captured_at: null,
              source: "exception",
              exception_code: "gps_unavailable",
              exception_note: "GPS no disponible al registrar offline",
            },
        resumeLocalUuid:
          existing?.op_type === "complete_delivery" ? existing.localUuid : null,
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

  if (!payload) return <div className="empty">Cargando snapshot…</div>

  if (existing && existing.op_type !== "complete_delivery") {
    return (
      <div className="stack-gap">
        <div className="banner banner-info">
          Ya hay una operación pendiente incompatible ({existing.op_type}) para
          este cliente. No se crea otra.
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
        <strong>Entrega completa local</strong>
        <div style={{ fontSize: "0.8rem", marginTop: 4 }}>
          No es confirmación del servidor. Cantidades = snapshot del picking
          descargado. Sin firma ni receptor.
        </div>
      </div>

      <div className="card">
        <div className="muted" style={{ fontSize: "0.78rem" }}>
          {clientName || customerKey}
        </div>
        <div>
          {docCount} documento(s) · picking{" "}
          {picking.pickingId != null
            ? `#${picking.pickingId} v${picking.pickingVersion || "—"}`
            : "no válido"}
        </div>
      </div>

      {existing?.status === "pending" ? (
        <div className="banner banner-info">
          Ya guardada localmente. Puede actualizar el borrador o volver a la
          ficha.
        </div>
      ) : null}

      <label className="field">
        <span>Forma de pago (sin montos)</span>
        <select
          value={method}
          onChange={(e) => setMethod(e.target.value as PaymentMethod | "")}
          disabled={busy}
        >
          <option value="">Seleccione…</option>
          {PAYMENT_METHODS.map((m) => (
            <option key={m.value} value={m.value}>
              {m.label}
            </option>
          ))}
        </select>
      </label>

      <label className="field">
        <span>Observación (opcional)</span>
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
        className="btn btn-primary"
        disabled={busy || picking.pickingId == null}
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
