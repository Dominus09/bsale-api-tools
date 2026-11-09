"use client"

import { useRef, useState } from "react"
import {
  ApiError,
  proposeClientLocation,
  uploadFacadePhoto,
} from "@/lib/api"

type LocReg = {
  status?: string
  label_es?: string
  can_register?: boolean
  pending_proposal_id?: number | null
  verified?: boolean
}

type Facade = {
  has_photo?: boolean
  photo_id?: number | null
  status?: string | null
  url?: string | null
}

type Props = {
  planId: number
  customerKey: string
  address?: string | null
  city?: string | null
  latitude?: number | null
  longitude?: number | null
  coordinatesSource?: string | null
  locationRegistration?: LocReg | null
  facade?: Facade | null
  facadePhoto?: string | null
  navigationUrl?: string | null
  warnAccuracyM?: number
  onChanged: () => void
}

async function compressImage(file: File, maxSide = 1600, quality = 0.82): Promise<Blob> {
  const bmp = await createImageBitmap(file)
  const scale = Math.min(1, maxSide / Math.max(bmp.width, bmp.height))
  const w = Math.max(1, Math.round(bmp.width * scale))
  const h = Math.max(1, Math.round(bmp.height * scale))
  const canvas = document.createElement("canvas")
  canvas.width = w
  canvas.height = h
  const ctx = canvas.getContext("2d")
  if (!ctx) return file
  ctx.drawImage(bmp, 0, 0, w, h)
  bmp.close()
  return new Promise((resolve, reject) => {
    canvas.toBlob(
      (b) => (b ? resolve(b) : reject(new Error("No se pudo comprimir"))),
      "image/jpeg",
      quality,
    )
  })
}

export function ClientLocationCapture(props: Props) {
  const [busy, setBusy] = useState<string | null>(null)
  const [msg, setMsg] = useState<string | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [draftGps, setDraftGps] = useState<{
    lat: number
    lng: number
    accuracy: number | null
    at: string
  } | null>(null)
  const [previewUrl, setPreviewUrl] = useState<string | null>(null)
  const [previewBlob, setPreviewBlob] = useState<Blob | null>(null)
  const fileRef = useRef<HTMLInputElement>(null)
  const sending = useRef(false)

  const loc = props.locationRegistration
  const canReg = Boolean(loc?.can_register)

  const captureGps = () => {
    setErr(null)
    setMsg(null)
    if (!navigator.geolocation) {
      setErr("Este dispositivo no ofrece geolocalización.")
      return
    }
    setBusy("gps")
    navigator.geolocation.getCurrentPosition(
      (pos) => {
        setDraftGps({
          lat: pos.coords.latitude,
          lng: pos.coords.longitude,
          accuracy: pos.coords.accuracy ?? null,
          at: new Date(pos.timestamp).toISOString(),
        })
        setBusy(null)
        setMsg("GPS capturado. Revise y confirme para enviar la propuesta.")
      },
      (e) => {
        setBusy(null)
        if (e.code === e.PERMISSION_DENIED) {
          setErr("Permiso de ubicación denegado. Actívelo en Ajustes del teléfono.")
        } else {
          setErr(e.message || "No se pudo obtener GPS. Reintente al aire libre.")
        }
      },
      { enableHighAccuracy: true, timeout: 20000, maximumAge: 0 },
    )
  }

  const submitGps = async (opts?: {
    confirm_low_accuracy?: boolean
    confirm_significant_move?: boolean
  }) => {
    if (!draftGps || sending.current) return
    sending.current = true
    setBusy("save-gps")
    setErr(null)
    try {
      const idem = `gps-${props.planId}-${props.customerKey}-${draftGps.at}`
      const res = (await proposeClientLocation(props.planId, props.customerKey, {
        latitude: draftGps.lat,
        longitude: draftGps.lng,
        accuracy_m: draftGps.accuracy,
        captured_at: draftGps.at,
        confirm_low_accuracy: opts?.confirm_low_accuracy,
        confirm_significant_move: opts?.confirm_significant_move,
        idempotency_key: idem,
      })) as {
        requires_confirmation?: boolean
        reason?: string
        message_es?: string
        proposal?: unknown
      }
      if (res.requires_confirmation) {
        const ok = window.confirm(res.message_es || "¿Confirmar de todos modos?")
        if (!ok) {
          setMsg(res.message_es || "Confirmación requerida")
          return
        }
        await submitGps({
          confirm_low_accuracy: true,
          confirm_significant_move: true,
        })
        return
      }
      setMsg(res.message_es || "Propuesta enviada")
      setDraftGps(null)
      props.onChanged()
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : "No se pudo guardar la ubicación")
    } finally {
      setBusy(null)
      sending.current = false
    }
  }

  const onPickFile = async (file: File | null) => {
    if (!file) return
    setErr(null)
    try {
      const blob = await compressImage(file)
      if (previewUrl) URL.revokeObjectURL(previewUrl)
      const url = URL.createObjectURL(blob)
      setPreviewUrl(url)
      setPreviewBlob(blob)
      setMsg("Vista previa lista. Confirme para enviar.")
    } catch {
      setErr("No se pudo preparar la fotografía")
    }
  }

  const submitPhoto = async () => {
    if (!previewBlob || sending.current) return
    sending.current = true
    setBusy("photo")
    setErr(null)
    try {
      const idem = `foto-${props.planId}-${props.customerKey}-${Date.now()}`
      const res = (await uploadFacadePhoto(props.planId, props.customerKey, previewBlob, {
        idempotency_key: idem,
        contentType: "image/jpeg",
      })) as { message_es?: string }
      setMsg(res.message_es || "Fotografía enviada")
      setPreviewBlob(null)
      if (previewUrl) URL.revokeObjectURL(previewUrl)
      setPreviewUrl(null)
      props.onChanged()
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : "No se pudo subir la fotografía")
    } finally {
      setBusy(null)
      sending.current = false
    }
  }

  const photoSrc = previewUrl || props.facade?.url || props.facadePhoto || null

  return (
    <div className="stack-gap">
      <div className="card">
        <h3 style={{ margin: "0 0 8px", fontSize: "1rem" }}>Ubicación del local</h3>
        <p className="muted" style={{ margin: "0 0 6px", fontSize: "0.85rem" }}>
          {props.address || "Sin dirección"}
          {props.city ? ` · ${props.city}` : ""}
        </p>
        <p className="muted" style={{ margin: "0 0 6px", fontSize: "0.8rem" }}>
          {props.latitude == null || props.longitude == null
            ? "Sin coordenadas"
            : `${Number(props.latitude).toFixed(5)}, ${Number(props.longitude).toFixed(5)}`}
          {props.coordinatesSource ? ` · fuente: ${props.coordinatesSource}` : ""}
        </p>
        <p style={{ margin: "0 0 10px", fontSize: "0.85rem" }}>
          Estado: <strong>{loc?.label_es || "—"}</strong>
        </p>
        {draftGps ? (
          <div className="banner banner-info" style={{ marginBottom: 8 }}>
            Captura: {draftGps.lat.toFixed(5)}, {draftGps.lng.toFixed(5)}
            {draftGps.accuracy != null ? ` · ±${Math.round(draftGps.accuracy)} m` : ""}
          </div>
        ) : null}
        <div className="stack-gap">
          <button
            type="button"
            className="btn btn-primary"
            disabled={!canReg || busy != null}
            onClick={captureGps}
          >
            {busy === "gps" ? "Obteniendo GPS…" : "Registrar / corregir ubicación"}
          </button>
          {draftGps ? (
            <>
              <button
                type="button"
                className="btn btn-primary"
                disabled={busy != null}
                onClick={() => void submitGps()}
              >
                {busy === "save-gps" ? "Enviando…" : "Confirmar y enviar propuesta"}
              </button>
              <button
                type="button"
                className="btn btn-ghost"
                disabled={busy != null}
                onClick={captureGps}
              >
                Reintentar captura
              </button>
            </>
          ) : null}
          {props.navigationUrl ? (
            <a
              className="btn btn-ghost"
              style={{ textAlign: "center" }}
              href={props.navigationUrl}
              target="_blank"
              rel="noopener noreferrer"
            >
              Cómo llegar
            </a>
          ) : null}
        </div>
      </div>

      <div className="card">
        <h3 style={{ margin: "0 0 8px", fontSize: "1rem" }}>Fotografía del establecimiento</h3>
        <div className="facade-ph" aria-hidden={!photoSrc}>
          {photoSrc ? (
            // eslint-disable-next-line @next/next/no-img-element
            <img src={photoSrc} alt="Fachada" />
          ) : (
            <span>Sin foto de fachada</span>
          )}
        </div>
        {props.facade?.status ? (
          <p className="muted" style={{ fontSize: "0.8rem" }}>
            Estado foto: {props.facade.status}
          </p>
        ) : null}
        <input
          ref={fileRef}
          type="file"
          accept="image/*"
          capture="environment"
          style={{ display: "none" }}
          onChange={(e) => void onPickFile(e.target.files?.[0] || null)}
        />
        <div className="stack-gap" style={{ marginTop: 8 }}>
          <button
            type="button"
            className="btn btn-primary"
            disabled={!canReg || busy != null}
            onClick={() => fileRef.current?.click()}
          >
            Tomar / elegir fotografía
          </button>
          {previewBlob ? (
            <>
              <button
                type="button"
                className="btn btn-primary"
                disabled={busy != null}
                onClick={() => void submitPhoto()}
              >
                {busy === "photo" ? "Enviando…" : "Confirmar y enviar foto"}
              </button>
              <button
                type="button"
                className="btn btn-ghost"
                disabled={busy != null}
                onClick={() => fileRef.current?.click()}
              >
                Repetir fotografía
              </button>
            </>
          ) : null}
        </div>
      </div>

      {msg ? <div className="banner banner-info">{msg}</div> : null}
      {err ? <div className="banner banner-error">{err}</div> : null}
    </div>
  )
}
