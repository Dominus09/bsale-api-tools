"use client"

import { useEffect, useRef, useState } from "react"
import { ApiError, submitVisitConformity } from "@/lib/api"
import { receptorNameIsValid } from "@/lib/receptor-name"

type Props = {
  planId: number
  visitId: number
  summaryLines: string[]
  summaryCompact?: boolean
  onToggleDetail?: () => void
  detailOpen?: boolean
  onDone: () => void
  blockReason?: string | null
}

/**
 * P4.3 — Conformidad.
 *
 * Causa raíz previa: authHeaders forzaba Content-Type: application/json sobre
 * FormData → FastAPI no parseaba receptor_name (vacío) aunque el input mostrara texto.
 * Aquí el input es no controlado + confirmación explícita del nombre.
 */
export function DeliverySignaturePad(props: Props) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null)
  const nameRef = useRef<HTMLInputElement | null>(null)
  const drawing = useRef(false)
  const [confirmedName, setConfirmedName] = useState<string | null>(null)
  const [refuse, setRefuse] = useState(false)
  const [refuseReason, setRefuseReason] = useState("")
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [hasStroke, setHasStroke] = useState(false)
  const [nameLocked, setNameLocked] = useState(false)

  useEffect(() => {
    const c = canvasRef.current
    if (!c) return
    const dpr = Math.min(window.devicePixelRatio || 1, 2)
    const rect = c.getBoundingClientRect()
    c.width = Math.max(1, Math.floor(rect.width * dpr))
    c.height = Math.max(1, Math.floor(rect.height * dpr))
    const ctx = c.getContext("2d")
    if (!ctx) return
    ctx.scale(dpr, dpr)
    ctx.fillStyle = "#fff"
    ctx.fillRect(0, 0, rect.width, rect.height)
    ctx.strokeStyle = "#111"
    ctx.lineWidth = 2.2
    ctx.lineCap = "round"
    ctx.lineJoin = "round"
  }, [])

  function readDomName(): string {
    return String(nameRef.current?.value || "").trim()
  }

  function confirmReceptorName() {
    const n = readDomName()
    if (!receptorNameIsValid(n, false)) {
      setErr("Nombre del receptor requerido")
      setConfirmedName(null)
      setNameLocked(false)
      return
    }
    setConfirmedName(n)
    setNameLocked(true)
    setErr(null)
  }

  function unlockName() {
    setNameLocked(false)
    setConfirmedName(null)
    requestAnimationFrame(() => nameRef.current?.focus())
  }

  function pos(e: React.PointerEvent<HTMLCanvasElement>) {
    const c = canvasRef.current!
    const r = c.getBoundingClientRect()
    return { x: e.clientX - r.left, y: e.clientY - r.top }
  }

  function clear() {
    const c = canvasRef.current
    if (!c) return
    const ctx = c.getContext("2d")
    if (!ctx) return
    const r = c.getBoundingClientRect()
    ctx.save()
    ctx.setTransform(1, 0, 0, 1, 0, 0)
    ctx.clearRect(0, 0, c.width, c.height)
    ctx.restore()
    const dpr = Math.min(window.devicePixelRatio || 1, 2)
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0)
    ctx.fillStyle = "#fff"
    ctx.fillRect(0, 0, r.width, r.height)
    ctx.strokeStyle = "#111"
    ctx.lineWidth = 2.2
    ctx.lineCap = "round"
    ctx.lineJoin = "round"
    setHasStroke(false)
  }

  async function captureGps(): Promise<{
    lat: number
    lng: number
    accuracy: number | null
  } | null> {
    return new Promise((resolve) => {
      if (!navigator.geolocation) {
        resolve(null)
        return
      }
      navigator.geolocation.getCurrentPosition(
        (p) =>
          resolve({
            lat: p.coords.latitude,
            lng: p.coords.longitude,
            accuracy: typeof p.coords.accuracy === "number" ? p.coords.accuracy : null,
          }),
        () => resolve(null),
        { enableHighAccuracy: true, timeout: 15000, maximumAge: 0 },
      )
    })
  }

  async function submit(e?: React.FormEvent) {
    e?.preventDefault()
    if (props.blockReason) {
      setErr(props.blockReason)
      return
    }
    setBusy(true)
    setErr(null)
    try {
      // Prioridad: nombre confirmado → DOM no controlado (nunca solo React state).
      const fromDom = readDomName()
      const receptorName = (confirmedName || fromDom).trim()
      if (!refuse && !confirmedName) {
        setErr("Confirme el nombre del receptor antes de firmar")
        setBusy(false)
        return
      }
      if (!receptorNameIsValid(receptorName, refuse)) {
        setErr("Nombre del receptor requerido")
        setBusy(false)
        return
      }
      if (refuse && !refuseReason.trim()) {
        setErr("Motivo de negativa requerido")
        setBusy(false)
        return
      }
      let blob: Blob | null = null
      if (!refuse) {
        if (!hasStroke) {
          setErr("Dibuje la firma o registre negativa")
          setBusy(false)
          return
        }
        const c = canvasRef.current
        if (!c) throw new Error("Canvas no disponible")
        blob = await new Promise<Blob>((resolve, reject) => {
          c.toBlob((b) => (b ? resolve(b) : reject(new Error("Firma vacía"))), "image/jpeg", 0.92)
        })
      }
      const gps = await captureGps()
      const res = await submitVisitConformity(props.planId, props.visitId, {
        receptor_name: receptorName,
        refused: refuse,
        refuse_reason: refuse ? refuseReason : undefined,
        latitude: gps?.lat,
        longitude: gps?.lng,
        accuracy_m: gps?.accuracy,
        signatureBlob: blob,
      })
      if (!res || (res as { persisted?: boolean }).persisted === false) {
        setErr("El servidor no confirmó la conformidad")
        return
      }
      setErr(null)
      props.onDone()
    } catch (err) {
      setErr(err instanceof ApiError ? err.message : "No se pudo guardar conformidad")
    } finally {
      setBusy(false)
    }
  }

  return (
    <form className="stack-gap" onSubmit={(ev) => void submit(ev)}>
      <div className="card" style={{ padding: 12 }}>
        <strong style={{ fontSize: "0.9rem" }}>Resumen a confirmar</strong>
        <ul style={{ margin: "8px 0 0", paddingLeft: 18, fontSize: "0.85rem" }}>
          {props.summaryLines.map((l, i) => (
            <li key={`${i}-${l.slice(0, 24)}`}>{l}</li>
          ))}
        </ul>
        {props.onToggleDetail ? (
          <button
            type="button"
            className="btn btn-ghost btn-compact"
            style={{ marginTop: 8 }}
            onClick={props.onToggleDetail}
          >
            {props.detailOpen ? "Ocultar detalle" : "Ver detalle"}
          </button>
        ) : null}
      </div>

      {props.blockReason ? (
        <div className="banner banner-error">{props.blockReason}</div>
      ) : null}

      <label className="field">
        Nombre del receptor
        <input
          ref={nameRef}
          name="receptor_name"
          type="text"
          defaultValue=""
          readOnly={nameLocked && !refuse}
          autoComplete="name"
          autoCorrect="off"
          spellCheck={false}
          enterKeyHint="done"
          inputMode="text"
        />
      </label>
      {!refuse ? (
        <div className="btn-row">
          {!nameLocked ? (
            <button
              type="button"
              className="btn btn-secondary"
              onClick={confirmReceptorName}
              disabled={busy}
            >
              Confirmar nombre del receptor
            </button>
          ) : (
            <>
              <span className="pill pill-ok">
                Nombre confirmado ({confirmedName?.length || 0} caracteres)
              </span>
              <button
                type="button"
                className="btn btn-ghost btn-compact"
                onClick={unlockName}
                disabled={busy}
              >
                Corregir nombre
              </button>
            </>
          )}
        </div>
      ) : null}

      <label className="field" style={{ flexDirection: "row", alignItems: "center", gap: 8 }}>
        <input
          type="checkbox"
          checked={refuse}
          onChange={(e) => setRefuse(e.target.checked)}
        />
        <span style={{ color: "var(--text)" }}>Receptor no desea firmar</span>
      </label>

      {refuse ? (
        <label className="field">
          Motivo de la negativa
          <textarea
            rows={2}
            value={refuseReason}
            onChange={(e) => setRefuseReason(e.target.value)}
          />
        </label>
      ) : (
        <>
          <div
            className="sig-wrap"
            style={{
              border: "1px solid var(--border)",
              borderRadius: 12,
              background: "#fff",
              touchAction: "none",
            }}
          >
            <canvas
              ref={canvasRef}
              style={{ width: "100%", height: 160, display: "block", borderRadius: 12 }}
              onPointerDown={(e) => {
                const c = canvasRef.current
                const ctx = c?.getContext("2d")
                if (!c || !ctx) return
                c.setPointerCapture(e.pointerId)
                drawing.current = true
                const p = pos(e)
                ctx.beginPath()
                ctx.moveTo(p.x, p.y)
              }}
              onPointerMove={(e) => {
                if (!drawing.current) return
                const ctx = canvasRef.current?.getContext("2d")
                if (!ctx) return
                const p = pos(e)
                ctx.lineTo(p.x, p.y)
                ctx.stroke()
                setHasStroke(true)
              }}
              onPointerUp={() => {
                drawing.current = false
              }}
              onPointerCancel={() => {
                drawing.current = false
              }}
            />
          </div>
          <button type="button" className="btn btn-ghost" onClick={clear} disabled={busy}>
            Borrar firma
          </button>
        </>
      )}

      <button
        type="submit"
        className="btn btn-primary"
        disabled={busy || Boolean(props.blockReason)}
      >
        {busy ? "Guardando…" : refuse ? "Registrar negativa" : "Confirmar conformidad"}
      </button>
      {err ? <div className="banner banner-error">{err}</div> : null}
    </form>
  )
}
