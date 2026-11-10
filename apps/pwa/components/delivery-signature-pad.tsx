"use client"

import { useEffect, useRef, useState } from "react"
import { ApiError, submitVisitConformity } from "@/lib/api"

type Props = {
  planId: number
  visitId: number
  summaryLines: string[]
  onDone: () => void
}

export function DeliverySignaturePad(props: Props) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null)
  const drawing = useRef(false)
  const [name, setName] = useState("")
  const [refuse, setRefuse] = useState(false)
  const [refuseReason, setRefuseReason] = useState("")
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [hasStroke, setHasStroke] = useState(false)

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

  async function submit() {
    setBusy(true)
    setErr(null)
    try {
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
      await submitVisitConformity(props.planId, props.visitId, {
        receptor_name: name,
        refused: refuse,
        refuse_reason: refuse ? refuseReason : undefined,
        latitude: gps?.lat,
        longitude: gps?.lng,
        accuracy_m: gps?.accuracy,
        signatureBlob: blob,
      })
      props.onDone()
    } catch (e) {
      setErr(e instanceof ApiError ? e.message : "No se pudo guardar conformidad")
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="stack-gap">
      <div className="card" style={{ padding: 12 }}>
        <strong style={{ fontSize: "0.9rem" }}>Resumen a confirmar</strong>
        <ul style={{ margin: "8px 0 0", paddingLeft: 18, fontSize: "0.85rem" }}>
          {props.summaryLines.map((l) => (
            <li key={l}>{l}</li>
          ))}
        </ul>
      </div>

      <label className="field">
        Nombre del receptor
        <input value={name} onChange={(e) => setName(e.target.value)} autoComplete="name" />
      </label>

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

      <button type="button" className="btn btn-primary" disabled={busy} onClick={() => void submit()}>
        {busy ? "Guardando…" : refuse ? "Registrar negativa" : "Confirmar conformidad"}
      </button>
      {err ? <div className="banner banner-error">{err}</div> : null}
    </div>
  )
}
