"use client"

import { useState } from "react"

type ShareMeta = {
  whatsapp_url?: string
  delivery_claimed?: boolean
  note_es?: string
}

type Props = {
  reportText: string | null
  share?: ShareMeta | null
  header?: string | null
  demo?: boolean
  busy?: boolean
  onGenerate: () => Promise<void>
  generateLabel?: string
}

type SharePhase = "idle" | "generated" | "share_requested" | "cancelled" | "unavailable"

export function WhatsappSharePanel(props: Props) {
  const [phase, setPhase] = useState<SharePhase>(props.reportText ? "generated" : "idle")
  const [copied, setCopied] = useState(false)
  const [localErr, setLocalErr] = useState<string | null>(null)

  async function copyText() {
    if (!props.reportText) return
    try {
      await navigator.clipboard.writeText(props.reportText)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch {
      setLocalErr("No se pudo copiar. Seleccione el texto manualmente.")
    }
  }

  async function shareNativeOrFallback() {
    if (!props.reportText) return
    setLocalErr(null)
    setPhase("share_requested")

    const canNative =
      typeof navigator !== "undefined" &&
      typeof navigator.share === "function" &&
      (!navigator.canShare || navigator.canShare({ text: props.reportText }))

    if (canNative) {
      try {
        await navigator.share({
          title: props.header || "Reporte Quillotana",
          text: props.reportText,
        })
        // No afirmamos entrega: el usuario pudo cancelar tras abrir el sheet en algunos OS.
        return
      } catch (e) {
        const name = e instanceof DOMException ? e.name : ""
        if (name === "AbortError") {
          setPhase("cancelled")
          return
        }
        // Continúa a fallback
      }
    }

    const url = props.share?.whatsapp_url
    if (url) {
      window.open(url, "_blank", "noopener,noreferrer")
      return
    }
    setPhase("unavailable")
    setLocalErr(
      "WhatsApp / compartir no disponible. Copie el texto y péguelo manualmente.",
    )
  }

  return (
    <div className="stack-gap">
      {props.demo ? (
        <div className="banner banner-info" style={{ fontSize: "0.82rem" }}>
          🧪 Modo prueba — datos ficticios. No es una entrega real.
        </div>
      ) : null}

      {!props.reportText ? (
        <button
          type="button"
          className="btn btn-secondary"
          disabled={props.busy}
          onClick={() => {
            void (async () => {
              setLocalErr(null)
              await props.onGenerate()
              setPhase("generated")
            })()
          }}
        >
          {props.generateLabel || "Generar reporte"}
        </button>
      ) : (
        <>
          <div className="ficha-summary">
            <span className="pill pill-ok">Reporte generado</span>
            <span className="pill">
              {phase === "share_requested"
                ? "Compartir solicitado (envío no verificable)"
                : phase === "cancelled"
                  ? "Compartir cancelado"
                  : phase === "unavailable"
                    ? "Compartir no disponible"
                    : "Listo para compartir"}
            </span>
          </div>

          <pre className="report-pre">{props.reportText}</pre>

          <div className="btn-row">
            <button
              type="button"
              className="btn btn-primary"
              disabled={props.busy}
              onClick={() => void shareNativeOrFallback()}
            >
              Compartir reporte por WhatsApp
            </button>
            <button
              type="button"
              className="btn btn-secondary"
              disabled={props.busy}
              onClick={() => void copyText()}
            >
              {copied ? "Copiado" : "Copiar mensaje"}
            </button>
          </div>

          <p className="muted" style={{ margin: 0, fontSize: "0.78rem" }}>
            {props.share?.note_es ||
              "Seleccione el grupo en WhatsApp y confirme el envío. Abrir el menú no acredita entrega."}
          </p>
          <p className="muted" style={{ margin: 0, fontSize: "0.75rem" }}>
            Estados: generado
            {phase === "share_requested" || phase === "cancelled" || phase === "unavailable"
              ? " · compartir solicitado"
              : ""}{" "}
            · envío no verificable por la app
          </p>
        </>
      )}

      {localErr ? <div className="banner banner-error">{localErr}</div> : null}
    </div>
  )
}
