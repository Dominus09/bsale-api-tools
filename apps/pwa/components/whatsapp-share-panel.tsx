"use client"

import { useState } from "react"
import {
  OPERATIONAL_WHATSAPP_INVITE_URL,
  openWhatsappReport,
} from "@/lib/family-whatsapp"

type ShareMeta = {
  whatsapp_url?: string
  delivery_claimed?: boolean
  note_es?: string
}

type Props = {
  reportText: string | null
  share?: ShareMeta | null
  header?: string | null
  /** Solo modo demostración familiar: destino = grupo de pruebas. */
  demo?: boolean
  busy?: boolean
  onGenerate: () => Promise<void>
  generateLabel?: string
}

type SharePhase = "idle" | "generated" | "opened" | "unavailable"

export function WhatsappSharePanel(props: Props) {
  const [phase, setPhase] = useState<SharePhase>(props.reportText ? "generated" : "idle")
  const [localErr, setLocalErr] = useState<string | null>(null)

  function sendReportByWhatsapp() {
    if (!props.reportText) return
    setLocalErr(null)
    try {
      openWhatsappReport(props.reportText, props.demo ? "demo" : "real")
      setPhase("opened")
    } catch {
      setPhase("unavailable")
      setLocalErr(
        "No se pudo abrir WhatsApp. Genere el reporte de nuevo o copie el texto manualmente.",
      )
    }
  }

  const hint = props.demo
    ? "Abre el grupo familiar de prueba con el mensaje precargado. Debe confirmar Enviar en WhatsApp. La app no envía sola."
    : OPERATIONAL_WHATSAPP_INVITE_URL
      ? "Abre el grupo operativo con el mensaje precargado. Debe confirmar Enviar en WhatsApp. La app no envía sola."
      : "Abre WhatsApp con el texto precargado para que elija el chat. Debe confirmar Enviar. La app no envía sola."

  return (
    <div className="stack-gap">
      {props.demo ? (
        <div className="banner banner-info" style={{ fontSize: "0.82rem" }}>
          🧪 Modo prueba — datos ficticios. No es una entrega real. Máximo 6 líneas.
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
              {phase === "opened"
                ? "WhatsApp abierto (envío no verificable)"
                : phase === "unavailable"
                  ? "WhatsApp no disponible"
                  : "Listo para compartir"}
            </span>
          </div>

          <pre className="report-pre">{props.reportText}</pre>

          <button
            type="button"
            className="btn btn-whatsapp"
            disabled={props.busy}
            onClick={() => sendReportByWhatsapp()}
          >
            Enviar reporte por WhatsApp
          </button>

          <p className="muted" style={{ margin: 0, fontSize: "0.78rem" }}>
            {props.share?.note_es || hint}
          </p>
        </>
      )}

      {localErr ? <div className="banner banner-error">{localErr}</div> : null}
    </div>
  )
}
