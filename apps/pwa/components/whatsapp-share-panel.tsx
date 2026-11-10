"use client"

import { useState } from "react"
import {
  copyReportAndOpenFamily,
  copyTextSafe,
  FAMILY_WHATSAPP_INVITE_URL,
  openFamilyGroup,
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
  demo?: boolean
  busy?: boolean
  onGenerate: () => Promise<void>
  generateLabel?: string
}

type SharePhase = "idle" | "generated" | "share_requested" | "cancelled" | "unavailable"

export function WhatsappSharePanel(props: Props) {
  const [phase, setPhase] = useState<SharePhase>(props.reportText ? "generated" : "idle")
  const [copied, setCopied] = useState(false)
  const [familyMsg, setFamilyMsg] = useState<string | null>(null)
  const [localErr, setLocalErr] = useState<string | null>(null)
  const [showManual, setShowManual] = useState(false)

  async function copyText() {
    if (!props.reportText) return
    const ok = await copyTextSafe(props.reportText)
    if (ok) {
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
      setLocalErr(null)
    } else {
      setLocalErr("No se pudo copiar. Seleccione el texto manualmente.")
      setShowManual(true)
    }
  }

  async function copyAndOpenFamily() {
    if (!props.reportText) return
    setLocalErr(null)
    setFamilyMsg(null)
    const res = await copyReportAndOpenFamily(props.reportText)
    if (res.copied) {
      setCopied(true)
      setFamilyMsg("Reporte copiado. Pegue en el grupo familiar y envíe.")
      setTimeout(() => setCopied(false), 2500)
    } else {
      setShowManual(true)
      setLocalErr("No se pudo copiar automáticamente. Use Copiar reporte y luego Abrir grupo.")
    }
    if (!res.opened) {
      setShowManual(true)
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
        return
      } catch (e) {
        const name = e instanceof DOMException ? e.name : ""
        if (name === "AbortError") {
          setPhase("cancelled")
          return
        }
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

          {props.demo ? (
            <div className="stack-gap">
              <button
                type="button"
                className="btn btn-primary"
                disabled={props.busy}
                onClick={() => void copyAndOpenFamily()}
              >
                Copiar reporte y abrir grupo familiar
              </button>
              {showManual ? (
                <div className="btn-row">
                  <button
                    type="button"
                    className="btn btn-secondary"
                    onClick={() => void copyText()}
                  >
                    {copied ? "Copiado" : "Copiar reporte"}
                  </button>
                  <button
                    type="button"
                    className="btn btn-secondary"
                    onClick={() => openFamilyGroup()}
                  >
                    Abrir grupo familiar
                  </button>
                </div>
              ) : null}
              <p className="muted" style={{ margin: 0, fontSize: "0.75rem" }}>
                No se envía automáticamente. Copie, abra el grupo, pegue y envíe.
                Enlace demo: {FAMILY_WHATSAPP_INVITE_URL.split("?")[0]}
              </p>
            </div>
          ) : null}

          <div className="btn-row">
            <button
              type="button"
              className="btn btn-primary"
              disabled={props.busy}
              onClick={() => void shareNativeOrFallback()}
            >
              Compartir por WhatsApp
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

          {familyMsg ? <div className="banner banner-info">{familyMsg}</div> : null}

          <p className="muted" style={{ margin: 0, fontSize: "0.78rem" }}>
            {props.share?.note_es ||
              "Seleccione el grupo en WhatsApp y confirme el envío. Abrir el menú no acredita entrega."}
          </p>
        </>
      )}

      {localErr ? <div className="banner banner-error">{localErr}</div> : null}
    </div>
  )
}
