import { QUILLOTANA_LOGO_GRUPO_URL } from "@/lib/quillotana-brand"

export type PdfLogoPayload = {
  dataUrl: string
  format: "PNG" | "JPEG" | "WEBP"
  widthPx: number
  heightPx: number
  bytes: number
}

let logoCache: PdfLogoPayload | null | undefined

function detectFormat(dataUrl: string): "PNG" | "JPEG" | "WEBP" {
  if (dataUrl.startsWith("data:image/jpeg") || dataUrl.startsWith("data:image/jpg")) {
    return "JPEG"
  }
  if (dataUrl.startsWith("data:image/webp")) return "WEBP"
  return "PNG"
}

function logLogo(message: string, extra?: Record<string, unknown>) {
  const parts = ["[PICKING_PDF_LOGO]", message]
  if (extra) {
    for (const [k, v] of Object.entries(extra)) {
      parts.push(`${k}=${String(v)}`)
    }
  }
  console.info(parts.join(" "))
}

/**
 * Carga robusta del logo para jsPDF vía Image + canvas (evita fallos de fetch/CORS silenciosos).
 */
export async function loadQuillotanaLogoForPdf(): Promise<PdfLogoPayload | null> {
  if (logoCache !== undefined) {
    logLogo("cache_hit", { ok: logoCache != null })
    return logoCache
  }

  const url = QUILLOTANA_LOGO_GRUPO_URL
  logLogo("loading", { url })

  if (typeof window === "undefined" || typeof document === "undefined") {
    logLogo("render_failed", { reason: "no_window" })
    logoCache = null
    return null
  }

  try {
    const payload = await new Promise<PdfLogoPayload>((resolve, reject) => {
      const img = new Image()
      img.crossOrigin = "anonymous"
      img.onload = () => {
        try {
          const canvas = document.createElement("canvas")
          canvas.width = img.naturalWidth
          canvas.height = img.naturalHeight
          const ctx = canvas.getContext("2d")
          if (!ctx) {
            reject(new Error("canvas_context_unavailable"))
            return
          }
          ctx.drawImage(img, 0, 0)
          const dataUrl = canvas.toDataURL("image/png")
          const bytes = Math.round((dataUrl.length * 3) / 4)
          resolve({
            dataUrl,
            format: detectFormat(dataUrl),
            widthPx: img.naturalWidth,
            heightPx: img.naturalHeight,
            bytes,
          })
        } catch (err) {
          reject(err)
        }
      }
      img.onerror = () => reject(new Error("image_load_failed"))
      img.src = url
    })

    logoCache = payload
    logLogo("render_ok", {
      url,
      width_px: payload.widthPx,
      height_px: payload.heightPx,
      bytes: payload.bytes,
      format: payload.format,
    })
    return payload
  } catch (err) {
    logLogo("render_failed", {
      url,
      error: err instanceof Error ? err.message : String(err),
    })
    logoCache = null
    return null
  }
}
