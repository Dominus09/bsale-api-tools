import { QUILLOTANA_LOGO_GRUPO_URL } from "@/lib/quillotana-brand"

/** Ancho máximo en px antes de incrustar (evita PNG 4267×1215 → PDF de 20 MB). */
const PDF_LOGO_MAX_WIDTH_PX = 480
const PDF_LOGO_JPEG_QUALITY = 0.78
export const PDF_LOGO_WIDTH_MM = 40

export type PdfLogoPayload = {
  dataUrl: string
  format: "JPEG"
  widthPx: number
  heightPx: number
  bytes: number
  aspectRatio: number
}

let logoCache: PdfLogoPayload | null | undefined

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
 * Carga logo optimizado para jsPDF: redimensionado + JPEG comprimido + caché en memoria.
 */
export async function loadQuillotanaLogoForPdf(): Promise<PdfLogoPayload | null> {
  if (logoCache !== undefined) {
    logLogo("cache_hit", { ok: logoCache != null, bytes: logoCache?.bytes ?? 0 })
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
          const scale = Math.min(1, PDF_LOGO_MAX_WIDTH_PX / img.naturalWidth)
          const widthPx = Math.max(1, Math.round(img.naturalWidth * scale))
          const heightPx = Math.max(1, Math.round(img.naturalHeight * scale))
          const canvas = document.createElement("canvas")
          canvas.width = widthPx
          canvas.height = heightPx
          const ctx = canvas.getContext("2d")
          if (!ctx) {
            reject(new Error("canvas_context_unavailable"))
            return
          }
          ctx.fillStyle = "#ffffff"
          ctx.fillRect(0, 0, widthPx, heightPx)
          ctx.drawImage(img, 0, 0, widthPx, heightPx)
          const dataUrl = canvas.toDataURL("image/jpeg", PDF_LOGO_JPEG_QUALITY)
          const bytes = Math.round((dataUrl.length * 3) / 4)
          resolve({
            dataUrl,
            format: "JPEG",
            widthPx,
            heightPx,
            bytes,
            aspectRatio: widthPx / heightPx,
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

const LOGO_ALIAS = "quillotana-logo-pdf"

export type PdfLogoDocState = { registered: boolean }

export function createPdfLogoDocState(): PdfLogoDocState {
  return { registered: false }
}

/** Registra el logo una sola vez por documento PDF (evita duplicar JPEG en cada página). */
export function drawQuillotanaLogoOnPdf(
  doc: import("jspdf").jsPDF,
  logo: PdfLogoPayload,
  state: PdfLogoDocState,
  x: number,
  y: number,
  widthMm: number = PDF_LOGO_WIDTH_MM,
): number {
  const heightMm = widthMm / logo.aspectRatio
  if (!state.registered) {
    doc.addImage(
      logo.dataUrl,
      logo.format,
      x,
      y,
      widthMm,
      heightMm,
      LOGO_ALIAS,
      "FAST",
    )
    state.registered = true
  } else {
    doc.addImage(LOGO_ALIAS, logo.format, x, y, widthMm, heightMm)
  }
  return heightMm
}
