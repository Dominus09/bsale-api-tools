import type { DistribuidoraResumenDiaJson, DistribuidoraResumenVendedorJson } from "@/lib/api"

const MM_MARGIN = 16
const PAGE_W_MM = 210
const PAGE_H_MM = 297
const CONTENT_W = PAGE_W_MM - MM_MARGIN * 2

const TEXTO_METODOLOGIA =
  "Las rutas fueron optimizadas considerando distancia entre clientes, punto de partida y retorno al origen, buscando minimizar los kilómetros recorridos y el tiempo de traslado."

function formatClp(n: number): string {
  return Math.round(n).toLocaleString("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  })
}

function nombreCliente(c: Record<string, unknown>): string {
  const raw =
    (c.cliente_nombre as string) ||
    (c.nombre as string) ||
    (c.nombre_fantasia as string) ||
    ""
  const t = String(raw).trim()
  return t || "Cliente"
}

function clientesOrdenados(dia: DistribuidoraResumenDiaJson): string[] {
  const raw = dia.clientes
  if (!Array.isArray(raw)) return []
  const rows = raw as Record<string, unknown>[]
  const withIdx = rows.map((c, i) => ({
    c,
    ov: Number(c.orden_visita) || i + 1,
  }))
  withIdx.sort((a, b) => a.ov - b.ov)
  return withIdx.map(({ c }) => nombreCliente(c))
}

async function loadImageDataUrl(path: string): Promise<string | null> {
  try {
    const res = await fetch(path)
    if (!res.ok) return null
    const blob = await res.blob()
    return await new Promise((resolve, reject) => {
      const fr = new FileReader()
      fr.onload = () => resolve(fr.result as string)
      fr.onerror = () => reject(new Error("FileReader"))
      fr.readAsDataURL(blob)
    })
  } catch {
    return null
  }
}

export type ExportResumenVendedorPdfParams = {
  resumen: DistribuidoraResumenVendedorJson
  mapElement: HTMLElement
  viaticoClp: number | null
  /** Ruta pública bajo / (ej. /placeholder-logo.png) */
  logoPublicPath?: string
}

/**
 * Genera y descarga un PDF del resumen semanal (mapa + métricas + clientes por día).
 */
export async function exportResumenVendedorPdf(
  params: ExportResumenVendedorPdfParams,
): Promise<void> {
  const [{ jsPDF }, html2canvasMod] = await Promise.all([
    import("jspdf"),
    import("html2canvas"),
  ])
  const html2canvas = html2canvasMod.default
  const { resumen, mapElement, viaticoClp } = params
  const logoPath = params.logoPublicPath ?? "/placeholder-logo.png"

  const doc = new jsPDF({
    orientation: "portrait",
    unit: "mm",
    format: "a4",
    compress: true,
  })
  doc.setFont("helvetica")

  let y = MM_MARGIN

  const ensureSpace = (neededMm: number) => {
    if (y + neededMm > PAGE_H_MM - MM_MARGIN) {
      doc.addPage()
      y = MM_MARGIN
    }
  }

  const addParagraph = (text: string, fontSize: number, lineMm: number) => {
    doc.setFontSize(fontSize)
    const lines = doc.splitTextToSize(text, CONTENT_W)
    const blockH = lines.length * lineMm
    ensureSpace(blockH + 2)
    doc.text(lines, MM_MARGIN, y)
    y += blockH + 4
  }

  const addHeading = (text: string, size: number) => {
    ensureSpace(12)
    doc.setFont("helvetica", "bold")
    doc.setFontSize(size)
    doc.text(text, MM_MARGIN, y)
    doc.setFont("helvetica", "normal")
    y += size * 0.45 + 4
  }

  // Logo
  const logoData = await loadImageDataUrl(logoPath)
  if (logoData) {
    try {
      const logoW = 42
      const logoH = 14
      ensureSpace(logoH + 4)
      doc.addImage(logoData, "PNG", MM_MARGIN, y, logoW, logoH)
      y += logoH + 6
    } catch {
      /* sin logo */
    }
  }

  addHeading(`Ruta semanal vendedor ${resumen.vendedor}`, 16)

  addHeading("1. Resumen general", 12)
  doc.setFontSize(10)
  const viaticoLine =
    viaticoClp != null && Number.isFinite(viaticoClp)
      ? formatClp(viaticoClp)
      : "No calculado (defina rendimiento y precio de combustible en pantalla)."
  const resumenLines = [
    `Km total semana: ${resumen.km_total_semana} km`,
    `Clientes (visitas semana): ${resumen.clientes_total_semana}`,
    `Tiempo estimado (conducción): ${resumen.min_total_semana} min`,
    `Viático estimado: ${viaticoLine}`,
  ]
  for (const line of resumenLines) {
    addParagraph(line, 10, 4.5)
  }

  // Mapa
  addHeading("2. Mapa semanal", 12)
  mapElement.scrollIntoView({ block: "nearest", behavior: "instant" })
  await new Promise((r) => setTimeout(r, 350))

  const canvas = await html2canvas(mapElement, {
    useCORS: true,
    allowTaint: true,
    scale: Math.min(2, Math.max(1, typeof window !== "undefined" ? window.devicePixelRatio : 1.5)),
    logging: false,
    backgroundColor: "#dfe6ee",
    windowWidth: mapElement.scrollWidth,
    windowHeight: mapElement.scrollHeight,
  })

  const imgData = canvas.toDataURL("image/jpeg", 0.88)
  const imgW = CONTENT_W
  const imgH = (canvas.height / canvas.width) * imgW
  const maxMapH = 115
  const finalH = Math.min(imgH, maxMapH)
  const finalW = (canvas.width / canvas.height) * finalH
  ensureSpace(finalH + 6)
  doc.addImage(imgData, "JPEG", MM_MARGIN, y, Math.min(finalW, CONTENT_W), finalH)
  y += finalH + 8

  // Detalle por día
  addHeading("3. Detalle por día", 12)
  for (const d of resumen.dias) {
    const nombres = clientesOrdenados(d)
    addHeading(`${d.dia}:`, 11)
    if (nombres.length === 0) {
      addParagraph("Sin clientes en ruta para este día.", 10, 4.5)
    } else {
      for (let i = 0; i < nombres.length; i++) {
        addParagraph(`• ${nombres[i]}`, 10, 4.5)
      }
    }
  }

  addHeading("4. Explicación simple", 12)
  addParagraph(TEXTO_METODOLOGIA, 10, 4.8)

  const safe = resumen.vendedor.replace(/[^\w.\-]+/g, "_").slice(0, 72) || "vendedor"
  const fecha = new Date().toISOString().slice(0, 10)
  doc.save(`ruta-semanal-${safe}-${fecha}.pdf`)
}
