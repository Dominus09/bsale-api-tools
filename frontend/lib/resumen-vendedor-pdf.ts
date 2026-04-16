import { format } from "date-fns"

import type { DistribuidoraResumenVendedorJson } from "@/lib/api"
import { buildOperationalInsights } from "@/lib/resumen-vendedor-analisis"
import type { PdfClienteColumn } from "@/lib/resumen-vendedor-pdf-clientes-layout"
import { buildPdfWeeklyRouteMapDataUrl } from "@/lib/resumen-vendedor-pdf-route-canvas"
import {
  buildClienteColumnsJueVie,
  buildClienteColumnsLunMie,
} from "@/lib/resumen-vendedor-pdf-clientes-layout"

const PAGE_W_MM = 210
const PAGE_H_MM = 297
const MARGIN_MM = 12
const INNER_W = PAGE_W_MM - MARGIN_MM * 2
const SECTION_PAD_MM = 4
const SECTION_HEADER_H_MM = 7.5
/** Fondo de bloques (#f3f4f6) */
const SECTION_BG: [number, number, number] = [243, 244, 246]
const BORDER_LIGHT: [number, number, number] = [229, 231, 235]
const CONTENT_PAD_MM = 3

const BRAND_R = 196
const BRAND_G = 41
const BRAND_B = 60

const TEXT_DARK: [number, number, number] = [15, 23, 42]
const TEXT_MUTED: [number, number, number] = [71, 85, 105]

export type ResumenPdfMapBlock = {
  /** Ej.: "Lunes y Martes" */
  title: string
  dataUrl: string
}

export type ExportResumenVendedorPdfParams = {
  resumen: DistribuidoraResumenVendedorJson
  /** Capturas del mapa (típicamente una: semana completa). */
  mapBlocks: ResumenPdfMapBlock[]
  viaticoClp: number | null
  rendimientoKmL: number | null
  precioCombustibleClp: number | null
}

function fmtMetric(v: unknown): string {
  const n = Number(v)
  return Number.isFinite(n) ? String(n) : "-"
}

function formatClp(n: number): string {
  return Math.round(n).toLocaleString("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  })
}

function slugFilePart(s: string): string {
  return (
    String(s || "vendedor")
      .trim()
      .toLowerCase()
      .replace(/\s+/g, "_")
      .replace(/[^a-z0-9._-]/g, "")
      .slice(0, 48) || "vendedor"
  )
}

/**
 * Título de sección: solo texto plano (sin símbolos fuera de Latin-1 del PDF).
 */
function drawSectionHeader(doc: import("jspdf").jsPDF, y: number, title: string): number {
  y = ensureSpace(doc, y, SECTION_HEADER_H_MM + SECTION_PAD_MM + 2)
  doc.setFillColor(...SECTION_BG)
  doc.rect(MARGIN_MM, y, INNER_W, SECTION_HEADER_H_MM, "F")
  doc.setDrawColor(...BORDER_LIGHT)
  doc.rect(MARGIN_MM, y, INNER_W, SECTION_HEADER_H_MM, "S")
  doc.setTextColor(...TEXT_DARK)
  doc.setFontSize(10.5)
  doc.setFont("helvetica", "bold")
  doc.text(title, MARGIN_MM + SECTION_PAD_MM, y + SECTION_HEADER_H_MM - 2.2)
  doc.setFont("helvetica", "normal")
  return y + SECTION_HEADER_H_MM + SECTION_PAD_MM
}

function addParagraph(
  doc: import("jspdf").jsPDF,
  text: string,
  x: number,
  y: number,
  maxW: number,
  fontSize: number,
  lineMm: number,
  color: [number, number, number] = TEXT_MUTED,
): number {
  doc.setFontSize(fontSize)
  doc.setTextColor(...color)
  const lines = doc.splitTextToSize(text, maxW)
  doc.text(lines, x, y)
  return y + lines.length * lineMm
}

function ensureSpace(doc: import("jspdf").jsPDF, y: number, needMm: number): number {
  if (y + needMm > PAGE_H_MM - MARGIN_MM) {
    doc.addPage()
    return MARGIN_MM
  }
  return y
}

/** Captura JPEG del contenedor del mapa (html2canvas-pro). Exportado para armar varios mapas en PDF. */
export async function captureMapElementJpeg(mapElement: HTMLElement): Promise<string> {
  const html2canvasMod = await import("html2canvas-pro")
  const html2canvas = html2canvasMod.default
  mapElement.scrollIntoView({ block: "nearest", behavior: "instant" })
  await new Promise((r) => setTimeout(r, 220))
  const scale = Math.max(2, Math.min(2.5, typeof window !== "undefined" ? window.devicePixelRatio || 2 : 2))
  const canvas = await html2canvas(mapElement, {
    useCORS: true,
    allowTaint: true,
    scale,
    logging: false,
    backgroundColor: "#f3f4f6",
    windowWidth: mapElement.scrollWidth,
    windowHeight: mapElement.scrollHeight,
    onclone: (_doc, el) => {
      el.style.setProperty("background-color", "#f3f4f6", "important")
    },
  })
  return canvas.toDataURL("image/jpeg", 0.92)
}

/**
 * Mapa en el rectángulo del PDF: imagen escalada al **ancho y alto del recuadro**
 * (misma relación de aspecto que el raster generado → sin bandas vacías).
 */
async function drawWeeklyMapInBox(
  doc: import("jspdf").jsPDF,
  boxX: number,
  boxY: number,
  boxW: number,
  boxH: number,
  dataUrl: string,
): Promise<void> {
  const img = new Image()
  await new Promise<void>((resolve, reject) => {
    img.onload = () => resolve()
    img.onerror = () => reject(new Error("No se pudo leer la imagen del mapa"))
    img.src = dataUrl
  })
  doc.addImage(dataUrl, "JPEG", boxX, boxY, boxW, boxH)
  doc.setDrawColor(...BORDER_LIGHT)
  doc.setLineWidth(0.3)
  doc.rect(boxX - 0.25, boxY - 0.25, boxW + 0.5, boxH + 0.5, "S")
}

function truncateLineToWidth(
  doc: import("jspdf").jsPDF,
  text: string,
  maxW: number,
  fontSize: number,
): string {
  doc.setFontSize(fontSize)
  if (doc.getTextWidth(text) <= maxW) return text
  const ell = "…"
  let lo = 0
  let hi = text.length
  while (lo < hi) {
    const mid = Math.ceil((lo + hi) / 2)
    const s = text.slice(0, mid) + ell
    if (doc.getTextWidth(s) <= maxW) lo = mid
    else hi = mid - 1
  }
  return text.slice(0, lo) + ell
}

/**
 * Tabla compacta: varias columnas (días), listas numeradas alineadas arriba.
 * Devuelve la posición Y final aproximada.
 */
function drawClienteDayColumns(
  doc: import("jspdf").jsPDF,
  yStart: number,
  sectionTitle: string,
  columns: PdfClienteColumn[],
): number {
  let y = drawSectionHeader(doc, yStart, sectionTitle)
  y += CONTENT_PAD_MM + 1

  const n = columns.length
  const gap = 4
  const colW = (INNER_W - gap * (n - 1)) / n
  const x0 = MARGIN_MM
  const headerH = 5.5
  const lineH = 3.85
  const bodyFont = 8.2
  const headFont = 9

  doc.setFont("helvetica", "bold")
  doc.setFontSize(headFont)
  doc.setTextColor(...TEXT_DARK)
  for (let c = 0; c < n; c++) {
    const x = x0 + c * (colW + gap)
    const tit = truncateLineToWidth(doc, columns[c].titulo, colW - 1, headFont)
    doc.text(tit, x, y + 4)
  }
  y += headerH + 2

  doc.setFont("helvetica", "normal")
  doc.setFontSize(bodyFont)
  doc.setTextColor(...TEXT_DARK)

  const maxLines = Math.max(0, ...columns.map((co) => co.lineas.length))
  for (let i = 0; i < maxLines; i++) {
    y = ensureSpace(doc, y, lineH + 1)
    for (let c = 0; c < n; c++) {
      const line = columns[c].lineas[i]
      if (!line) continue
      const x = x0 + c * (colW + gap)
      const t = truncateLineToWidth(doc, line, colW - 0.5, bodyFont)
      doc.text(t, x, y + 3.2)
    }
    y += lineH
  }

  return y + 6
}

/**
 * Genera y descarga el informe PDF (mapa real vía html2canvas-pro, texto operativo dinámico).
 * Estructura: p.1 título + resumen + mapa casi hoja completa; p.2–3 clientes por columnas;
 * última(s) página(s) análisis operativo.
 */
export async function exportResumenVendedorPdf(params: ExportResumenVendedorPdfParams): Promise<void> {
  const { jsPDF } = await import("jspdf")
  const { resumen, mapBlocks, viaticoClp, rendimientoKmL, precioCombustibleClp } = params

  const doc = new jsPDF({
    orientation: "portrait",
    unit: "mm",
    format: "a4",
    compress: true,
  })

  const vNombre = String(resumen.vendedor ?? "").trim() || "Vendedor"
  const km = Number(resumen.km_total_semana) || 0

  let y = MARGIN_MM

  doc.setFillColor(BRAND_R, BRAND_G, BRAND_B)
  doc.rect(0, 0, PAGE_W_MM, 18, "F")
  doc.setTextColor(255, 255, 255)
  doc.setFontSize(13)
  doc.setFont("helvetica", "bold")
  doc.text(`Ruta — ${vNombre} — Semana`, MARGIN_MM, 11)
  doc.setFont("helvetica", "normal")
  doc.setFontSize(8)
  doc.text(`Generado: ${format(new Date(), "yyyy-MM-dd HH:mm")}`, MARGIN_MM, 15.5)
  doc.setTextColor(...TEXT_DARK)

  y = 22

  doc.setFont("helvetica", "bold")
  doc.setFontSize(9.5)
  doc.setTextColor(...TEXT_DARK)
  doc.text("Resumen", MARGIN_MM, y)
  y += 5
  doc.setFont("helvetica", "normal")
  doc.setFontSize(9.2)

  const lineViatico = (() => {
    if (
      rendimientoKmL != null &&
      precioCombustibleClp != null &&
      rendimientoKmL > 0 &&
      precioCombustibleClp > 0
    ) {
      const litros = km / rendimientoKmL
      const total = litros * precioCombustibleClp
      const totalClp =
        viaticoClp != null && Number.isFinite(viaticoClp) ? Math.round(viaticoClp) : Math.round(total)
      return `Viático estimado: ${formatClp(totalClp)} (${rendimientoKmL} km/l, ${formatClp(precioCombustibleClp)}/l, ${litros.toFixed(2)} l)`
    }
    return "Viático: ingrese rendimiento (km/l) y precio combustible en el panel para ver monto."
  })()

  const resumenLineas = [
    `Km totales semana: ${fmtMetric(resumen.km_total_semana)} km`,
    `Clientes (visitas): ${fmtMetric(resumen.clientes_total_semana)}`,
    `Tiempo conducción (estim.): ${fmtMetric(resumen.min_total_semana)} min`,
    lineViatico,
  ]
  doc.setFontSize(9.2)
  for (const ln of resumenLineas) {
    const wrapped = doc.splitTextToSize(ln, INNER_W)
    for (const wline of wrapped) {
      y = ensureSpace(doc, y, 5.5)
      doc.text(wline, MARGIN_MM, y)
      y += 4.5
    }
  }
  y += 3

  const mapBottom = PAGE_H_MM - MARGIN_MM - 2
  const mapTop = y + 2
  /** Usa todo el espacio disponible bajo el resumen (típicamente ~80 % de la hoja si el resumen es breve). */
  const mapH = Math.max(60, mapBottom - mapTop)

  const mapAspect = INNER_W / mapH
  const rasterW = 2400
  const rasterH = Math.max(900, Math.round(rasterW / mapAspect))

  const mapFromGeometry = await buildPdfWeeklyRouteMapDataUrl(resumen, {
    width: rasterW,
    height: rasterH,
    paddingPx: 30,
  })
  const mapDataUrl = mapFromGeometry ?? mapBlocks[0]?.dataUrl ?? null

  if (mapDataUrl) {
    await drawWeeklyMapInBox(doc, MARGIN_MM, mapTop, INNER_W, mapH, mapDataUrl)
  } else {
    y = ensureSpace(doc, y, 14)
    doc.setFontSize(9)
    doc.setTextColor(...TEXT_MUTED)
    doc.text("Sin geometría de ruta ni imagen de mapa para mostrar.", MARGIN_MM, mapTop + 8)
  }

  const colsLunMie = buildClienteColumnsLunMie(resumen)
  doc.addPage()
  y = MARGIN_MM
  y = drawClienteDayColumns(doc, y, "Clientes por día (Lunes a miércoles)", colsLunMie)

  const colsJueVie = buildClienteColumnsJueVie(resumen)
  doc.addPage()
  y = MARGIN_MM
  y = drawClienteDayColumns(doc, y, "Clientes por día (Jueves y viernes)", colsJueVie)

  doc.addPage()
  y = MARGIN_MM
  y = drawSectionHeader(doc, y, "Análisis operativo")
  y += CONTENT_PAD_MM
  const { paragraphs: insights } = (() => {
    try {
      return { paragraphs: buildOperationalInsights(resumen) }
    } catch {
      return {
        paragraphs: [
          "No fue posible generar el análisis automático para este vendedor con los datos recibidos.",
        ],
      }
    }
  })()
  doc.setFont("helvetica", "normal")
  doc.setTextColor(...TEXT_DARK)
  for (const para of insights) {
    y = ensureSpace(doc, y, 26)
    y = addParagraph(
      doc,
      para,
      MARGIN_MM + CONTENT_PAD_MM,
      y,
      INNER_W - CONTENT_PAD_MM * 2,
      9.2,
      4.1,
      TEXT_DARK,
    )
    y += 2.5
  }

  const fname = `ruta_${slugFilePart(resumen.vendedor)}_${format(new Date(), "yyyy-MM-dd")}.pdf`
  doc.save(fname)
}
