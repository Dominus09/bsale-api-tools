import { format } from "date-fns"

import type { DistribuidoraResumenVendedorJson } from "@/lib/api"
import {
  buildOperationalInsights,
  clasificarEficiencia,
  kmPorClienteSemana,
} from "@/lib/resumen-vendedor-analisis"

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

async function drawMapImageBlock(
  doc: import("jspdf").jsPDF,
  y: number,
  dataUrl: string,
  title: string,
  maxMapH: number,
): Promise<number> {
  y = drawSectionHeader(doc, y, `Mapa de rutas (${title})`)
  y += CONTENT_PAD_MM
  const img = new Image()
  await new Promise<void>((resolve, reject) => {
    img.onload = () => resolve()
    img.onerror = () => reject(new Error("No se pudo leer la imagen del mapa"))
    img.src = dataUrl
  })
  const maxMapW = INNER_W
  const aspect = img.width / img.height
  let wMm = maxMapW
  let hMm = wMm / aspect
  if (hMm > maxMapH) {
    hMm = maxMapH
    wMm = hMm * aspect
  }
  y = ensureSpace(doc, y, hMm + 10)
  doc.setDrawColor(...BORDER_LIGHT)
  doc.rect(MARGIN_MM - 0.25, y - 0.25, wMm + 0.5, hMm + 0.5, "S")
  doc.addImage(dataUrl, "JPEG", MARGIN_MM, y, wMm, hMm)
  return y + hMm + 8
}

/**
 * Genera y descarga el informe PDF (mapa real vía html2canvas-pro, texto operativo dinámico).
 * Tipografía y secciones pensadas para impresión (sin iconos ni emojis).
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

  let y = MARGIN_MM

  doc.setFillColor(BRAND_R, BRAND_G, BRAND_B)
  doc.rect(0, 0, PAGE_W_MM, 24, "F")
  doc.setTextColor(255, 255, 255)
  doc.setFontSize(15)
  doc.setFont("helvetica", "bold")
  doc.text("Resumen semanal por vendedor", MARGIN_MM, 11)
  doc.setFontSize(12)
  doc.text(String(resumen.vendedor ?? "").trim() || "Vendedor", MARGIN_MM, 18)
  doc.setFont("helvetica", "normal")
  doc.setFontSize(8.5)
  doc.text(`Generado: ${format(new Date(), "yyyy-MM-dd HH:mm")}`, MARGIN_MM, 22.5)
  doc.setTextColor(...TEXT_DARK)

  y = 30

  y = drawSectionHeader(doc, y, "Viático (cálculo transparente)")
  y += CONTENT_PAD_MM
  const km = Number(resumen.km_total_semana) || 0
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
    const linesVi = [
      `Km totales: ${km.toFixed(1)} km`,
      `Rendimiento: ${rendimientoKmL} km/l`,
      `Consumo estimado: ${litros.toFixed(2)} l`,
      `Precio combustible: ${formatClp(precioCombustibleClp)} /l`,
      `Viático estimado: ${formatClp(totalClp)}`,
    ]
    for (const ln of linesVi) {
      y = ensureSpace(doc, y, 6)
      doc.setFontSize(9.2)
      doc.setTextColor(...TEXT_DARK)
      doc.text(ln, MARGIN_MM + CONTENT_PAD_MM, y)
      y += 5
    }
  } else {
    y = ensureSpace(doc, y, 6)
    doc.setFontSize(9.2)
    doc.setTextColor(...TEXT_MUTED)
    y = addParagraph(
      doc,
      "Para desglosar litros y costo, ingrese en el panel el rendimiento del vehículo (km/l) y el precio del combustible (CLP/l). El total del resumen superior usa esos valores y los km de la semana.",
      MARGIN_MM + CONTENT_PAD_MM,
      y,
      INNER_W - CONTENT_PAD_MM * 2,
      9.2,
      4,
    )
    y += 2
  }

  y += 4
  const kmPc = kmPorClienteSemana(resumen)
  const eff = clasificarEficiencia(kmPc)
  y = drawSectionHeader(doc, y, "Métricas")
  y += CONTENT_PAD_MM
  const bloquesMetricas: string[] = [
    `Km total semana: ${fmtMetric(resumen.km_total_semana)} km`,
    `Clientes (visitas): ${fmtMetric(resumen.clientes_total_semana)}`,
    `Tiempo conducción (estim.): ${fmtMetric(resumen.min_total_semana)} min`,
    `Promedio km / día: ${fmtMetric(resumen.promedio_km_por_dia)} km`,
    `Día más largo: ${fmtMetric(resumen.km_dia_mas_largo)} km`,
    `Día más corto: ${fmtMetric(resumen.km_dia_mas_corto)} km`,
  ]
  doc.setFontSize(9.2)
  doc.setTextColor(...TEXT_DARK)
  const colW = INNER_W / 2 - 3
  let col = 0
  let rowY = y
  const startY = y
  for (const line of bloquesMetricas) {
    const x = MARGIN_MM + col * (colW + 6)
    doc.text(line, x, rowY)
    col += 1
    if (col >= 2) {
      col = 0
      rowY += 4.8
    }
  }
  if (col !== 0) rowY += 4.8
  y = Math.max(rowY, startY + 22) + 3
  doc.setFont("helvetica", "bold")
  doc.setFontSize(9.5)
  doc.text(`Indicador de eficiencia: ${eff.etiqueta}`, MARGIN_MM + CONTENT_PAD_MM, y)
  doc.setFont("helvetica", "normal")
  y += 4.5
  y = addParagraph(
    doc,
    eff.texto,
    MARGIN_MM + CONTENT_PAD_MM,
    y,
    INNER_W - CONTENT_PAD_MM * 2,
    9.2,
    4,
    TEXT_DARK,
  )
  y += 6

  const nMap = mapBlocks.length

  if (nMap > 0) {
    const firstMaxH = Math.max(88, Math.min(168, PAGE_H_MM - MARGIN_MM - y - 10))
    y = await drawMapImageBlock(doc, y, mapBlocks[0].dataUrl, mapBlocks[0].title, firstMaxH)
    for (let mi = 1; mi < nMap; mi++) {
      doc.addPage()
      y = MARGIN_MM
      const maxH = Math.min(155, PAGE_H_MM - MARGIN_MM - y - 8)
      y = await drawMapImageBlock(doc, y, mapBlocks[mi].dataUrl, mapBlocks[mi].title, maxH)
    }
  }

  if (y > PAGE_H_MM - MARGIN_MM - 48) {
    doc.addPage()
    y = MARGIN_MM
  }
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
