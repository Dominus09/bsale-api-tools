import { format } from "date-fns"

import type { DistribuidoraResumenVendedorJson } from "@/lib/api"
import {
  buildConsolidatedSemanaClientRows,
  buildDiasCargaSummaryLines,
  type ConsolidatedClientePdfRow,
} from "@/lib/resumen-vendedor-pdf-consolidado"
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
  /** Un mapa por bloque de días (misma lógica que el detalle: 2 días por bloque). */
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

export function chunkDias<T>(arr: T[], size: number): T[][] {
  const out: T[][] = []
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size))
  }
  return out
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

function truncatePdfCell(s: string, maxLen: number): string {
  const t = s.trim()
  if (t.length <= maxLen) return t
  return `${t.slice(0, Math.max(0, maxLen - 1))}…`
}

function drawConsolidatedClientesTable(
  doc: import("jspdf").jsPDF,
  yStart: number,
  rows: ConsolidatedClientePdfRow[],
): number {
  let y = yStart
  const x0 = MARGIN_MM
  const wOrd = 12
  const wNom = 78
  const wDia = 28
  const wCom = INNER_W - wOrd - wNom - wDia - 2
  const rowH = 4.05
  const headH = 5.2

  y = ensureSpace(doc, y, headH + 6)
  const headTop = y
  doc.setFillColor(226, 232, 240)
  doc.rect(x0, headTop, INNER_W, headH, "F")
  doc.setDrawColor(...BORDER_LIGHT)
  doc.rect(x0, headTop, INNER_W, headH, "S")
  doc.setFont("helvetica", "bold")
  doc.setFontSize(7.8)
  doc.setTextColor(...TEXT_DARK)
  const headTextY = headTop + headH - 1.4
  doc.text("Orden", x0 + 1, headTextY)
  doc.text("Cliente", x0 + wOrd + 1, headTextY)
  doc.text("Día", x0 + wOrd + wNom + 1, headTextY)
  doc.text("Comuna", x0 + wOrd + wNom + wDia + 1, headTextY)
  doc.setFont("helvetica", "normal")
  y = headTop + headH + 1.5

  for (const r of rows) {
    y = ensureSpace(doc, y, rowH + 1)
    const rowTop = y
    doc.setDrawColor(...BORDER_LIGHT)
    doc.line(x0, rowTop + rowH, x0 + INNER_W, rowTop + rowH)
    doc.setFontSize(7.5)
    doc.setTextColor(...TEXT_DARK)
    const ty = rowTop + 3.15
    doc.text(String(r.ordenGlobal), x0 + 1, ty)
    doc.text(truncatePdfCell(r.nombre, 52), x0 + wOrd + 1, ty)
    doc.text(truncatePdfCell(r.dia, 14), x0 + wOrd + wNom + 1, ty)
    doc.text(truncatePdfCell(r.comuna, 36), x0 + wOrd + wNom + wDia + 1, ty)
    y = rowTop + rowH
  }
  return y + 2
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

  const consolidated = buildConsolidatedSemanaClientRows(resumen)
  const diasCargaLines = buildDiasCargaSummaryLines(resumen)
  const nMap = mapBlocks.length

  if (nMap > 0) {
    const firstMaxH = Math.max(88, Math.min(168, PAGE_H_MM - MARGIN_MM - y - 10))
    y = await drawMapImageBlock(doc, y, mapBlocks[0].dataUrl, mapBlocks[0].title, firstMaxH)
    for (let mi = 1; mi < nMap; mi++) {
      const isLast = mi === nMap - 1
      doc.addPage()
      y = MARGIN_MM
      const maxH =
        isLast && consolidated.length > 0 ? Math.min(105, 132) : Math.min(155, PAGE_H_MM - MARGIN_MM - y - 8)
      y = await drawMapImageBlock(doc, y, mapBlocks[mi].dataUrl, mapBlocks[mi].title, maxH)
    }
  }

  if (consolidated.length > 0) {
    if (y > PAGE_H_MM - MARGIN_MM - 50) {
      doc.addPage()
      y = MARGIN_MM
    }
    y = drawSectionHeader(doc, y, "Clientes de la semana")
    y += CONTENT_PAD_MM
    y = drawConsolidatedClientesTable(doc, y, consolidated)
  }

  if (diasCargaLines.length > 0) {
    y = ensureSpace(doc, y, 18)
    if (y > PAGE_H_MM - MARGIN_MM - 36) {
      doc.addPage()
      y = MARGIN_MM
    }
    y = drawSectionHeader(doc, y, "Resumen por día")
    y += CONTENT_PAD_MM
    doc.setFontSize(9)
    doc.setTextColor(...TEXT_DARK)
    for (const ln of diasCargaLines) {
      y = ensureSpace(doc, y, 5)
      doc.text(ln, MARGIN_MM + CONTENT_PAD_MM, y)
      y += 4.5
    }
    y += 3
  }

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
