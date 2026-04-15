import { format } from "date-fns"

import type { DistribuidoraResumenDiaJson, DistribuidoraResumenVendedorJson } from "@/lib/api"
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

export type ExportResumenVendedorPdfParams = {
  resumen: DistribuidoraResumenVendedorJson
  mapElement: HTMLElement
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

function parseClienteRow(c: Record<string, unknown>): {
  orden: number
  nombre: string
  comuna: string
  tipoLabel: string
} {
  const orden = Number(c.orden_visita) || 0
  const nombre = String(
    c.cliente_nombre ?? c.nombre_fantasia ?? c.nombre ?? c.razon_social ?? "Cliente",
  ).trim()
  const comuna = String(c.municipality ?? c.comuna ?? "").trim() || "-"
  const tipoRaw = String(c.tipo_atencion ?? "").toLowerCase()
  const tipoLabel = tipoRaw.includes("telefon") ? "Telefónico" : "Terreno"
  return { orden, nombre, comuna, tipoLabel }
}

function clientesOrdenadosRows(dia: DistribuidoraResumenDiaJson): Record<string, unknown>[] {
  const raw = dia.clientes
  if (!Array.isArray(raw)) return []
  const rows = raw as Record<string, unknown>[]
  return [...rows].sort((a, b) => (Number(a.orden_visita) || 0) - (Number(b.orden_visita) || 0))
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

function chunkDias<T>(arr: T[], size: number): T[][] {
  const out: T[][] = []
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size))
  }
  return out
}

function parseHexRgb(s: string | undefined): [number, number, number] {
  const raw = String(s ?? "#b91c1c").trim()
  const hex = raw.startsWith("#") ? raw.slice(1) : raw
  if (/^[0-9a-fA-F]{6}$/.test(hex)) {
    return [parseInt(hex.slice(0, 2), 16), parseInt(hex.slice(2, 4), 16), parseInt(hex.slice(4, 6), 16)]
  }
  return [BRAND_R, BRAND_G, BRAND_B]
}

async function captureMapJpeg(mapElement: HTMLElement): Promise<string> {
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
 * Genera y descarga el informe PDF (mapa real vía html2canvas-pro, texto operativo dinámico).
 * Tipografía y secciones pensadas para impresión (sin iconos ni emojis).
 */
export async function exportResumenVendedorPdf(params: ExportResumenVendedorPdfParams): Promise<void> {
  const { jsPDF } = await import("jspdf")
  const { resumen, mapElement, viaticoClp, rendimientoKmL, precioCombustibleClp } = params

  const mapDataUrl = await captureMapJpeg(mapElement)

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

  y = drawSectionHeader(doc, y, "Mapa de rutas")
  y += CONTENT_PAD_MM
  const maxMapW = INNER_W
  const maxMapH = 148
  const img = new Image()
  await new Promise<void>((resolve, reject) => {
    img.onload = () => resolve()
    img.onerror = () => reject(new Error("No se pudo leer la imagen del mapa"))
    img.src = mapDataUrl
  })
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
  doc.addImage(mapDataUrl, "JPEG", MARGIN_MM, y, wMm, hMm)
  y += hMm + 8

  const dias = resumen?.dias ?? []
  const pairs = chunkDias(dias, 2)
  const minSpaceToStartDetalle = 36
  if (pairs.length > 0) {
    if (y > PAGE_H_MM - MARGIN_MM - minSpaceToStartDetalle) {
      doc.addPage()
      y = MARGIN_MM
    }
    for (let pi = 0; pi < pairs.length; pi++) {
      if (pi > 0) {
        doc.addPage()
        y = MARGIN_MM
      }
      y = drawSectionHeader(doc, y, "Detalle por jornadas")
      y += CONTENT_PAD_MM - 1
      for (const dia of pairs[pi]) {
        y = ensureSpace(doc, y, 11)
        const [r, g, b] = parseHexRgb(dia.color)
        const bandH = 6.8
        doc.setFillColor(...SECTION_BG)
        doc.rect(MARGIN_MM + 2.2, y, INNER_W - 2.2, bandH, "F")
        doc.setFillColor(r, g, b)
        doc.rect(MARGIN_MM, y, 2.2, bandH, "F")
        doc.setDrawColor(...BORDER_LIGHT)
        doc.rect(MARGIN_MM, y, INNER_W, bandH, "S")
        doc.setTextColor(...TEXT_DARK)
        doc.setFont("helvetica", "bold")
        doc.setFontSize(9.5)
        doc.text(
          `${String(dia.dia ?? "-")}  |  ${fmtMetric(dia.km_totales)} km  |  ${fmtMetric(dia.clientes_count)} clientes  |  ${fmtMetric(dia.minutos_totales)} min`,
          MARGIN_MM + 5,
          y + bandH - 2,
        )
        doc.setFont("helvetica", "normal")
        y += bandH + 2.2

        const rows = clientesOrdenadosRows(dia)
        if (rows.length === 0) {
          y = ensureSpace(doc, y, 5)
          doc.setFontSize(8.5)
          doc.setTextColor(...TEXT_MUTED)
          doc.text("Sin clientes listados para esta jornada.", MARGIN_MM + CONTENT_PAD_MM, y)
          y += 5.5
          continue
        }

        for (const raw of rows) {
          const c = parseClienteRow(raw)
          const line = `${c.orden}. ${c.nombre} - ${c.comuna} (${c.tipoLabel})`
          y = ensureSpace(doc, y, 5)
          doc.setFontSize(8.2)
          doc.setTextColor(...TEXT_DARK)
          const wrapped = doc.splitTextToSize(line, INNER_W - CONTENT_PAD_MM * 2)
          doc.text(wrapped, MARGIN_MM + CONTENT_PAD_MM, y)
          y += wrapped.length * 3.5 + 0.3
        }
        y += 2.5
      }
    }
  }

  y += 2
  y = ensureSpace(doc, y, 28)
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
