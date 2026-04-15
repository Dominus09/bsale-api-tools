import { format } from "date-fns"

import type { DistribuidoraResumenDiaJson, DistribuidoraResumenVendedorJson } from "@/lib/api"

const PAGE_W_MM = 210
const PAGE_H_MM = 297
const MARGIN_MM = 12
const INNER_W = PAGE_W_MM - MARGIN_MM * 2

const BRAND_R = 196
const BRAND_G = 41
const BRAND_B = 60

const HEADER_FILL: [number, number, number] = [241, 245, 249]
const TEXT_DARK: [number, number, number] = [15, 23, 42]
const TEXT_MUTED: [number, number, number] = [71, 85, 105]

export type ExportResumenVendedorPdfParams = {
  resumen: DistribuidoraResumenVendedorJson
  mapElement: HTMLElement
  viaticoClp: number | null
  rendimientoKmL: number | null
  precioCombustibleClp: number | null
}

function formatClp(n: number): string {
  return Math.round(n).toLocaleString("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  })
}

function slugFilePart(s: string): string {
  return String(s || "vendedor")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/[^a-z0-9._-]/g, "")
    .slice(0, 48) || "vendedor"
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
  const comuna = String(c.municipality ?? c.comuna ?? "").trim() || "—"
  const tipoRaw = String(c.tipo_atencion ?? "").toLowerCase()
  const tipoLabel = tipoRaw.includes("telefon") ? "Telefónico ☎️" : "Terreno"
  return { orden, nombre, comuna, tipoLabel }
}

function clientesOrdenadosRows(dia: DistribuidoraResumenDiaJson): Record<string, unknown>[] {
  const raw = dia.clientes
  if (!Array.isArray(raw)) return []
  const rows = raw as Record<string, unknown>[]
  return [...rows].sort((a, b) => (Number(a.orden_visita) || 0) - (Number(b.orden_visita) || 0))
}

/** km totales / visitas semana (misma semana que el informe). */
function kmPorClienteSemana(resumen: DistribuidoraResumenVendedorJson): number {
  const n = resumen.clientes_total_semana
  if (!n || n <= 0) return 0
  return resumen.km_total_semana / n
}

function clasificarEficiencia(kmPorCli: number): { etiqueta: "Alta" | "Media" | "Baja"; texto: string } {
  if (kmPorCli <= 0) return { etiqueta: "Media", texto: "sin datos de clientes" }
  if (kmPorCli < 5) return { etiqueta: "Alta", texto: `${kmPorCli.toFixed(1)} km por cliente` }
  if (kmPorCli <= 10) return { etiqueta: "Media", texto: `${kmPorCli.toFixed(1)} km por cliente` }
  return { etiqueta: "Baja", texto: `${kmPorCli.toFixed(1)} km por cliente` }
}

function buildOperationalInsights(resumen: DistribuidoraResumenVendedorJson): string[] {
  const out: string[] = []
  const dias = resumen.dias
  const kmT = Number(resumen.km_total_semana) || 0
  const nCli = resumen.clientes_total_semana || 0

  if (!dias.length) {
    out.push("No hay jornadas con ruta terrestre registrada para este vendedor en el resumen actual.")
    return out
  }

  const entries = dias.map((d) => ({
    dia: d.dia,
    km: Number(d.km_totales) || 0,
    clis: Number(d.clientes_count) || 0,
    kmPc: d.km_por_cliente != null ? Number(d.km_por_cliente) : 0,
  }))

  const maxEntry = entries.reduce((a, b) => (a.km >= b.km ? a : b))
  const pctMax = kmT > 0 ? (100 * maxEntry.km) / kmT : 0
  if (pctMax >= 40 && entries.length >= 2) {
    out.push(
      `Durante la semana, la carga de kilómetros se concentra con fuerza el día ${maxEntry.dia} (aprox. ${pctMax.toFixed(
        0,
      )}% del total), lo que marca el pico operativo de la semana.`,
    )
  }

  const avgKmDia = entries.length ? kmT / entries.length : 0
  const lowDays = entries.filter((e) => e.km > 0 && avgKmDia > 0 && e.km < avgKmDia * 0.45)
  if (lowDays.length >= 1 && entries.length >= 3) {
    out.push(
      `Existen jornadas con menor recorrido (${lowDays
        .map((x) => x.dia)
        .join(", ")}), lo que puede representar oportunidades de redistribución de visitas.`,
    )
  }

  if (kmT >= 400) {
    out.push("El kilometraje semanal acumulado es elevado: la ruta implica alta exigencia logística en tiempo de manejo y costo de combustible.")
  } else if (kmT > 0 && kmT <= 130 && entries.length >= 2) {
    out.push("El kilometraje total es moderado: podría existir margen para densificar visitas en algunas jornadas sin saturar la semana.")
  }

  const kmPerClienteGlobal = nCli > 0 ? kmT / nCli : 0
  if (kmPerClienteGlobal >= 12) {
    out.push(
      "El promedio de kilómetros por cliente sugiere puntos de atención relativamente dispersos, con impacto directo en tiempos de traslado.",
    )
  } else if (kmPerClienteGlobal > 0 && kmPerClienteGlobal <= 4.5) {
    out.push(
      "El promedio de kilómetros por cliente es favorable: la geografía de visitas tiende a ser compacta entre paradas consecutivas.",
    )
  }

  const kms = entries.map((e) => e.km)
  const mean = kms.reduce((s, x) => s + x, 0) / Math.max(kms.length, 1)
  const stdev = Math.sqrt(kms.reduce((s, x) => s + (x - mean) ** 2, 0) / Math.max(kms.length, 1))
  if (mean > 0 && stdev / mean > 0.38 && entries.length >= 3) {
    out.push(
      "Hay desbalance entre jornadas (variación notable en km por día); conviene revisar la asignación semanal frente al mix de clientes y prioridades.",
    )
  }

  if (entries.some((e) => e.clis > 0 && e.km / e.clis > 18)) {
    out.push("Algunos días muestran ratios altos de km por cliente, típicos de trayectos largos o secuencias poco compactas.")
  }

  out.push(
    "Se sugiere revisar periódicamente la secuencia de visitas y el equilibrio entre días para optimizar costos y tiempos, alineando la operación con metas comerciales y de servicio.",
  )

  return out
}

function drawSectionBand(
  doc: import("jspdf").jsPDF,
  y: number,
  title: string,
  icon: string,
): number {
  const h = 7
  doc.setFillColor(...HEADER_FILL)
  doc.rect(MARGIN_MM, y, INNER_W, h, "F")
  doc.setDrawColor(226, 232, 240)
  doc.rect(MARGIN_MM, y, INNER_W, h, "S")
  doc.setTextColor(...TEXT_DARK)
  doc.setFontSize(10)
  doc.setFont("helvetica", "bold")
  doc.text(`${icon} ${title}`, MARGIN_MM + 2, y + 5)
  doc.setFont("helvetica", "normal")
  return y + h + 3
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
  await new Promise((r) => setTimeout(r, 200))
  const canvas = await html2canvas(mapElement, {
    useCORS: true,
    allowTaint: true,
    scale: Math.min(2, Math.max(1, typeof window !== "undefined" ? window.devicePixelRatio : 1.5)),
    logging: false,
    backgroundColor: "#dfe6ee",
    windowWidth: mapElement.scrollWidth,
    windowHeight: mapElement.scrollHeight,
    onclone: (_doc, el) => {
      el.style.setProperty("background-color", "#dfe6ee", "important")
    },
  })
  return canvas.toDataURL("image/jpeg", 0.9)
}

/**
 * Genera y descarga el informe PDF (mapa real vía html2canvas-pro, texto operativo dinámico).
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
  doc.rect(0, 0, PAGE_W_MM, 22, "F")
  doc.setTextColor(255, 255, 255)
  doc.setFontSize(16)
  doc.setFont("helvetica", "bold")
  doc.text(`Informe de Ruta Semanal — ${resumen.vendedor}`, MARGIN_MM, 14)
  doc.setFont("helvetica", "normal")
  doc.setFontSize(9)
  doc.text(`Generado: ${format(new Date(), "yyyy-MM-dd HH:mm")}`, MARGIN_MM, 19)
  doc.setTextColor(...TEXT_DARK)

  y = 28

  y = drawSectionBand(doc, y, "Resumen general", "📊")

  const bloques: string[] = [
    `Km total semana: ${resumen.km_total_semana} km`,
    `Clientes (visitas): ${resumen.clientes_total_semana}`,
    `Tiempo conducción (estim.): ${resumen.min_total_semana} min`,
    `Promedio km / día: ${resumen.promedio_km_por_dia} km`,
    `Día más largo: ${resumen.km_dia_mas_largo} km`,
    `Día más corto: ${resumen.km_dia_mas_corto} km`,
  ]
  if (viaticoClp != null && Number.isFinite(viaticoClp)) {
    bloques.push(`Viático estimado: ${formatClp(viaticoClp)}`)
  } else {
    bloques.push("Viático estimado: (complete rendimiento y precio en el panel)")
  }

  doc.setFontSize(9.5)
  doc.setTextColor(...TEXT_DARK)
  const colW = INNER_W / 2 - 4
  let col = 0
  let rowY = y
  const startY = y
  for (const line of bloques) {
    const x = MARGIN_MM + col * (colW + 8)
    doc.text(line, x, rowY)
    col += 1
    if (col >= 2) {
      col = 0
      rowY += 5
    }
  }
  if (col !== 0) rowY += 5
  y = Math.max(rowY, startY + 28) + 4

  y = drawSectionBand(doc, y, "Viático (cálculo transparente)", "⛽")
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
      `👉 Viático estimado: ${formatClp(totalClp)}`,
    ]
    for (const ln of linesVi) {
      y = ensureSpace(doc, y, 6)
      doc.setFontSize(9.5)
      doc.setTextColor(...TEXT_DARK)
      doc.text(ln, MARGIN_MM, y)
      y += 5.2
    }
  } else {
    y = ensureSpace(doc, y, 6)
    doc.setFontSize(9.5)
    doc.setTextColor(...TEXT_MUTED)
    y = addParagraph(
      doc,
      "Para desglosar litros y costo, ingrese en el panel el rendimiento del vehículo (km/l) y el precio del combustible (CLP/l). El total mostrado en el resumen superior se calcula con esos valores y los km de la semana.",
      MARGIN_MM,
      y,
      INNER_W,
      9.5,
      4.2,
    )
    y += 2
  }

  y += 2
  const kmPc = kmPorClienteSemana(resumen)
  const eff = clasificarEficiencia(kmPc)
  y = drawSectionBand(doc, y, "Indicador de eficiencia", "📈")
  y = ensureSpace(doc, y, 8)
  doc.setFontSize(10)
  doc.setTextColor(...TEXT_DARK)
  doc.setFont("helvetica", "bold")
  doc.text(`Eficiencia de ruta: ${eff.etiqueta} (${eff.texto})`, MARGIN_MM, y)
  doc.setFont("helvetica", "normal")
  y += 8

  y = drawSectionBand(doc, y, "Mapa de rutas (captura pantalla)", "📍")
  const maxMapW = INNER_W
  const maxMapH = 118
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
  y = ensureSpace(doc, y, hMm + 8)
  doc.addImage(mapDataUrl, "JPEG", MARGIN_MM, y, wMm, hMm)
  y += hMm + 10

  const pairs = chunkDias(resumen.dias, 2)
  if (pairs.length > 0) {
    doc.addPage()
    y = MARGIN_MM
    for (let pi = 0; pi < pairs.length; pi++) {
      if (pi > 0) {
        doc.addPage()
        y = MARGIN_MM
      }
      y = drawSectionBand(doc, y, "Detalle por día (visitas)", "📍")
      for (const dia of pairs[pi]) {
        y = ensureSpace(doc, y, 16)
        const [r, g, b] = parseHexRgb(dia.color)
        doc.setFillColor(248, 250, 252)
        doc.rect(MARGIN_MM + 3, y, INNER_W - 3, 8, "F")
        doc.setFillColor(r, g, b)
        doc.rect(MARGIN_MM, y, 3, 8, "F")
        doc.setDrawColor(226, 232, 240)
        doc.rect(MARGIN_MM, y, INNER_W, 8, "S")
        doc.setTextColor(...TEXT_DARK)
        doc.setFont("helvetica", "bold")
        doc.setFontSize(10)
        doc.text(
          `${dia.dia}  ·  ${dia.km_totales} km  ·  ${dia.clientes_count} clientes  ·  ${dia.minutos_totales} min`,
          MARGIN_MM + 6,
          y + 5.5,
        )
        doc.setFont("helvetica", "normal")
        y += 11

        const rows = clientesOrdenadosRows(dia)
        if (rows.length === 0) {
          y = ensureSpace(doc, y, 6)
          doc.setFontSize(9)
          doc.setTextColor(...TEXT_MUTED)
          doc.text("Sin clientes con coordenadas en esta jornada.", MARGIN_MM, y)
          y += 7
          continue
        }

        for (const raw of rows) {
          const c = parseClienteRow(raw)
          const line = `${c.orden}. ${c.nombre} — ${c.comuna} (${c.tipoLabel})`
          y = ensureSpace(doc, y, 5)
          doc.setFontSize(8.8)
          doc.setTextColor(...TEXT_DARK)
          const wrapped = doc.splitTextToSize(line, INNER_W - 2)
          doc.text(wrapped, MARGIN_MM + 1, y)
          y += wrapped.length * 4.2 + 0.5
        }
        y += 4
      }
    }
  }

  doc.addPage()
  y = MARGIN_MM
  y = drawSectionBand(doc, y, "Análisis operativo (automático)", "📊")
  const insights = buildOperationalInsights(resumen)
  doc.setFont("helvetica", "normal")
  doc.setTextColor(...TEXT_DARK)
  for (const para of insights) {
    y = ensureSpace(doc, y, 28)
    y = addParagraph(doc, para, MARGIN_MM, y, INNER_W, 9.5, 4.3, TEXT_DARK)
    y += 3
  }

  const fname = `ruta_${slugFilePart(resumen.vendedor)}_${format(new Date(), "yyyy-MM-dd")}.pdf`
  doc.save(fname)
}
