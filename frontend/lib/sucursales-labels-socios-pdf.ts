/**
 * PDF carta — Etiquetas Socios V2 · Socio Estándar horizontal góndola.
 * Medida objetivo por etiqueta: 100 mm × 40 mm (10 cm × 4 cm).
 * Precio Normal (izq.) + Precio Socio (der., azul dominante).
 * Sin % descuento. "Provisional" nunca se imprime.
 */

import {
  loadQuillotanaLogoForPdf,
  type PdfLogoPayload,
} from "@/lib/quillotana-logo-pdf"

export type SocioLabelPrintOptions = {
  showProductType: boolean
  showBarcode: boolean
  showPrices: boolean
}

export type SocioLabelPrintItem = {
  barcode: string
  productType: string
  productName: string
  variantName: string
  normalPrice: number | null
  socioPrice: number | null
  quantity: number
}

/** 10 cm × 4 cm — 2×6 = 12/hoja carta (legibilidad > densidad). */
export const SOCIO_ESTANDAR_FORMAT = {
  id: "SOCIO_ESTANDAR" as const,
  label: "Socio Estándar",
  description: "12 etiquetas · 10×4 cm · góndola horizontal",
  cols: 2,
  rows: 6,
  perPage: 12,
  /** Ancho real de la etiqueta impresa (mm) */
  labelWMm: 100,
  /** Alto real de la etiqueta impresa (mm) */
  labelHMm: 40,
}

const PAGE_W = 215.9
const PAGE_H = 279.4
const MARGIN_MM = 5

const COLOR_CATEGORY = { r: 110, g: 110, b: 110 }
const COLOR_VARIANT = { r: 70, g: 70, b: 70 }
const COLOR_BORDER = { r: 180, g: 180, b: 180 }
const COLOR_SOCIO = { r: 0, g: 90, b: 168 }
const COLOR_SOCIO_BOX = { r: 230, g: 242, b: 255 }
const COLOR_NORMAL_LABEL = { r: 90, g: 90, b: 90 }

type BarcodeSpec = { w: number; h: number; bar: number; minMm: number }

/** Barcode bajo y ancho para franja inferior horizontal */
const BARCODE_SPEC: BarcodeSpec = { w: 420, h: 36, bar: 1.35, minMm: 6.5 }

function formatClp(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "—"
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  }).format(value)
}

function flattenItems(items: SocioLabelPrintItem[]): SocioLabelPrintItem[] {
  return items.flatMap((item) =>
    Array.from({ length: Math.max(1, item.quantity) }, () => ({
      ...item,
      quantity: 1,
    })),
  )
}

function fitLines(
  doc: import("jspdf").jsPDF,
  text: string,
  maxWidth: number,
  maxLines: number,
  fontSize: number,
): string[] {
  doc.setFontSize(fontSize)
  const lines = doc.splitTextToSize(text.trim(), maxWidth) as string[]
  if (lines.length <= maxLines) return lines
  const kept = lines.slice(0, maxLines)
  let last = kept[maxLines - 1] ?? ""
  while (last.length > 4 && doc.getTextWidth(`${last}…`) > maxWidth) {
    last = last.slice(0, -1)
  }
  kept[maxLines - 1] = `${last.replace(/\s+$/, "")}…`
  return kept
}

async function barcodeDataUrl(
  code: string,
  spec: BarcodeSpec,
): Promise<string | null> {
  if (typeof window === "undefined" || !code.trim()) return null
  try {
    const JsBarcode = (await import("jsbarcode")).default
    const raw = document.createElement("canvas")
    const trimmed = code.trim()
    const fmt = /^\d{12,13}$/.test(trimmed) ? "EAN13" : "CODE128"
    JsBarcode(raw, trimmed, {
      format: fmt,
      width: spec.bar,
      height: spec.h,
      displayValue: false,
      margin: 1,
      marginTop: 0,
      marginBottom: 0,
      flat: true,
    })
    const scaled = document.createElement("canvas")
    scaled.width = spec.w
    scaled.height = spec.h
    const ctx = scaled.getContext("2d")
    if (!ctx) return null
    ctx.fillStyle = "#ffffff"
    ctx.fillRect(0, 0, spec.w, spec.h)
    ctx.imageSmoothingEnabled = false
    ctx.drawImage(raw, 0, 0, spec.w, spec.h)
    return scaled.toDataURL("image/jpeg", 0.92)
  } catch {
    return null
  }
}

async function buildBarcodeCache(
  barcodes: string[],
  spec: BarcodeSpec,
): Promise<Map<string, string | null>> {
  const unique = [...new Set(barcodes.map((b) => b.trim()).filter(Boolean))]
  const pairs = await Promise.all(
    unique.map(async (bc) => [bc, await barcodeDataUrl(bc, spec)] as const),
  )
  return new Map(pairs)
}

/**
 * Centra celdas fijas 100×40 mm en hoja carta (2 columnas × 6 filas).
 * Gap residual se reparte alrededor para centrar el bloque.
 */
function labelPlacement(col: number, row: number): { x: number; y: number } {
  const { cols, rows, labelWMm, labelHMm } = SOCIO_ESTANDAR_FORMAT
  const usableW = PAGE_W - MARGIN_MM * 2
  const usableH = PAGE_H - MARGIN_MM * 2
  const gapX = Math.max(0, (usableW - cols * labelWMm) / (cols + 1))
  const gapY = Math.max(0, (usableH - rows * labelHMm) / (rows + 1))
  return {
    x: MARGIN_MM + gapX + col * (labelWMm + gapX),
    y: MARGIN_MM + gapY + row * (labelHMm + gapY),
  }
}

function drawSocioEstandarLabel(
  doc: import("jspdf").jsPDF,
  item: SocioLabelPrintItem,
  x: number,
  y: number,
  w: number,
  h: number,
  options: SocioLabelPrintOptions,
  logo: PdfLogoPayload,
  barcodeImg: string | null,
) {
  const padX = 2.2
  const padY = 1.4
  const innerX = x + padX
  const innerW = w - padX * 2

  doc.setDrawColor(COLOR_BORDER.r, COLOR_BORDER.g, COLOR_BORDER.b)
  doc.setLineWidth(0.15)
  doc.setFillColor(255, 255, 255)
  doc.rect(x, y, w, h, "FD")

  // —— Franja superior: logo + categoría ——
  const logoH = 4.2
  const logoW = Math.min(22, logoH * logo.aspectRatio)
  doc.addImage(
    logo.dataUrl,
    logo.format,
    innerX,
    y + padY,
    logoW,
    logoW / logo.aspectRatio,
    undefined,
    "FAST",
  )

  let textLeft = innerX + logoW + 1.8
  let textTop = y + padY + 1.2

  if (options.showProductType && item.productType) {
    doc.setFontSize(5)
    doc.setFont("helvetica", "normal")
    doc.setTextColor(COLOR_CATEGORY.r, COLOR_CATEGORY.g, COLOR_CATEGORY.b)
    doc.text(
      item.productType.toUpperCase(),
      textLeft,
      textTop,
      { maxWidth: innerW - logoW - 2 },
    )
    textTop += 2.1
  } else {
    textTop += 0.4
  }

  // Producto (1–2 líneas)
  const nameMaxW = innerW - (logoW + 2)
  doc.setFont("helvetica", "bold")
  doc.setTextColor(0, 0, 0)
  const productLines = fitLines(doc, item.productName, nameMaxW, 2, 8)
  doc.setFontSize(8)
  doc.text(productLines, textLeft, textTop)
  textTop += productLines.length * 2.85

  const variant =
    item.variantName &&
    item.variantName.trim().toLowerCase() !== item.productName.trim().toLowerCase()
      ? item.variantName.trim()
      : ""
  if (variant) {
    doc.setFontSize(6)
    doc.setFont("helvetica", "normal")
    doc.setTextColor(COLOR_VARIANT.r, COLOR_VARIANT.g, COLOR_VARIANT.b)
    doc.text(fitLines(doc, variant, nameMaxW, 1, 6), textLeft, textTop)
    textTop += 2.3
  }

  // —— Separador ——
  const pricesTop = Math.max(textTop + 0.6, y + 14.5)
  doc.setDrawColor(210, 210, 210)
  doc.setLineWidth(0.12)
  doc.line(innerX, pricesTop, x + w - padX, pricesTop)

  // —— Precios lado a lado ——
  const priceBandTop = pricesTop + 1.0
  const priceBandH = 12.5
  const gap = 1.6
  const colW = (innerW - gap) / 2

  if (options.showPrices) {
    // Izquierda: Precio Normal
    const nX = innerX
    doc.setFontSize(4.8)
    doc.setFont("helvetica", "normal")
    doc.setTextColor(
      COLOR_NORMAL_LABEL.r,
      COLOR_NORMAL_LABEL.g,
      COLOR_NORMAL_LABEL.b,
    )
    doc.text("PRECIO NORMAL", nX + colW / 2, priceBandTop + 2.4, {
      align: "center",
    })
    doc.setFontSize(11)
    doc.setFont("helvetica", "bold")
    doc.setTextColor(25, 25, 25)
    doc.text(formatClp(item.normalPrice), nX + colW / 2, priceBandTop + 8.2, {
      align: "center",
    })

    // Derecha: Precio Socio (bloque horizontal azul)
    const sX = innerX + colW + gap
    doc.setFillColor(COLOR_SOCIO_BOX.r, COLOR_SOCIO_BOX.g, COLOR_SOCIO_BOX.b)
    doc.setDrawColor(COLOR_SOCIO.r, COLOR_SOCIO.g, COLOR_SOCIO.b)
    doc.setLineWidth(0.3)
    doc.roundedRect(sX, priceBandTop, colW, priceBandH - 0.5, 0.6, 0.6, "FD")

    doc.setFontSize(4.8)
    doc.setFont("helvetica", "bold")
    doc.setTextColor(COLOR_SOCIO.r, COLOR_SOCIO.g, COLOR_SOCIO.b)
    doc.text("PRECIO SOCIO", sX + colW / 2, priceBandTop + 2.6, {
      align: "center",
    })
    doc.setFontSize(13.5)
    doc.setFont("helvetica", "bold")
    doc.setTextColor(COLOR_SOCIO.r, COLOR_SOCIO.g, COLOR_SOCIO.b)
    doc.text(formatClp(item.socioPrice), sX + colW / 2, priceBandTop + 9.2, {
      align: "center",
    })
  }

  // —— Barcode inferior ——
  if (options.showBarcode) {
    const bcZoneTop = y + h - padY - 9.2
    doc.setDrawColor(220, 220, 220)
    doc.setLineWidth(0.1)
    doc.line(innerX, bcZoneTop - 0.6, x + w - padX, bcZoneTop - 0.6)

    const bcH = BARCODE_SPEC.minMm
    const bcW = innerW * 0.72
    const bcX = innerX + (innerW - bcW) / 2

    if (barcodeImg) {
      try {
        doc.addImage(barcodeImg, "JPEG", bcX, bcZoneTop, bcW, bcH, undefined, "FAST")
      } catch {
        /* número solo */
      }
    }

    doc.setFontSize(5.5)
    doc.setFont("helvetica", "normal")
    doc.setTextColor(30, 30, 30)
    doc.text(item.barcode, x + w / 2, y + h - padY - 0.3, { align: "center" })
  }
}

export function estimateSocioLabelPages(totalLabels: number): number {
  const perPage = SOCIO_ESTANDAR_FORMAT.perPage
  return totalLabels > 0 ? Math.ceil(totalLabels / perPage) : 0
}

export async function generateSocioLabelsPdf(
  items: SocioLabelPrintItem[],
  options: SocioLabelPrintOptions,
  filename = "etiquetas-socios.pdf",
): Promise<void> {
  const flat = flattenItems(items)
  if (flat.length === 0) return

  const logo = await loadQuillotanaLogoForPdf()
  if (!logo) {
    throw new Error("No se pudo cargar el logo Quillotana")
  }

  const barcodeCache = await buildBarcodeCache(
    flat.map((it) => it.barcode),
    BARCODE_SPEC,
  )

  const { jsPDF } = await import("jspdf")
  const doc = new jsPDF({
    orientation: "portrait",
    unit: "mm",
    format: "letter",
    compress: true,
  })

  const { cols, rows, labelWMm, labelHMm, perPage } = SOCIO_ESTANDAR_FORMAT

  for (let i = 0; i < flat.length; i++) {
    const posOnPage = i % perPage
    if (i > 0 && posOnPage === 0) doc.addPage()

    const col = posOnPage % cols
    const row = Math.floor(posOnPage / cols)
    // safety if rows math drifts
    if (row >= rows) continue

    const { x, y } = labelPlacement(col, row)
    const item = flat[i]

    drawSocioEstandarLabel(
      doc,
      item,
      x,
      y,
      labelWMm,
      labelHMm,
      options,
      logo,
      barcodeCache.get(item.barcode.trim()) ?? null,
    )
  }

  doc.save(filename)
}
