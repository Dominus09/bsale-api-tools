/**
 * PDF carta — etiquetas de precio sucursales.
 * Formato A económico · B estándar (default) · C oferta/promoción.
 */

import {
  createPdfLogoDocState,
  drawQuillotanaLogoOnPdf,
  loadQuillotanaLogoForPdf,
  type PdfLogoDocState,
  type PdfLogoPayload,
} from "@/lib/quillotana-logo-pdf"

export type LabelFormat = "A" | "B" | "C"

export type LabelPrintOptions = {
  showProductType: boolean
  showBarcode: boolean
  showPrice: boolean
}

export type LabelPrintItem = {
  barcode: string
  productType: string
  productName: string
  variantName: string
  /** Precio lista / precio actual */
  price: number | null
  /** Precio oferta (formato C); fallback: price */
  sale_price?: number | null
  /** Precio anterior tachado (formato C) */
  regular_price?: number | null
  isOffer: boolean
  quantity: number
}

export const LABEL_FORMATS: Record<
  LabelFormat,
  {
    id: LabelFormat
    label: string
    shortLabel: string
    description: string
    cols: number
    rows: number
    perPage: number
  }
> = {
  A: {
    id: "A",
    label: "Económico (A)",
    shortLabel: "Económico",
    description: "30 etiquetas · reposición masiva",
    cols: 3,
    rows: 10,
    perPage: 30,
  },
  B: {
    id: "B",
    label: "Estándar (B)",
    shortLabel: "Estándar",
    description: "24 etiquetas · uso general",
    cols: 3,
    rows: 8,
    perPage: 24,
  },
  C: {
    id: "C",
    label: "Oferta (C)",
    shortLabel: "Oferta",
    description: "12 etiquetas · promociones y remates",
    cols: 2,
    rows: 6,
    perPage: 12,
  },
}

const MARGIN_MM = 2.5
const PAGE_W = 215.9
const PAGE_H = 279.4

const COLOR_CATEGORY = { r: 74, g: 74, b: 74 }
const COLOR_VARIANT = { r: 55, g: 55, b: 55 }
const COLOR_OFFER = { r: 220, g: 38, b: 38 }
const COLOR_BORDER = { r: 190, g: 190, b: 190 }

function formatClp(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "—"
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  }).format(value)
}

function abbreviate(text: string, max = 24): string {
  const t = text.trim()
  if (t.length <= max) return t
  return `${t.slice(0, max - 1)}…`
}

function flattenItems(items: LabelPrintItem[]): LabelPrintItem[] {
  return items.flatMap((item) =>
    Array.from({ length: Math.max(1, item.quantity) }, () => ({ ...item, quantity: 1 })),
  )
}

function effectiveSalePrice(item: LabelPrintItem): number | null {
  return item.sale_price ?? item.price
}

function effectiveRegularPrice(item: LabelPrintItem): number | null {
  return item.regular_price ?? null
}

async function barcodeDataUrl(
  code: string,
  widthPx: number,
  heightPx: number,
  barWidth = 1.2,
): Promise<string | null> {
  if (typeof window === "undefined" || !code.trim()) return null
  try {
    const JsBarcode = (await import("jsbarcode")).default
    const canvas = document.createElement("canvas")
    const fmt = /^\d{12,13}$/.test(code.trim()) ? "EAN13" : "CODE128"
    JsBarcode(canvas, code.trim(), {
      format: fmt,
      width: barWidth,
      height: heightPx,
      displayValue: false,
      margin: 0,
    })
    const scaled = document.createElement("canvas")
    scaled.width = widthPx
    scaled.height = heightPx
    const ctx = scaled.getContext("2d")
    if (!ctx) return null
    ctx.fillStyle = "#ffffff"
    ctx.fillRect(0, 0, widthPx, heightPx)
    ctx.drawImage(canvas, 0, 0, widthPx, heightPx)
    return scaled.toDataURL("image/png")
  } catch {
    return null
  }
}

function labelGrid(format: LabelFormat) {
  const { cols, rows } = LABEL_FORMATS[format]
  const usableW = PAGE_W - MARGIN_MM * 2
  const usableH = PAGE_H - MARGIN_MM * 2
  return { cols, rows, labelW: usableW / cols, labelH: usableH / rows }
}

function drawLogo(
  doc: import("jspdf").jsPDF,
  logo: PdfLogoPayload,
  logoState: PdfLogoDocState,
  x: number,
  y: number,
  innerW: number,
  labelH: number,
  ratio: number,
): number {
  const logoH = labelH * ratio
  const logoW = Math.min(innerW * 0.58, logoH * logo.aspectRatio)
  drawQuillotanaLogoOnPdf(doc, logo, logoState, x, y, logoW)
  return logoH + 0.6
}

function drawBarcodeBlock(
  doc: import("jspdf").jsPDF,
  item: LabelPrintItem,
  x: number,
  y: number,
  w: number,
  h: number,
  pad: number,
  barcodeImg: string | null,
  showBarcode: boolean,
  maxHeightRatio: number,
  fontSize: number,
) {
  if (!showBarcode) return
  const bottom = y + h - pad
  const maxBcH = h * maxHeightRatio
  const numH = fontSize * 0.38
  const bcH = Math.min(maxBcH, bottom - (y + h * 0.55) - numH - 0.5)
  const bcY = bottom - numH - bcH - 0.3

  if (barcodeImg && bcH > 2) {
    try {
      doc.addImage(barcodeImg, "PNG", x + pad, bcY, w - pad * 2, bcH)
    } catch {
      /* texto fallback */
    }
  }

  doc.setFontSize(fontSize)
  doc.setFont("helvetica", "normal")
  doc.setTextColor(30, 30, 30)
  doc.text(item.barcode, x + w / 2, bottom - 0.2, { align: "center" })
}

function drawLabelBorder(doc: import("jspdf").jsPDF, x: number, y: number, w: number, h: number) {
  doc.setDrawColor(COLOR_BORDER.r, COLOR_BORDER.g, COLOR_BORDER.b)
  doc.setLineWidth(0.12)
  doc.rect(x, y, w, h)
}

/** Formato B — estándar 3×8 */
function drawLabelB(
  doc: import("jspdf").jsPDF,
  item: LabelPrintItem,
  x: number,
  y: number,
  w: number,
  h: number,
  options: LabelPrintOptions,
  logo: PdfLogoPayload,
  logoState: PdfLogoDocState,
  barcodeImg: string | null,
) {
  const pad = 1.6
  const innerX = x + pad
  const innerW = w - pad * 2
  let cy = y + pad

  drawLabelBorder(doc, x, y, w, h)
  cy += drawLogo(doc, logo, logoState, innerX, cy, innerW, h, 0.15)

  if (options.showProductType && item.productType) {
    doc.setFontSize(7)
    doc.setFont("helvetica", "bold")
    doc.setTextColor(COLOR_CATEGORY.r, COLOR_CATEGORY.g, COLOR_CATEGORY.b)
    doc.text(item.productType.toUpperCase(), innerX, cy + 2.2, { maxWidth: innerW })
    cy += 3.2
  }

  doc.setFontSize(8.5)
  doc.setFont("helvetica", "bold")
  doc.setTextColor(0, 0, 0)
  const productLines = doc.splitTextToSize(item.productName, innerW).slice(0, 2)
  doc.text(productLines, innerX, cy + 2.5)
  cy += productLines.length * 3.2 + 0.4

  const variant =
    item.variantName && item.variantName.trim().toLowerCase() !== item.productName.trim().toLowerCase()
      ? item.variantName
      : ""
  if (variant) {
    doc.setFontSize(7)
    doc.setFont("helvetica", "normal")
    doc.setTextColor(COLOR_VARIANT.r, COLOR_VARIANT.g, COLOR_VARIANT.b)
    doc.text(doc.splitTextToSize(variant, innerW).slice(0, 1), innerX, cy + 2)
    cy += 3.2
  }

  if (options.showPrice) {
    doc.setFontSize(13.5)
    doc.setFont("helvetica", "bold")
    doc.setTextColor(0, 0, 0)
    doc.text(formatClp(item.price), x + w / 2, cy + 4.5, { align: "center" })
    cy += 6
  }

  drawBarcodeBlock(doc, item, x, y, w, h, pad, barcodeImg, options.showBarcode, 0.25, 6)
}

/** Formato A — económico 3×10 */
function drawLabelA(
  doc: import("jspdf").jsPDF,
  item: LabelPrintItem,
  x: number,
  y: number,
  w: number,
  h: number,
  options: LabelPrintOptions,
  logo: PdfLogoPayload,
  logoState: PdfLogoDocState,
  barcodeImg: string | null,
) {
  const pad = 1.1
  const innerX = x + pad
  const innerW = w - pad * 2
  let cy = y + pad

  drawLabelBorder(doc, x, y, w, h)
  cy += drawLogo(doc, logo, logoState, innerX, cy, innerW, h, 0.11)

  const line = abbreviate(
    [item.productType, item.productName].filter(Boolean).join(" · "),
    28,
  )
  doc.setFontSize(6)
  doc.setFont("helvetica", "bold")
  doc.setTextColor(0, 0, 0)
  doc.text(doc.splitTextToSize(line, innerW).slice(0, 1), innerX, cy + 1.8)
  cy += 2.8

  if (options.showPrice) {
    doc.setFontSize(9.5)
    doc.setFont("helvetica", "bold")
    doc.text(formatClp(item.price), x + w / 2, cy + 3, { align: "center" })
    cy += 4
  }

  drawBarcodeBlock(doc, item, x, y, w, h, pad, barcodeImg, options.showBarcode, 0.22, 5)
}

/** Formato C — oferta 2×6 */
function drawLabelC(
  doc: import("jspdf").jsPDF,
  item: LabelPrintItem,
  x: number,
  y: number,
  w: number,
  h: number,
  options: LabelPrintOptions,
  logo: PdfLogoPayload,
  logoState: PdfLogoDocState,
  barcodeImg: string | null,
) {
  const pad = 2.2
  const innerX = x + pad
  const innerW = w - pad * 2
  let cy = y + pad

  drawLabelBorder(doc, x, y, w, h)
  cy += drawLogo(doc, logo, logoState, innerX, cy, innerW, h, 0.12)

  doc.setFillColor(COLOR_OFFER.r, COLOR_OFFER.g, COLOR_OFFER.b)
  doc.roundedRect(innerX, cy, innerW, 5.5, 0.8, 0.8, "F")
  doc.setFontSize(9)
  doc.setFont("helvetica", "bold")
  doc.setTextColor(255, 255, 255)
  doc.text("OFERTA", x + w / 2, cy + 3.8, { align: "center" })
  cy += 6.5

  if (options.showProductType && item.productType) {
    doc.setFontSize(7.5)
    doc.setFont("helvetica", "bold")
    doc.setTextColor(COLOR_CATEGORY.r, COLOR_CATEGORY.g, COLOR_CATEGORY.b)
    doc.text(item.productType.toUpperCase(), innerX, cy + 2.2, { maxWidth: innerW })
    cy += 3.5
  }

  doc.setFontSize(10)
  doc.setFont("helvetica", "bold")
  doc.setTextColor(0, 0, 0)
  const productLines = doc.splitTextToSize(item.productName, innerW).slice(0, 2)
  doc.text(productLines, innerX, cy + 2.5)
  cy += productLines.length * 3.6 + 0.3

  const variant =
    item.variantName && item.variantName.trim().toLowerCase() !== item.productName.trim().toLowerCase()
      ? item.variantName
      : ""
  if (variant) {
    doc.setFontSize(8)
    doc.setFont("helvetica", "normal")
    doc.setTextColor(COLOR_VARIANT.r, COLOR_VARIANT.g, COLOR_VARIANT.b)
    doc.text(doc.splitTextToSize(variant, innerW).slice(0, 1), innerX, cy + 2)
    cy += 3.5
  }

  if (options.showPrice) {
    const regular = effectiveRegularPrice(item)
    const sale = effectiveSalePrice(item)
    if (regular != null && sale != null && regular > sale) {
      doc.setFontSize(8)
      doc.setFont("helvetica", "normal")
      doc.setTextColor(120, 120, 120)
      const antes = `ANTES ${formatClp(regular)}`
      doc.text(antes, x + w / 2, cy + 2.5, { align: "center" })
      const tw = doc.getTextWidth(antes)
      doc.setDrawColor(120, 120, 120)
      doc.setLineWidth(0.25)
      doc.line(x + w / 2 - tw / 2, cy + 2.2, x + w / 2 + tw / 2, cy + 2.2)
      cy += 4
      doc.setFontSize(15)
      doc.setFont("helvetica", "bold")
      doc.setTextColor(COLOR_OFFER.r, COLOR_OFFER.g, COLOR_OFFER.b)
      doc.text(`AHORA ${formatClp(sale)}`, x + w / 2, cy + 4.5, { align: "center" })
      cy += 6
    } else {
      doc.setFontSize(15)
      doc.setFont("helvetica", "bold")
      doc.setTextColor(COLOR_OFFER.r, COLOR_OFFER.g, COLOR_OFFER.b)
      doc.text(formatClp(sale), x + w / 2, cy + 4.5, { align: "center" })
      cy += 6
    }
  }

  drawBarcodeBlock(doc, item, x, y, w, h, pad, barcodeImg, options.showBarcode, 0.24, 7)
}

function drawLabel(
  doc: import("jspdf").jsPDF,
  item: LabelPrintItem,
  x: number,
  y: number,
  w: number,
  h: number,
  format: LabelFormat,
  options: LabelPrintOptions,
  logo: PdfLogoPayload,
  logoState: PdfLogoDocState,
  barcodeImg: string | null,
) {
  if (format === "A") {
    drawLabelA(doc, item, x, y, w, h, options, logo, logoState, barcodeImg)
  } else if (format === "C") {
    drawLabelC(doc, item, x, y, w, h, options, logo, logoState, barcodeImg)
  } else {
    drawLabelB(doc, item, x, y, w, h, options, logo, logoState, barcodeImg)
  }
}

export function estimateLabelPages(totalLabels: number, format: LabelFormat): number {
  const perPage = LABEL_FORMATS[format].perPage
  return totalLabels > 0 ? Math.ceil(totalLabels / perPage) : 0
}

export async function generateLabelsPdf(
  items: LabelPrintItem[],
  format: LabelFormat,
  options: LabelPrintOptions,
  filename = "etiquetas-sucursal.pdf",
): Promise<void> {
  const flat = flattenItems(items)
  if (flat.length === 0) return

  const logo = await loadQuillotanaLogoForPdf()
  if (!logo) {
    throw new Error("No se pudo cargar el logo Quillotana")
  }

  const { jsPDF } = await import("jspdf")
  const doc = new jsPDF({ orientation: "portrait", unit: "mm", format: "letter" })
  const grid = labelGrid(format)
  const logoState = createPdfLogoDocState()

  const barcodeCache = new Map<string, string | null>()
  const bcSpec =
    format === "C"
      ? { w: 320, h: 64, bar: 1.4 }
      : format === "B"
        ? { w: 260, h: 52, bar: 1.25 }
        : { w: 200, h: 36, bar: 1.0 }

  for (let i = 0; i < flat.length; i++) {
    const posOnPage = i % (grid.cols * grid.rows)
    const col = posOnPage % grid.cols
    const row = Math.floor(posOnPage / grid.cols)

    if (i > 0 && posOnPage === 0) doc.addPage()

    const item = flat[i]
    if (!barcodeCache.has(item.barcode)) {
      barcodeCache.set(
        item.barcode,
        await barcodeDataUrl(item.barcode, bcSpec.w, bcSpec.h, bcSpec.bar),
      )
    }

    drawLabel(
      doc,
      item,
      MARGIN_MM + col * grid.labelW,
      MARGIN_MM + row * grid.labelH,
      grid.labelW,
      grid.labelH,
      format,
      options,
      logo,
      logoState,
      barcodeCache.get(item.barcode) ?? null,
    )
  }

  doc.save(filename)
}
