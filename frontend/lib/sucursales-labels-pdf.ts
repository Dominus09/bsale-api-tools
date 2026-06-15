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
  price: number | null
  sale_price?: number | null
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

const COLOR_CATEGORY = { r: 110, g: 110, b: 110 }
const COLOR_VARIANT = { r: 55, g: 55, b: 55 }
const COLOR_OFFER = { r: 220, g: 38, b: 38 }
const COLOR_BORDER = { r: 190, g: 190, b: 190 }

type BarcodeSpec = { w: number; h: number; bar: number; minMm: number }

const BARCODE_SPECS: Record<LabelFormat, BarcodeSpec> = {
  A: { w: 220, h: 44, bar: 1.15, minMm: 5.5 },
  B: { w: 280, h: 58, bar: 1.45, minMm: 7.5 },
  C: { w: 340, h: 72, bar: 1.55, minMm: 9 },
}

function formatClp(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "—"
  return new Intl.NumberFormat("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  }).format(value)
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

/** Máx. líneas con truncado elegante en la última. */
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

function displayNameA(item: LabelPrintItem): string {
  const variant =
    item.variantName &&
    item.variantName.trim().toLowerCase() !== item.productName.trim().toLowerCase()
      ? item.variantName.trim()
      : ""
  return [item.productName, variant].filter(Boolean).join(" ")
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
      margin: 2,
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
    return scaled.toDataURL("image/jpeg", 0.9)
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
  const logoW = Math.min(innerW * 0.62, logoH * logo.aspectRatio)
  drawQuillotanaLogoOnPdf(doc, logo, logoState, x, y, logoW)
  return logoH + 0.5
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
  zoneRatio: number,
  spec: BarcodeSpec,
  numFontSize: number,
) {
  if (!showBarcode) return
  const zoneH = Math.max(h * zoneRatio, spec.minMm + numFontSize * 0.45)
  const bottom = y + h - pad
  const zoneTop = bottom - zoneH
  const numY = bottom - 0.25
  const bcH = Math.max(spec.minMm, zoneH - numFontSize * 0.5 - 0.8)
  const bcY = zoneTop + 0.3

  if (barcodeImg) {
    try {
      doc.addImage(barcodeImg, "JPEG", x + pad, bcY, w - pad * 2, bcH, undefined, "FAST")
    } catch {
      /* fallback numérico */
    }
  }

  doc.setFontSize(numFontSize)
  doc.setFont("helvetica", "normal")
  doc.setTextColor(20, 20, 20)
  doc.text(item.barcode, x + w / 2, numY, { align: "center" })
}

function drawLabelBorder(doc: import("jspdf").jsPDF, x: number, y: number, w: number, h: number) {
  doc.setDrawColor(COLOR_BORDER.r, COLOR_BORDER.g, COLOR_BORDER.b)
  doc.setLineWidth(0.1)
  doc.rect(x, y, w, h)
}

/** Formato B — precio dominante, categoría discreta */
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
  spec: BarcodeSpec,
) {
  const pad = 1.5
  const innerX = x + pad
  const innerW = w - pad * 2
  const barcodeZone = 0.27
  const contentMaxY = y + h - pad - h * barcodeZone
  let cy = y + pad

  drawLabelBorder(doc, x, y, w, h)
  cy += drawLogo(doc, logo, logoState, innerX, cy, innerW, h, 0.13)

  if (options.showProductType && item.productType) {
    doc.setFontSize(5.5)
    doc.setFont("helvetica", "normal")
    doc.setTextColor(COLOR_CATEGORY.r, COLOR_CATEGORY.g, COLOR_CATEGORY.b)
    doc.text(item.productType.toUpperCase(), innerX, cy + 1.8, { maxWidth: innerW })
    cy += 2.6
  }

  doc.setFont("helvetica", "bold")
  doc.setTextColor(0, 0, 0)
  const productLines = fitLines(doc, item.productName, innerW, 2, 7.8)
  doc.setFontSize(7.8)
  doc.text(productLines, innerX, cy + 2.2)
  cy += productLines.length * 2.9 + 0.3

  const variant =
    item.variantName &&
    item.variantName.trim().toLowerCase() !== item.productName.trim().toLowerCase()
      ? item.variantName
      : ""
  if (variant) {
    doc.setFontSize(6.5)
    doc.setFont("helvetica", "normal")
    doc.setTextColor(COLOR_VARIANT.r, COLOR_VARIANT.g, COLOR_VARIANT.b)
    doc.text(fitLines(doc, variant, innerW, 1, 6.5), innerX, cy + 2)
    cy += 2.8
  }

  if (options.showPrice) {
    const priceY = cy + (contentMaxY - cy) * 0.55
    doc.setFontSize(16)
    doc.setFont("helvetica", "bold")
    doc.setTextColor(0, 0, 0)
    doc.text(formatClp(item.price), x + w / 2, priceY, { align: "center" })
  }

  drawBarcodeBlock(doc, item, x, y, w, h, pad, barcodeImg, options.showBarcode, barcodeZone, spec, 6)
}

/** Formato A — 2 líneas máx. con truncado */
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
  spec: BarcodeSpec,
) {
  const pad = 1
  const innerX = x + pad
  const innerW = w - pad * 2
  const barcodeZone = 0.24
  let cy = y + pad

  drawLabelBorder(doc, x, y, w, h)
  cy += drawLogo(doc, logo, logoState, innerX, cy, innerW, h, 0.1)

  doc.setFont("helvetica", "bold")
  doc.setTextColor(0, 0, 0)
  const nameLines = fitLines(doc, displayNameA(item), innerW, 2, 5.5)
  doc.setFontSize(5.5)
  doc.text(nameLines, innerX, cy + 1.6)
  cy += nameLines.length * 2.4 + 0.4

  if (options.showPrice) {
    doc.setFontSize(9)
    doc.setFont("helvetica", "bold")
    doc.text(formatClp(item.price), x + w / 2, cy + 2.8, { align: "center" })
  }

  drawBarcodeBlock(doc, item, x, y, w, h, pad, barcodeImg, options.showBarcode, barcodeZone, spec, 5)
}

/** Formato C — banda OFERTA + ANTES/AHORA */
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
  spec: BarcodeSpec,
) {
  const pad = 2
  const innerX = x + pad
  const innerW = w - pad * 2
  const barcodeZone = 0.25
  let cy = y + pad

  drawLabelBorder(doc, x, y, w, h)
  cy += drawLogo(doc, logo, logoState, innerX, cy, innerW, h, 0.11)

  doc.setFillColor(COLOR_OFFER.r, COLOR_OFFER.g, COLOR_OFFER.b)
  doc.roundedRect(innerX, cy, innerW, 5, 0.6, 0.6, "F")
  doc.setFontSize(8.5)
  doc.setFont("helvetica", "bold")
  doc.setTextColor(255, 255, 255)
  doc.text("OFERTA", x + w / 2, cy + 3.5, { align: "center" })
  cy += 6

  if (options.showProductType && item.productType) {
    doc.setFontSize(6.5)
    doc.setFont("helvetica", "normal")
    doc.setTextColor(COLOR_CATEGORY.r, COLOR_CATEGORY.g, COLOR_CATEGORY.b)
    doc.text(item.productType.toUpperCase(), innerX, cy + 2, { maxWidth: innerW })
    cy += 2.8
  }

  doc.setFont("helvetica", "bold")
  doc.setTextColor(0, 0, 0)
  const productLines = fitLines(doc, item.productName, innerW, 2, 9)
  doc.setFontSize(9)
  doc.text(productLines, innerX, cy + 2.2)
  cy += productLines.length * 3.2 + 0.2

  const variant =
    item.variantName &&
    item.variantName.trim().toLowerCase() !== item.productName.trim().toLowerCase()
      ? item.variantName
      : ""
  if (variant) {
    doc.setFontSize(7.5)
    doc.setFont("helvetica", "normal")
    doc.setTextColor(COLOR_VARIANT.r, COLOR_VARIANT.g, COLOR_VARIANT.b)
    doc.text(fitLines(doc, variant, innerW, 1, 7.5), innerX, cy + 2)
    cy += 3
  }

  if (options.showPrice) {
    const regular = effectiveRegularPrice(item)
    const sale = effectiveSalePrice(item)
    if (regular != null && sale != null) {
      doc.setFontSize(7.5)
      doc.setFont("helvetica", "normal")
      doc.setTextColor(120, 120, 120)
      const antes = `ANTES ${formatClp(regular)}`
      doc.text(antes, x + w / 2, cy + 2.2, { align: "center" })
      const tw = doc.getTextWidth(antes)
      doc.setDrawColor(140, 140, 140)
      doc.setLineWidth(0.2)
      doc.line(x + w / 2 - tw / 2, cy + 1.9, x + w / 2 + tw / 2, cy + 1.9)
      cy += 3.8
      doc.setFontSize(14.5)
      doc.setFont("helvetica", "bold")
      doc.setTextColor(COLOR_OFFER.r, COLOR_OFFER.g, COLOR_OFFER.b)
      doc.text(`AHORA ${formatClp(sale)}`, x + w / 2, cy + 4, { align: "center" })
    } else {
      doc.setFontSize(14.5)
      doc.setFont("helvetica", "bold")
      doc.setTextColor(COLOR_OFFER.r, COLOR_OFFER.g, COLOR_OFFER.b)
      doc.text(formatClp(sale), x + w / 2, cy + 4, { align: "center" })
    }
  }

  drawBarcodeBlock(doc, item, x, y, w, h, pad, barcodeImg, options.showBarcode, barcodeZone, spec, 7)
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
  spec: BarcodeSpec,
) {
  if (format === "A") {
    drawLabelA(doc, item, x, y, w, h, options, logo, logoState, barcodeImg, spec)
  } else if (format === "C") {
    drawLabelC(doc, item, x, y, w, h, options, logo, logoState, barcodeImg, spec)
  } else {
    drawLabelB(doc, item, x, y, w, h, options, logo, logoState, barcodeImg, spec)
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

  const spec = BARCODE_SPECS[format]
  const barcodeCache = await buildBarcodeCache(
    flat.map((it) => it.barcode),
    spec,
  )

  const { jsPDF } = await import("jspdf")
  const doc = new jsPDF({
    orientation: "portrait",
    unit: "mm",
    format: "letter",
    compress: true,
  })
  const grid = labelGrid(format)
  const logoState = createPdfLogoDocState()
  const perPage = grid.cols * grid.rows

  for (let i = 0; i < flat.length; i++) {
    const posOnPage = i % perPage
    if (i > 0 && posOnPage === 0) doc.addPage()

    const col = posOnPage % grid.cols
    const row = Math.floor(posOnPage / grid.cols)
    const item = flat[i]

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
      barcodeCache.get(item.barcode.trim()) ?? null,
      spec,
    )
  }

  doc.save(filename)
}
