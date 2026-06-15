/**
 * PDF carta — etiquetas de precio para sucursales (Formatos A / B / C).
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
  showLogo: boolean
  showProductType: boolean
  showBarcode: boolean
  showPrice: boolean
  companyName?: string
}

export type LabelPrintItem = {
  barcode: string
  productType: string
  productName: string
  variantName: string
  price: number | null
  isOffer: boolean
  quantity: number
}

export const LABEL_FORMATS: Record<
  LabelFormat,
  { id: LabelFormat; label: string; cols: number; rows: number; perPage: number }
> = {
  A: { id: "A", label: "Pequeño (30/hoja)", cols: 3, rows: 10, perPage: 30 },
  B: { id: "B", label: "Mediano (24/hoja)", cols: 3, rows: 8, perPage: 24 },
  C: { id: "C", label: "Premium / oferta (12/hoja)", cols: 2, rows: 6, perPage: 12 },
}

const MARGIN_MM = 4
const PAGE_W = 215.9
const PAGE_H = 279.4

function formatClp(value: number | null): string {
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

/** Genera imagen de código de barras (canvas → data URL). */
async function barcodeDataUrl(code: string, widthPx: number, heightPx: number): Promise<string | null> {
  if (typeof window === "undefined" || !code.trim()) return null
  try {
    const JsBarcode = (await import("jsbarcode")).default
    const canvas = document.createElement("canvas")
    const fmt = /^\d{12,13}$/.test(code.trim()) ? "EAN13" : "CODE128"
    JsBarcode(canvas, code.trim(), {
      format: fmt,
      width: 1.2,
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
  return {
    cols,
    rows,
    labelW: usableW / cols,
    labelH: usableH / rows,
  }
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
  logo: PdfLogoPayload | null,
  logoState: PdfLogoDocState,
  barcodeImg: string | null,
) {
  const pad = format === "C" ? 2.5 : format === "B" ? 1.8 : 1.2
  const innerX = x + pad
  const innerY = y + pad
  const innerW = w - pad * 2
  let cursorY = innerY

  if (item.isOffer) {
    doc.setFillColor(220, 38, 38)
    doc.rect(x + 0.5, y + 0.5, w - 1, 4.5, "F")
    doc.setFontSize(format === "C" ? 8 : 6)
    doc.setTextColor(255, 255, 255)
    doc.setFont("helvetica", "bold")
    doc.text("OFERTA", x + w / 2, y + 3.2, { align: "center" })
    cursorY += 4
  }

  doc.setDrawColor(180, 180, 180)
  doc.setLineWidth(0.15)
  doc.rect(x, y, w, h)

  if (options.showLogo && logo) {
    const logoW = format === "C" ? 22 : format === "B" ? 16 : 12
    const logoH = logoW / logo.aspectRatio
    drawQuillotanaLogoOnPdf(doc, logo, logoState, innerX, cursorY, logoW)
    if (options.companyName) {
      doc.setFontSize(format === "C" ? 6 : 4.5)
      doc.setTextColor(80, 80, 80)
      doc.setFont("helvetica", "normal")
      doc.text(options.companyName, innerX + logoW + 1.5, cursorY + logoH * 0.55, {
        maxWidth: innerW - logoW - 2,
      })
    }
    cursorY += logoH + 1
  } else if (options.companyName) {
    doc.setFontSize(format === "C" ? 7 : 5)
    doc.setTextColor(60, 60, 60)
    doc.setFont("helvetica", "bold")
    doc.text(options.companyName, innerX, cursorY + 2, { maxWidth: innerW })
    cursorY += 4
  }

  if (options.showProductType && item.productType) {
    doc.setFontSize(format === "C" ? 7 : format === "B" ? 6 : 5)
    doc.setTextColor(100, 100, 100)
    doc.setFont("helvetica", "normal")
    doc.text(item.productType.toUpperCase(), innerX, cursorY + 2, { maxWidth: innerW })
    cursorY += format === "C" ? 4 : 3
  }

  doc.setFontSize(format === "C" ? 9 : format === "B" ? 7.5 : 6.5)
  doc.setTextColor(0, 0, 0)
  doc.setFont("helvetica", "bold")
  const nameLines = doc.splitTextToSize(item.productName, innerW)
  doc.text(nameLines.slice(0, format === "C" ? 2 : 1), innerX, cursorY + 2)
  cursorY += (Math.min(nameLines.length, format === "C" ? 2 : 1)) * (format === "C" ? 4 : 3.2) + 0.5

  if (item.variantName && item.variantName !== item.productName) {
    doc.setFontSize(format === "C" ? 7 : 6)
    doc.setFont("helvetica", "normal")
    doc.setTextColor(50, 50, 50)
    const varLines = doc.splitTextToSize(item.variantName, innerW)
    doc.text(varLines.slice(0, 1), innerX, cursorY + 2)
    cursorY += 3.5
  }

  if (options.showPrice) {
    doc.setFontSize(format === "C" ? 14 : format === "B" ? 11 : 9)
    doc.setFont("helvetica", "bold")
    doc.setTextColor(item.isOffer ? 200 : 0, item.isOffer ? 30 : 0, item.isOffer ? 30 : 0)
    doc.text(formatClp(item.price), innerX, cursorY + (format === "C" ? 5 : 4))
    cursorY += format === "C" ? 7 : 5.5
  }

  const barcodeAreaH = format === "C" ? 14 : format === "B" ? 10 : 8
  const barcodeBottom = y + h - pad

  if (options.showBarcode && barcodeImg) {
    const imgY = barcodeBottom - barcodeAreaH
    try {
      doc.addImage(barcodeImg, "PNG", innerX, imgY, innerW, barcodeAreaH - 2)
    } catch {
      /* fallback texto abajo */
    }
  }

  doc.setFontSize(format === "C" ? 7 : 5.5)
  doc.setFont("helvetica", "normal")
  doc.setTextColor(30, 30, 30)
  doc.text(item.barcode, x + w / 2, barcodeBottom - 0.5, { align: "center" })
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

  const { jsPDF } = await import("jspdf")
  const doc = new jsPDF({ orientation: "portrait", unit: "mm", format: "letter" })
  const grid = labelGrid(format)
  const logo = options.showLogo ? await loadQuillotanaLogoForPdf() : null
  const logoState = createPdfLogoDocState()

  const barcodeCache = new Map<string, string | null>()
  const bcWidth = format === "C" ? 280 : format === "B" ? 220 : 180
  const bcHeight = format === "C" ? 56 : format === "B" ? 44 : 36

  for (let i = 0; i < flat.length; i++) {
    const pageIdx = Math.floor(i / grid.cols / grid.rows)
    const posOnPage = i % (grid.cols * grid.rows)
    const col = posOnPage % grid.cols
    const row = Math.floor(posOnPage / grid.cols)

    if (i > 0 && posOnPage === 0) {
      doc.addPage()
    }

    const item = flat[i]
    if (!barcodeCache.has(item.barcode)) {
      barcodeCache.set(item.barcode, await barcodeDataUrl(item.barcode, bcWidth, bcHeight))
    }

    const x = MARGIN_MM + col * grid.labelW
    const y = MARGIN_MM + row * grid.labelH

    drawLabel(
      doc,
      item,
      x,
      y,
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
