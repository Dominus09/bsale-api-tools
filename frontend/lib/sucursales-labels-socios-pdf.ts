/**
 * PDF carta — Etiquetas Socios V2 (formato Socio Estándar ≈ 24/hoja).
 * Precio Normal (negro, menor) + Precio Socio (azul, dominante).
 * Sin porcentajes de descuento. "Provisional" nunca se imprime.
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

export const SOCIO_ESTANDAR_FORMAT = {
  id: "SOCIO_ESTANDAR" as const,
  label: "Socio Estándar",
  description: "24 etiquetas · Precio Normal + Precio Socio",
  cols: 3,
  rows: 8,
  perPage: 24,
}

const MARGIN_MM = 2.5
const PAGE_W = 215.9
const PAGE_H = 279.4

const COLOR_CATEGORY = { r: 110, g: 110, b: 110 }
const COLOR_VARIANT = { r: 55, g: 55, b: 55 }
const COLOR_BORDER = { r: 190, g: 190, b: 190 }
/** Azul institucional Quillotana (Precio Socio) */
const COLOR_SOCIO = { r: 0, g: 90, b: 168 }
const COLOR_SOCIO_BOX = { r: 230, g: 242, b: 255 }

type BarcodeSpec = { w: number; h: number; bar: number; minMm: number }

const BARCODE_SPEC: BarcodeSpec = { w: 280, h: 58, bar: 1.45, minMm: 7.5 }

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

function labelGrid() {
  const { cols, rows } = SOCIO_ESTANDAR_FORMAT
  const usableW = PAGE_W - MARGIN_MM * 2
  const usableH = PAGE_H - MARGIN_MM * 2
  return { cols, rows, labelW: usableW / cols, labelH: usableH / rows }
}

const LOGO_SIZE_BOOST = 1.15

function drawLogo(
  doc: import("jspdf").jsPDF,
  logo: PdfLogoPayload,
  x: number,
  y: number,
  innerW: number,
  labelH: number,
  ratio: number,
): number {
  const logoH = labelH * ratio * LOGO_SIZE_BOOST
  const logoW = Math.min(innerW * 0.72, logoH * logo.aspectRatio)
  const heightMm = logoW / logo.aspectRatio
  doc.addImage(logo.dataUrl, logo.format, x, y, logoW, heightMm, undefined, "FAST")
  return heightMm + 0.35
}

function drawBarcodeBlock(
  doc: import("jspdf").jsPDF,
  item: SocioLabelPrintItem,
  x: number,
  y: number,
  w: number,
  h: number,
  pad: number,
  barcodeImg: string | null,
  showBarcode: boolean,
) {
  if (!showBarcode) return
  const zoneRatio = 0.24
  const zoneH = Math.max(h * zoneRatio, BARCODE_SPEC.minMm + 3)
  const bottom = y + h - pad
  const zoneTop = bottom - zoneH
  const numY = bottom - 0.25
  const bcH = Math.max(BARCODE_SPEC.minMm, zoneH - 4)
  const bcY = zoneTop + 0.25

  if (barcodeImg) {
    try {
      doc.addImage(barcodeImg, "JPEG", x + pad, bcY, w - pad * 2, bcH, undefined, "FAST")
    } catch {
      /* fallback numérico */
    }
  }

  doc.setFontSize(5.5)
  doc.setFont("helvetica", "normal")
  doc.setTextColor(20, 20, 20)
  doc.text(item.barcode, x + w / 2, numY, { align: "center" })
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
  const pad = 1.4
  const innerX = x + pad
  const innerW = w - pad * 2
  let cy = y + pad

  doc.setDrawColor(COLOR_BORDER.r, COLOR_BORDER.g, COLOR_BORDER.b)
  doc.setLineWidth(0.1)
  doc.rect(x, y, w, h)

  cy += drawLogo(doc, logo, innerX, cy, innerW, h, 0.11)

  if (options.showProductType && item.productType) {
    doc.setFontSize(5)
    doc.setFont("helvetica", "normal")
    doc.setTextColor(COLOR_CATEGORY.r, COLOR_CATEGORY.g, COLOR_CATEGORY.b)
    doc.text(item.productType.toUpperCase(), innerX, cy + 1.6, { maxWidth: innerW })
    cy += 2.3
  }

  doc.setFont("helvetica", "bold")
  doc.setTextColor(0, 0, 0)
  const productLines = fitLines(doc, item.productName, innerW, 2, 7)
  doc.setFontSize(7)
  doc.text(productLines, innerX, cy + 2)
  cy += productLines.length * 2.6 + 0.2

  const variant =
    item.variantName &&
    item.variantName.trim().toLowerCase() !== item.productName.trim().toLowerCase()
      ? item.variantName
      : ""
  if (variant) {
    doc.setFontSize(5.8)
    doc.setFont("helvetica", "normal")
    doc.setTextColor(COLOR_VARIANT.r, COLOR_VARIANT.g, COLOR_VARIANT.b)
    doc.text(fitLines(doc, variant, innerW, 1, 5.8), innerX, cy + 1.8)
    cy += 2.4
  }

  if (options.showPrices) {
    // Precio Normal — negro, menor
    doc.setFontSize(5)
    doc.setFont("helvetica", "normal")
    doc.setTextColor(80, 80, 80)
    doc.text("PRECIO NORMAL", x + w / 2, cy + 1.6, { align: "center" })
    cy += 2.4
    doc.setFontSize(8)
    doc.setFont("helvetica", "bold")
    doc.setTextColor(0, 0, 0)
    doc.text(formatClp(item.normalPrice), x + w / 2, cy + 2.2, { align: "center" })
    cy += 3.2

    // Caja Precio Socio — azul dominante
    const boxH = 9.5
    const boxY = cy
    doc.setFillColor(COLOR_SOCIO_BOX.r, COLOR_SOCIO_BOX.g, COLOR_SOCIO_BOX.b)
    doc.setDrawColor(COLOR_SOCIO.r, COLOR_SOCIO.g, COLOR_SOCIO.b)
    doc.setLineWidth(0.35)
    doc.roundedRect(innerX, boxY, innerW, boxH, 0.8, 0.8, "FD")

    doc.setFontSize(5.2)
    doc.setFont("helvetica", "bold")
    doc.setTextColor(COLOR_SOCIO.r, COLOR_SOCIO.g, COLOR_SOCIO.b)
    doc.text("PRECIO SOCIO", x + w / 2, boxY + 2.8, { align: "center" })

    doc.setFontSize(12.5)
    doc.setFont("helvetica", "bold")
    doc.setTextColor(COLOR_SOCIO.r, COLOR_SOCIO.g, COLOR_SOCIO.b)
    doc.text(formatClp(item.socioPrice), x + w / 2, boxY + 7.6, { align: "center" })
  }

  drawBarcodeBlock(
    doc,
    item,
    x,
    y,
    w,
    h,
    pad,
    barcodeImg,
    options.showBarcode,
  )
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
  const grid = labelGrid()
  const perPage = grid.cols * grid.rows

  for (let i = 0; i < flat.length; i++) {
    const posOnPage = i % perPage
    if (i > 0 && posOnPage === 0) doc.addPage()

    const col = posOnPage % grid.cols
    const row = Math.floor(posOnPage / grid.cols)
    const item = flat[i]

    drawSocioEstandarLabel(
      doc,
      item,
      MARGIN_MM + col * grid.labelW,
      MARGIN_MM + row * grid.labelH,
      grid.labelW,
      grid.labelH,
      options,
      logo,
      barcodeCache.get(item.barcode.trim()) ?? null,
    )
  }

  doc.save(filename)
}
