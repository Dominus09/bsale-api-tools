/**
 * PDF — Etiquetas Socios V2 · Socio Estándar 100×40 mm
 * Mockup aprobado: encabezado retail + Normal/Socio con PPUM + Cód. textual (sin barras).
 */

import {
  loadQuillotanaLogoForPdf,
  type PdfLogoPayload,
} from "@/lib/quillotana-logo-pdf"
import { resolvePpumLabel } from "@/lib/etiquetas2-ppum"

export type SocioLabelPrintOptions = {
  showProductType: boolean
  /** Muestra el código textual (Cód: …). Ya no dibuja barcode gráfico. */
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
  description: "12 etiquetas · 10×4 cm · góndola retail + PPUM",
  cols: 2,
  rows: 6,
  perPage: 12,
  labelWMm: 100,
  labelHMm: 40,
}

const PAGE_W = 215.9
const PAGE_H = 279.4
const MARGIN_MM = 5

/** Franjas (mm) — total 40 */
const BAND_PRODUCT_H = 13.2
const BAND_PRICES_H = 20.0
// footer código ≈ 6.8

const C = {
  category: { r: 140, g: 140, b: 140 },
  variant: { r: 90, g: 90, b: 90 },
  border: { r: 222, g: 222, b: 222 },
  ink: { r: 18, g: 18, b: 18 },
  normalLabel: { r: 120, g: 120, b: 120 },
  ppum: { r: 110, g: 110, b: 110 },
  socio: { r: 0, g: 90, b: 168 },
  socioSoft: { r: 244, g: 249, b: 253 },
  code: { r: 100, g: 100, b: 100 },
}

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
  const lines = doc.splitTextToSize(String(text || "").trim(), maxWidth) as string[]
  if (lines.length <= maxLines) return lines
  const kept = lines.slice(0, maxLines)
  let last = kept[maxLines - 1] ?? ""
  while (last.length > 4 && doc.getTextWidth(`${last}…`) > maxWidth) {
    last = last.slice(0, -1)
  }
  kept[maxLines - 1] = `${last.replace(/\s+$/, "")}…`
  return kept
}

function presentationLine(item: SocioLabelPrintItem): string {
  const variant = String(item.variantName || "").trim()
  const name = String(item.productName || "").trim()
  if (!variant) return ""
  if (variant.toLowerCase() === name.toLowerCase()) return ""
  return variant
}

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
) {
  const padX = 3.0
  const padY = 1.6
  const innerX = x + padX
  const innerW = w - padX * 2
  const productBottom = y + BAND_PRODUCT_H

  doc.setDrawColor(C.border.r, C.border.g, C.border.b)
  doc.setLineWidth(0.12)
  doc.setFillColor(255, 255, 255)
  doc.roundedRect(x, y, w, h, 0.7, 0.7, "FD")

  // ─── PRODUCTO ───
  const band1Top = y + padY
  const logoW = 19
  const logoH = Math.min(logoW / logo.aspectRatio, BAND_PRODUCT_H - 2.2)
  const logoY = band1Top + Math.max(0, (BAND_PRODUCT_H - padY - logoH) / 2 - 0.2)
  doc.addImage(
    logo.dataUrl,
    logo.format,
    innerX,
    logoY,
    logoW,
    logoH,
    undefined,
    "FAST",
  )

  const textX = innerX + logoW + 2.6
  const textW = innerW - logoW - 2.6
  const maxTextY = productBottom - 0.8

  let ty = band1Top + 2.0
  const showCat = options.showProductType && Boolean(item.productType?.trim())
  const presentation = presentationLine(item)

  // Reservar espacio: categoría (opcional) → nombre (prioridad) → presentación
  const catH = showCat ? 2.0 : 0
  const presH = presentation ? 2.3 : 0
  const nameBudget = maxTextY - ty - catH - (presentation ? 0.3 + presH : 0)

  if (showCat) {
    doc.setFont("helvetica", "normal")
    doc.setFontSize(4.6)
    doc.setTextColor(C.category.r, C.category.g, C.category.b)
    const cat = fitLines(doc, item.productType.toUpperCase(), textW, 1, 4.6)
    doc.text(cat, textX, ty)
    ty += 2.05
  }

  // Nombre: 1 o 2 líneas según espacio; si aprieta, prioriza nombre sobre categoría (ya dibujada)
  let nameSize = 9.6
  let nameLines = fitLines(doc, item.productName, textW, 2, nameSize)
  let nameBlockH = nameLines.length * 3.15
  if (nameBlockH > nameBudget && nameBudget > 0) {
    nameSize = 8.6
    nameLines = fitLines(doc, item.productName, textW, nameBudget >= 5.5 ? 2 : 1, nameSize)
    nameBlockH = nameLines.length * 2.85
  }

  doc.setFont("helvetica", "bold")
  doc.setFontSize(nameSize)
  doc.setTextColor(C.ink.r, C.ink.g, C.ink.b)
  // No sobrepasar franja producto
  if (ty + nameBlockH > maxTextY + 0.4) {
    nameLines = fitLines(doc, item.productName, textW, 1, 8.4)
    doc.setFontSize(8.4)
  }
  doc.text(nameLines, textX, ty)
  ty += nameLines.length * (nameLines.length > 1 ? 3.05 : 3.2)

  if (presentation && ty + 1.6 <= maxTextY + 0.2) {
    doc.setFont("helvetica", "normal")
    doc.setFontSize(5.6)
    doc.setTextColor(C.variant.r, C.variant.g, C.variant.b)
    doc.text(fitLines(doc, presentation, textW, 1, 5.6), textX, Math.min(ty, maxTextY))
  }

  // ─── PRECIOS + PPUM ───
  const band2Top = y + BAND_PRODUCT_H
  const band2H = BAND_PRICES_H

  if (options.showPrices) {
    const normalW = innerW * 0.4
    const socioW = innerW * 0.6
    const socioX = innerX + normalW
    const inset = 1.0

    doc.setFillColor(C.socioSoft.r, C.socioSoft.g, C.socioSoft.b)
    doc.roundedRect(
      socioX + 0.4,
      band2Top + inset,
      socioW - 0.8,
      band2H - inset * 2,
      1.1,
      1.1,
      "F",
    )

    const normalPpum = resolvePpumLabel(
      item.normalPrice,
      item.productName,
      item.variantName,
    )
    const socioPpum = resolvePpumLabel(
      item.socioPrice,
      item.productName,
      item.variantName,
    )

    // Normal — izquierda, alineación izquierda (más retail que centrado)
    const nX = innerX + 0.4
    let ny = band2Top + 3.4
    doc.setFont("helvetica", "normal")
    doc.setFontSize(4.5)
    doc.setTextColor(C.normalLabel.r, C.normalLabel.g, C.normalLabel.b)
    doc.text("PRECIO NORMAL", nX, ny)

    ny += 5.2
    doc.setFont("helvetica", "bold")
    doc.setFontSize(14.5)
    doc.setTextColor(C.ink.r, C.ink.g, C.ink.b)
    doc.text(formatClp(item.normalPrice), nX, ny)

    ny += 4.0
    doc.setFont("helvetica", "normal")
    doc.setFontSize(5.2)
    doc.setTextColor(C.ppum.r, C.ppum.g, C.ppum.b)
    doc.text(fitLines(doc, normalPpum, normalW - 2, 1, 5.2), nX, ny)

    // Socio — derecha, protagonista
    const sPad = 2.2
    const sCx = socioX + socioW / 2
    const pill = "SOCIO QUILLOTANA"
    doc.setFont("helvetica", "bold")
    doc.setFontSize(4.5)
    const pillTw = doc.getTextWidth(pill)
    const pillW = Math.min(socioW - 6, pillTw + 3.8)
    const pillH = 3.3
    const pillX = sCx - pillW / 2
    const pillY = band2Top + 2.4

    doc.setFillColor(C.socio.r, C.socio.g, C.socio.b)
    doc.roundedRect(pillX, pillY, pillW, pillH, pillH / 2, pillH / 2, "F")
    doc.setTextColor(255, 255, 255)
    doc.text(pill, sCx, pillY + 2.35, { align: "center" })

    doc.setFont("helvetica", "bold")
    doc.setFontSize(17)
    doc.setTextColor(C.socio.r, C.socio.g, C.socio.b)
    doc.text(formatClp(item.socioPrice), sCx, pillY + pillH + 6.0, {
      align: "center",
    })

    doc.setFont("helvetica", "normal")
    doc.setFontSize(5.3)
    doc.setTextColor(C.socio.r, C.socio.g, C.socio.b)
    const socioPpumLines = fitLines(doc, socioPpum, socioW - sPad * 2, 1, 5.3)
    doc.setTextColor(70, 110, 150)
    doc.text(socioPpumLines, sCx, pillY + pillH + 9.8, { align: "center" })
  }

  // ─── CÓDIGO TEXTUAL (sin barras) ───
  if (options.showBarcode && item.barcode?.trim()) {
    doc.setFont("helvetica", "normal")
    doc.setFontSize(5.4)
    doc.setTextColor(C.code.r, C.code.g, C.code.b)
    const codeText = `Cód: ${item.barcode.trim()}`
    doc.text(codeText, x + w - padX, y + h - 2.0, { align: "right" })
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
  if (!logo) throw new Error("No se pudo cargar el logo Quillotana")

  const { jsPDF } = await import("jspdf")
  const doc = new jsPDF({
    orientation: "portrait",
    unit: "mm",
    format: "letter",
    compress: true,
  })

  const { cols, rows, labelWMm, labelHMm, perPage } = SOCIO_ESTANDAR_FORMAT

  for (let i = 0; i < flat.length; i++) {
    const pos = i % perPage
    if (i > 0 && pos === 0) doc.addPage()
    const col = pos % cols
    const row = Math.floor(pos / cols)
    if (row >= rows) continue
    const { x, y } = labelPlacement(col, row)
    drawSocioEstandarLabel(
      doc,
      flat[i],
      x,
      y,
      labelWMm,
      labelHMm,
      options,
      logo,
    )
  }

  doc.save(filename)
}
