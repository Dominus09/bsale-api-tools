/**
 * PDF — Etiquetas Socios V2 · Socio Estándar 100×40 mm
 * Look retail premium (mockup aprobado): limpio, integrado, jerarquía clara.
 * Sin % / provisional. Solo visual.
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
  description: "12 etiquetas · 10×4 cm · góndola retail",
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
const BAND_PRODUCT_H = 12.0
const BAND_PRICES_H = 16.5
// barcode ≈ 11.5

const C = {
  category: { r: 130, g: 130, b: 130 },
  variant: { r: 85, g: 85, b: 85 },
  border: { r: 220, g: 220, b: 220 },
  ink: { r: 18, g: 18, b: 18 },
  normalLabel: { r: 120, g: 120, b: 120 },
  socio: { r: 0, g: 90, b: 168 },
  socioSoft: { r: 245, g: 249, b: 253 },
}

type BarcodeSpec = { w: number; h: number; bar: number; minMm: number }
const BARCODE_SPEC: BarcodeSpec = { w: 500, h: 32, bar: 1.2, minMm: 6.8 }

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
    return scaled.toDataURL("image/jpeg", 0.93)
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
  const padX = 2.8
  const padY = 1.5
  const innerX = x + padX
  const innerW = w - padX * 2

  // Pieza única — borde muy suave
  doc.setDrawColor(C.border.r, C.border.g, C.border.b)
  doc.setLineWidth(0.1)
  doc.setFillColor(255, 255, 255)
  doc.roundedRect(x, y, w, h, 0.6, 0.6, "FD")

  // ─── PRODUCTO ───
  const band1Top = y + padY
  const logoW = 18.5
  const logoH = logoW / logo.aspectRatio
  const logoY = band1Top + 0.2
  doc.addImage(
    logo.dataUrl,
    logo.format,
    innerX,
    logoY,
    logoW,
    Math.min(logoH, BAND_PRODUCT_H - 1.2),
    undefined,
    "FAST",
  )

  const textX = innerX + logoW + 2.4
  const textW = innerW - logoW - 2.4
  let ty = band1Top + 2.1

  if (options.showProductType && item.productType) {
    doc.setFont("helvetica", "normal")
    doc.setFontSize(4.8)
    doc.setTextColor(C.category.r, C.category.g, C.category.b)
    doc.text(item.productType.toUpperCase(), textX, ty, { maxWidth: textW })
    ty += 2.15
  }

  doc.setFont("helvetica", "bold")
  doc.setFontSize(10.2)
  doc.setTextColor(C.ink.r, C.ink.g, C.ink.b)
  const nameLines = fitLines(doc, item.productName, textW, 2, 10.2)
  doc.text(nameLines, textX, ty)
  ty += nameLines.length * 3.2

  const variant =
    item.variantName &&
    item.variantName.trim().toLowerCase() !== item.productName.trim().toLowerCase()
      ? item.variantName.trim()
      : ""
  if (variant) {
    doc.setFont("helvetica", "normal")
    doc.setFontSize(6)
    doc.setTextColor(C.variant.r, C.variant.g, C.variant.b)
    doc.text(fitLines(doc, variant, textW, 1, 6), textX, Math.min(ty, y + BAND_PRODUCT_H - 0.4))
  }

  // ─── PRECIOS (40 / 60) ───
  const band2Top = y + BAND_PRODUCT_H
  const band2Mid = band2Top + BAND_PRICES_H / 2

  if (options.showPrices) {
    const normalW = innerW * 0.38
    const socioW = innerW * 0.62
    const socioX = innerX + normalW

    // Wash celeste sutil solo detrás del precio socio (inset elegante)
    const inset = 1.2
    doc.setFillColor(C.socioSoft.r, C.socioSoft.g, C.socioSoft.b)
    doc.roundedRect(
      socioX + inset * 0.4,
      band2Top + inset,
      socioW - inset * 0.8,
      BAND_PRICES_H - inset * 2,
      1.0,
      1.0,
      "F",
    )

    // Normal
    const nCx = innerX + normalW / 2
    doc.setFont("helvetica", "normal")
    doc.setFontSize(4.6)
    doc.setTextColor(C.normalLabel.r, C.normalLabel.g, C.normalLabel.b)
    doc.text("PRECIO NORMAL", nCx, band2Mid - 3.4, { align: "center" })

    doc.setFont("helvetica", "bold")
    doc.setFontSize(15)
    doc.setTextColor(C.ink.r, C.ink.g, C.ink.b)
    doc.text(formatClp(item.normalPrice), nCx, band2Mid + 4.0, { align: "center" })

    // Socio — píldora + valor hero
    const sCx = socioX + socioW / 2
    const pill = "SOCIO QUILLOTANA"
    doc.setFont("helvetica", "bold")
    doc.setFontSize(4.6)
    const pillTw = doc.getTextWidth(pill)
    const pillW = Math.min(socioW - 8, pillTw + 3.6)
    const pillH = 3.4
    const pillX = sCx - pillW / 2
    const pillY = band2Top + 2.6

    doc.setFillColor(C.socio.r, C.socio.g, C.socio.b)
    doc.roundedRect(pillX, pillY, pillW, pillH, pillH / 2, pillH / 2, "F")
    doc.setTextColor(255, 255, 255)
    doc.text(pill, sCx, pillY + 2.4, { align: "center" })

    doc.setFont("helvetica", "bold")
    doc.setFontSize(17.5)
    doc.setTextColor(C.socio.r, C.socio.g, C.socio.b)
    doc.text(formatClp(item.socioPrice), sCx, band2Mid + 6.2, { align: "center" })
  }

  // ─── BARCODE ───
  if (options.showBarcode) {
    const band3Top = y + BAND_PRODUCT_H + BAND_PRICES_H
    const band3H = h - BAND_PRODUCT_H - BAND_PRICES_H
    const quiet = 5
    const bcW = Math.min(innerW - quiet * 2, innerW * 0.76)
    const bcH = Math.min(BARCODE_SPEC.minMm, band3H - 4.5)
    const bcX = innerX + (innerW - bcW) / 2
    const bcY = band3Top + 1.2

    if (barcodeImg) {
      try {
        doc.addImage(barcodeImg, "JPEG", bcX, bcY, bcW, bcH, undefined, "FAST")
      } catch {
        /* número */
      }
    }

    doc.setFont("helvetica", "normal")
    doc.setFontSize(5.8)
    doc.setTextColor(50, 50, 50)
    doc.text(item.barcode, x + w / 2, y + h - 1.6, { align: "center" })
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
      barcodeCache.get(flat[i].barcode.trim()) ?? null,
    )
  }

  doc.save(filename)
}
