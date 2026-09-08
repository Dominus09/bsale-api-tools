/**
 * PDF carta — Etiquetas Socios V2 · Socio Estándar góndola 100×40 mm.
 *
 * Tres franjas (sin líneas divisorias):
 *   producto ~11 mm | precios ~17.5 mm | barcode ~11.5 mm
 *
 * Precio Socio: píldora azul pequeña + valor grande (sin rectángulo gigante).
 * "Provisional" nunca se imprime.
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

/** 10 cm × 4 cm — 2×6 = 12/hoja carta. */
export const SOCIO_ESTANDAR_FORMAT = {
  id: "SOCIO_ESTANDAR" as const,
  label: "Socio Estándar",
  description: "12 etiquetas · 10×4 cm · góndola horizontal",
  cols: 2,
  rows: 6,
  perPage: 12,
  labelWMm: 100,
  labelHMm: 40,
}

const PAGE_W = 215.9
const PAGE_H = 279.4
const MARGIN_MM = 5

/** Franjas internas (mm) sobre alto 40 */
const BAND_PRODUCT_H = 11
const BAND_PRICES_H = 17.5
// barcode = resto ≈ 11.5

const COLOR_CATEGORY = { r: 120, g: 120, b: 120 }
const COLOR_VARIANT = { r: 75, g: 75, b: 75 }
const COLOR_BORDER = { r: 210, g: 210, b: 210 }
const COLOR_SOCIO = { r: 0, g: 90, b: 168 }
const COLOR_SOCIO_SOFT = { r: 242, g: 248, b: 255 }
const COLOR_NORMAL_LABEL = { r: 110, g: 110, b: 110 }

type BarcodeSpec = { w: number; h: number; bar: number; minMm: number }

const BARCODE_SPEC: BarcodeSpec = { w: 480, h: 34, bar: 1.25, minMm: 7 }

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
  const padX = 2.4
  const padY = 1.2
  const innerX = x + padX
  const innerRight = x + w - padX
  const innerW = innerRight - innerX

  // Contorno suave (una sola pieza)
  doc.setDrawColor(COLOR_BORDER.r, COLOR_BORDER.g, COLOR_BORDER.b)
  doc.setLineWidth(0.12)
  doc.setFillColor(255, 255, 255)
  doc.rect(x, y, w, h, "FD")

  // ═══════════════════════════════════════════
  // FRANJA 1 — PRODUCTO (~11 mm)
  // ═══════════════════════════════════════════
  const band1Top = y + padY
  const band1Bottom = y + BAND_PRODUCT_H

  // Logo ~16–18 mm ancho
  const logoW = 17
  const logoH = logoW / logo.aspectRatio
  const logoY = band1Top + Math.max(0, (BAND_PRODUCT_H - padY - logoH) / 2)
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

  const textX = innerX + logoW + 2.2
  const textW = innerW - logoW - 2.2
  let ty = band1Top + 2.0

  if (options.showProductType && item.productType) {
    doc.setFontSize(5.2)
    doc.setFont("helvetica", "normal")
    doc.setTextColor(COLOR_CATEGORY.r, COLOR_CATEGORY.g, COLOR_CATEGORY.b)
    doc.text(item.productType.toUpperCase(), textX, ty, { maxWidth: textW })
    ty += 2.35
  }

  doc.setFont("helvetica", "bold")
  doc.setTextColor(15, 15, 15)
  const productLines = fitLines(doc, item.productName, textW, 2, 9)
  doc.setFontSize(9)
  doc.text(productLines, textX, ty)
  ty += productLines.length * 3.1

  const variant =
    item.variantName &&
    item.variantName.trim().toLowerCase() !== item.productName.trim().toLowerCase()
      ? item.variantName.trim()
      : ""
  if (variant && ty < band1Bottom - 0.5) {
    doc.setFontSize(6.2)
    doc.setFont("helvetica", "normal")
    doc.setTextColor(COLOR_VARIANT.r, COLOR_VARIANT.g, COLOR_VARIANT.b)
    doc.text(fitLines(doc, variant, textW, 1, 6.2), textX, ty)
  }

  // ═══════════════════════════════════════════
  // FRANJA 2 — PRECIOS (~17.5 mm)  40% / 60%
  // ═══════════════════════════════════════════
  const band2Top = y + BAND_PRODUCT_H
  const band2MidY = band2Top + BAND_PRICES_H / 2

  if (options.showPrices) {
    const normalW = innerW * 0.4
    const socioW = innerW * 0.6
    const socioX = innerX + normalW

    // Fondo celeste muy suave solo en sector Socio (sin borde grueso)
    doc.setFillColor(COLOR_SOCIO_SOFT.r, COLOR_SOCIO_SOFT.g, COLOR_SOCIO_SOFT.b)
    doc.rect(socioX, band2Top + 0.4, socioW, BAND_PRICES_H - 0.8, "F")

    // —— Precio Normal (40%) ——
    const nCx = innerX + normalW / 2
    doc.setFontSize(5)
    doc.setFont("helvetica", "normal")
    doc.setTextColor(
      COLOR_NORMAL_LABEL.r,
      COLOR_NORMAL_LABEL.g,
      COLOR_NORMAL_LABEL.b,
    )
    doc.text("PRECIO NORMAL", nCx, band2MidY - 3.2, { align: "center" })

    doc.setFontSize(14)
    doc.setFont("helvetica", "bold")
    doc.setTextColor(20, 20, 20)
    doc.text(formatClp(item.normalPrice), nCx, band2MidY + 4.2, {
      align: "center",
    })

    // —— Precio Socio (60%) ——
    const sCx = socioX + socioW / 2

    // Píldora azul pequeña
    const pillText = "SOCIO QUILLOTANA"
    doc.setFontSize(5)
    doc.setFont("helvetica", "bold")
    const pillTw = doc.getTextWidth(pillText)
    const pillPadX = 1.8
    const pillW = Math.min(socioW - 4, pillTw + pillPadX * 2)
    const pillH = 3.6
    const pillX = sCx - pillW / 2
    const pillY = band2Top + 2.2

    doc.setFillColor(COLOR_SOCIO.r, COLOR_SOCIO.g, COLOR_SOCIO.b)
    doc.roundedRect(pillX, pillY, pillW, pillH, 1.2, 1.2, "F")
    doc.setTextColor(255, 255, 255)
    doc.text(pillText, sCx, pillY + 2.55, { align: "center" })

    // Precio Socio grande
    doc.setFontSize(18)
    doc.setFont("helvetica", "bold")
    doc.setTextColor(COLOR_SOCIO.r, COLOR_SOCIO.g, COLOR_SOCIO.b)
    doc.text(formatClp(item.socioPrice), sCx, band2MidY + 6.4, {
      align: "center",
    })
  }

  // ═══════════════════════════════════════════
  // FRANJA 3 — BARCODE (~11.5 mm)
  // ═══════════════════════════════════════════
  if (options.showBarcode) {
    const band3Top = y + BAND_PRODUCT_H + BAND_PRICES_H
    const band3H = h - BAND_PRODUCT_H - BAND_PRICES_H
    const quiet = 4 // quiet zone lateral
    const bcW = Math.min(innerW - quiet * 2, innerW * 0.78)
    const bcH = Math.min(BARCODE_SPEC.minMm, band3H - 4.2)
    const bcX = innerX + (innerW - bcW) / 2
    const bcY = band3Top + 1.0

    if (barcodeImg) {
      try {
        doc.addImage(barcodeImg, "JPEG", bcX, bcY, bcW, bcH, undefined, "FAST")
      } catch {
        /* solo número */
      }
    }

    doc.setFontSize(6)
    doc.setFont("helvetica", "normal")
    doc.setTextColor(35, 35, 35)
    doc.text(item.barcode, x + w / 2, band3Top + band3H - 1.4, {
      align: "center",
    })
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
