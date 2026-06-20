import { formatClp } from "@/lib/ors-map-ui"
import {
  categoryStatsFromItems,
  clientDeliveryNotes,
  clientPhone,
  countDistinctClients,
  displayCityLabel,
  effectiveBoxes,
  formatOperativeBoxes,
  normalizeCityKey,
  normalizePickingCategory,
  productLineLabel,
  stablePickingKpiLine,
} from "@/lib/picking-display"
import {
  createPdfLogoDocState,
  drawQuillotanaLogoOnPdf,
  loadQuillotanaLogoForPdf,
  PDF_LOGO_WIDTH_MM,
  type PdfLogoDocState,
  type PdfLogoPayload,
} from "@/lib/quillotana-logo-pdf"
import type {
  DispatchPlanPickingClientRow,
  DispatchPlanPickingHeader,
  DispatchPlanPickingProductRow,
} from "@/lib/api"

const MARGIN = { left: 6, right: 6, top: 6, bottom: 11 }
const HEADER_TEXT_TOP = 13
const HEADER_TEXT_BOTTOM = 29

function slugFile(s: string): string {
  return s.replace(/[^\w\-]+/g, "_").slice(0, 40) || "plan"
}

type PdfMeta = {
  version?: number | null
  generatedAt?: string | null
  planningNumber?: string | null
}

type LayoutMode = "comfortable" | "normal" | "compact"

type PdfCol = {
  key: string
  w: number
  align?: "left" | "right" | "center"
}

function layoutForRowCount(n: number): LayoutMode {
  if (n <= 12) return "comfortable"
  if (n > 28) return "compact"
  return "normal"
}

function planLabel(header: DispatchPlanPickingHeader): string {
  const pn = (header.planning_number || "").trim()
  if (!pn) return "Planificación"
  return pn.toUpperCase().startsWith("PLAN") ? pn : `PLAN-${pn}`
}

function crewLabel(header: DispatchPlanPickingHeader): string {
  return header.driver_name || header.driver_label || "—"
}

function assistantLabel(header: DispatchPlanPickingHeader): string {
  if (header.assistant_names?.length) return header.assistant_names.join(", ")
  return header.assistant_label || "—"
}

function sellerLine(client: DispatchPlanPickingClientRow): string {
  const name = (client.seller_name || "").trim() || "—"
  const phone = (client.seller_phone || "").trim()
  return phone ? `${name} · ${phone}` : name
}

function drawOperativePlanBlock(
  doc: import("jspdf").jsPDF,
  header: DispatchPlanPickingHeader,
  y: number,
): number {
  const pageW = doc.internal.pageSize.getWidth()
  const w = contentWidth(pageW)
  const rows = [
    ["Planificación", planLabel(header)],
    ["Vehículo", header.truck_name || "—"],
    ["Chofer", crewLabel(header)],
    ["Peoneta(s)", assistantLabel(header)],
  ]
  const blockH = rows.length * 4.2 + 3
  doc.setFillColor(248, 250, 252)
  doc.rect(MARGIN.left, y, w, blockH, "F")
  doc.setDrawColor(210, 214, 220)
  doc.setLineWidth(0.15)
  doc.rect(MARGIN.left, y, w, blockH, "S")
  doc.setFontSize(6.8)
  let ty = y + 3.5
  for (const [label, value] of rows) {
    doc.setFont("helvetica", "bold")
    doc.text(`${label}:`, MARGIN.left + 2, ty)
    doc.setFont("helvetica", "normal")
    doc.text(value, MARGIN.left + 28, ty, { maxWidth: w - 30 })
    ty += 4.2
  }
  return y + blockH + 2
}

function drawClientOperativeBand(
  doc: import("jspdf").jsPDF,
  header: DispatchPlanPickingHeader,
  client: DispatchPlanPickingClientRow,
  y: number,
  pageW: number,
): number {
  const w = contentWidth(pageW)
  const bandH = 8.5
  doc.setFillColor(241, 245, 249)
  doc.rect(MARGIN.left, y, w, bandH, "F")
  doc.setDrawColor(203, 213, 225)
  doc.setLineWidth(0.15)
  doc.rect(MARGIN.left, y, w, bandH, "S")
  doc.setFontSize(6.5)
  const clientName = client.fantasy_name || client.client_name || "—"
  const monto = formatClp(Number(client.document_total) || 0)
  const line = `Cliente: ${clientName}  ·  Vendedor: ${sellerLine(client)}  ·  Chofer: ${crewLabel(header)}  ·  Peoneta: ${assistantLabel(header)}  ·  Monto: ${monto}`
  doc.setFont("helvetica", "bold")
  doc.text(line, MARGIN.left + 1.5, y + 5.2, { maxWidth: w - 3 })
  doc.setFont("helvetica", "normal")
  return y + bandH + 1.5
}

function contentWidth(pageW: number): number {
  return pageW - MARGIN.left - MARGIN.right
}

function applyFooters(doc: import("jspdf").jsPDF, meta: PdfMeta) {
  const total = doc.getNumberOfPages()
  const w = doc.internal.pageSize.getWidth()
  const h = doc.internal.pageSize.getHeight()
  const plan = meta.planningNumber ? `Planificación ${meta.planningNumber}` : "Planificación"
  const ver = meta.version != null ? `Snapshot v${meta.version}` : "Snapshot"
  for (let p = 1; p <= total; p++) {
    doc.setPage(p)
    doc.setDrawColor(200, 200, 200)
    doc.setLineWidth(0.2)
    doc.line(MARGIN.left, h - MARGIN.bottom - 2, w - MARGIN.right, h - MARGIN.bottom - 2)
    doc.setFontSize(6.5)
    doc.setTextColor(70, 70, 70)
    doc.setFont("helvetica", "bold")
    doc.text("Grupo Quillotana ERP", MARGIN.left, h - MARGIN.bottom + 1)
    doc.setFont("helvetica", "normal")
    doc.text(plan, MARGIN.left, h - MARGIN.bottom + 4.5)
    doc.text(ver, w / 2, h - MARGIN.bottom + 1, { align: "center" })
    doc.text(`Página ${p} de ${total}`, w - MARGIN.right, h - MARGIN.bottom + 2.5, {
      align: "right",
    })
    doc.setTextColor(0, 0, 0)
  }
}

function tableXBounds(cols: PdfCol[], x0: number): number[] {
  const xs = [x0]
  for (const c of cols) xs.push(xs[xs.length - 1] + c.w)
  return xs
}

function drawTableHeader(
  doc: import("jspdf").jsPDF,
  cols: PdfCol[],
  x0: number,
  y: number,
  fontSize: number,
  rowH: number,
): number {
  const xs = tableXBounds(cols, x0)
  const w = xs[xs.length - 1] - x0
  doc.setFillColor(30, 64, 120)
  doc.rect(x0, y - rowH + 1.2, w, rowH, "F")
  doc.setTextColor(255, 255, 255)
  doc.setFont("helvetica", "bold")
  doc.setFontSize(fontSize)
  for (let i = 0; i < cols.length; i++) {
    const align = cols[i].align ?? "left"
    const tx = align === "right" ? xs[i + 1] - 1.5 : xs[i] + 1.5
    doc.text(cols[i].key, tx, y, { align: align === "right" ? "right" : "left" })
  }
  doc.setTextColor(0, 0, 0)
  doc.setFont("helvetica", "normal")
  return y + 1.2
}

function drawRowGrid(
  doc: import("jspdf").jsPDF,
  cols: PdfCol[],
  x0: number,
  yTop: number,
  rowH: number,
  shade = false,
) {
  const xs = tableXBounds(cols, x0)
  const w = xs[xs.length - 1] - x0
  if (shade) {
    doc.setFillColor(248, 250, 252)
    doc.rect(x0, yTop, w, rowH, "F")
  }
  doc.setDrawColor(210, 214, 220)
  doc.setLineWidth(0.15)
  doc.rect(x0, yTop, w, rowH, "S")
  for (let i = 1; i < xs.length - 1; i++) {
    doc.line(xs[i], yTop, xs[i], yTop + rowH)
  }
}

function drawCellText(
  doc: import("jspdf").jsPDF,
  text: string,
  col: PdfCol,
  x: number,
  y: number,
  fontSize: number,
  lineH: number,
): number {
  const pad = 1.2
  const maxW = col.w - pad * 2
  const lines = doc.splitTextToSize(text || "", maxW) as string[]
  const align = col.align ?? "left"
  for (let i = 0; i < lines.length; i++) {
    const ty = y + pad + (i + 1) * lineH - 0.5
    const tx =
      align === "right" ? x + col.w - pad : align === "center" ? x + col.w / 2 : x + pad
    doc.text(lines[i] || "—", tx, ty, {
      align: align === "right" ? "right" : align === "center" ? "center" : "left",
    })
  }
  return Math.max(lineH * lines.length + pad * 2, lineH + pad)
}

function drawBrandedHeader(
  doc: import("jspdf").jsPDF,
  y: number,
  header: DispatchPlanPickingHeader,
  title: string,
  kpiLine: string,
  logo: PdfLogoPayload | null,
  logoState: PdfLogoDocState,
): number {
  const pageW = doc.internal.pageSize.getWidth()
  const textX = MARGIN.left + (logo ? PDF_LOGO_WIDTH_MM + 3 : 0)
  const textBlockCenter = (HEADER_TEXT_TOP + HEADER_TEXT_BOTTOM) / 2

  if (logo) {
    const logoH = PDF_LOGO_WIDTH_MM / logo.aspectRatio
    const logoY = textBlockCenter - logoH / 2
    drawQuillotanaLogoOnPdf(doc, logo, logoState, MARGIN.left, logoY, PDF_LOGO_WIDTH_MM)
  } else {
    doc.setFont("helvetica", "bold")
    doc.setFontSize(10)
    doc.setTextColor(30, 64, 120)
    doc.text("GRUPO QUILLOTANA", textX, HEADER_TEXT_TOP)
  }

  doc.setFont("helvetica", "bold")
  doc.setFontSize(12)
  doc.setTextColor(30, 64, 120)
  doc.text(title, textX, HEADER_TEXT_TOP + 4)
  doc.setTextColor(0, 0, 0)
  doc.setFont("helvetica", "normal")
  doc.setFontSize(7.5)
  doc.text(
    `${header.planning_number} · ${header.delivery_date} · ${header.truck_name}`,
    textX,
    HEADER_TEXT_TOP + 9,
  )
  doc.text(
    `Chofer: ${crewLabel(header)} · Peonetas: ${assistantLabel(header)}`,
    textX,
    HEADER_TEXT_TOP + 13,
  )
  doc.text(
    `${header.route_name}${header.communes ? ` · ${header.communes}` : ""}`,
    textX,
    HEADER_TEXT_TOP + 17,
  )

  const ruleY = HEADER_TEXT_BOTTOM + 2
  doc.setDrawColor(30, 64, 120)
  doc.setLineWidth(0.3)
  doc.line(MARGIN.left, ruleY, pageW - MARGIN.right, ruleY)

  y = Math.max(y, ruleY + 3)
  const kpiH = 7.5
  doc.setFillColor(241, 245, 249)
  doc.rect(MARGIN.left, y, contentWidth(pageW), kpiH, "F")
  doc.setFont("helvetica", "bold")
  doc.setFontSize(7)
  doc.text("Resumen de carga", MARGIN.left + 2, y + 3)
  doc.setFont("helvetica", "normal")
  doc.text(kpiLine, MARGIN.left + 2, y + 6.2, { maxWidth: contentWidth(pageW) - 4 })
  return y + kpiH + 2
}

/** Escala columnas para ocupar el ancho útil de la página. */
function fitColumns(cols: PdfCol[], targetWidth: number): PdfCol[] {
  const sum = cols.reduce((s, c) => s + c.w, 0)
  if (sum <= 0) return cols
  const scale = targetWidth / sum
  return cols.map((c) => ({ ...c, w: Math.round(c.w * scale * 10) / 10 }))
}

export async function exportDispatchPlanPickingClientePdf(params: {
  header: DispatchPlanPickingHeader
  clients: DispatchPlanPickingClientRow[]
  warnings?: string[]
  version?: number | null
  generatedAt?: string | null
}): Promise<void> {
  const { jsPDF } = await import("jspdf")
  const doc = new jsPDF({ orientation: "landscape", unit: "mm", format: "letter" })
  const pageW = doc.internal.pageSize.getWidth()
  const pageH = doc.internal.pageSize.getHeight()
  const logo = await loadQuillotanaLogoForPdf()
  const logoState = createPdfLogoDocState()
  const meta: PdfMeta = {
    version: params.version,
    generatedAt: params.generatedAt,
    planningNumber: planLabel(params.header),
  }
  const mode = layoutForRowCount(params.clients.length)
  const fontSize = mode === "comfortable" ? 6.8 : mode === "compact" ? 5.3 : 6.2
  const lineH = fontSize * 0.42
  const bottomY = pageH - MARGIN.bottom - 5
  const x0 = MARGIN.left

  const cols = fitColumns(
    [
      { key: "Orden", w: 7 },
      { key: "Ciudad", w: 14 },
      { key: "Cliente", w: 22 },
      { key: "Fantasía", w: 18 },
      { key: "Teléfono", w: 17 },
      { key: "Dirección", w: 52 },
      { key: "Documento", w: 13 },
      { key: "Pago", w: 10 },
      { key: "Observaciones", w: 44 },
      { key: "Total", w: 18, align: "right" },
    ],
    contentWidth(pageW),
  )

  const kpiLine = stablePickingKpiLine(params.header, params.clients, [], formatClp)

  let y = drawBrandedHeader(
    doc,
    MARGIN.top,
    params.header,
    "Picking Cliente",
    kpiLine,
    logo,
    logoState,
  )

  if (params.warnings?.length) {
    doc.setTextColor(180, 83, 9)
    doc.setFontSize(6.5)
    for (const w of params.warnings.slice(0, 3)) {
      doc.text(`⚠ ${w}`, MARGIN.left, y, { maxWidth: contentWidth(pageW) })
      y += 3.5
    }
    doc.setTextColor(0, 0, 0)
    y += 1
  }

  const headH = mode === "compact" ? 4.5 : 5.5
  const drawHead = () => {
    doc.setFontSize(fontSize)
    y = drawTableHeader(doc, cols, x0, y + headH, fontSize, headH)
  }

  drawHead()

  let lastCityKey = ""
  let total = 0
  let rowIdx = 0

  for (const c of params.clients) {
    const cityKey = normalizeCityKey(c.city)
    const cityLabel = displayCityLabel(c.city)
    if (cityKey && cityKey !== lastCityKey && mode !== "compact") {
      if (y > bottomY - 8) {
        doc.addPage()
        y = drawBrandedHeader(
          doc,
          MARGIN.top,
          params.header,
          "Picking Cliente (cont.)",
          kpiLine,
          logo,
          logoState,
        )
        drawHead()
      }
      doc.setFont("helvetica", "bold")
      doc.setFontSize(fontSize + 0.3)
      doc.text(cityLabel, x0, y + 2.5)
      doc.setFont("helvetica", "normal")
      doc.setFontSize(fontSize)
      y += 4
      lastCityKey = cityKey
    }

    const notes = clientDeliveryNotes(c)
    const tel = clientPhone(c)
    const amt = Number(c.document_total) || 0
    total += amt
    const cellTexts = [
      String(c.route_order ?? ""),
      cityLabel,
      c.client_name || "",
      c.fantasy_name || "",
      tel,
      c.address || "",
      String(c.document_number ?? ""),
      c.payment_method || "",
      notes,
      formatClp(amt),
    ]

    let maxRowH = lineH + 2.5
    for (let i = 0; i < cols.length; i++) {
      const lines = doc.splitTextToSize(cellTexts[i], cols[i].w - 2.5) as string[]
      maxRowH = Math.max(maxRowH, lineH * lines.length + 2.5)
    }

    const bandH = 10
    if (y + maxRowH + bandH > bottomY) {
      doc.addPage()
      y = drawBrandedHeader(
        doc,
        MARGIN.top,
        params.header,
        "Picking Cliente (cont.)",
        kpiLine,
        logo,
        logoState,
      )
      drawHead()
    }
    y = drawClientOperativeBand(doc, params.header, c, y, pageW)

    drawRowGrid(doc, cols, x0, y, maxRowH, rowIdx % 2 === 1)
    doc.setFontSize(fontSize)
    let x = x0
    for (let i = 0; i < cols.length; i++) {
      drawCellText(doc, cellTexts[i], cols[i], x, y, fontSize, lineH)
      x += cols[i].w
    }
    y += maxRowH
    rowIdx += 1
  }

  doc.setFont("helvetica", "bold")
  doc.setFontSize(7.5)
  doc.text(
    `Total: ${formatClp(total)} · ${countDistinctClients(params.clients)} clientes · ${params.clients.length} documentos`,
    MARGIN.left,
    Math.min(y + 4, bottomY),
  )

  applyFooters(doc, meta)
  const slug = slugFile(params.header.truck_name)
  doc.save(`picking_cliente_${slug}_${params.header.delivery_date?.slice(0, 10) || "plan"}.pdf`)
}

export async function exportDispatchPlanPickingProductoPdf(params: {
  header: DispatchPlanPickingHeader
  items: DispatchPlanPickingProductRow[]
  version?: number | null
  generatedAt?: string | null
}): Promise<void> {
  const { jsPDF } = await import("jspdf")
  const doc = new jsPDF({ orientation: "portrait", unit: "mm", format: "letter" })
  const pageW = doc.internal.pageSize.getWidth()
  const pageH = doc.internal.pageSize.getHeight()
  const logo = await loadQuillotanaLogoForPdf()
  const logoState = createPdfLogoDocState()
  const meta: PdfMeta = {
    version: params.version,
    generatedAt: params.generatedAt,
    planningNumber: planLabel(params.header),
  }
  const mode = layoutForRowCount(params.items.length)
  const fontSize = mode === "comfortable" ? 7 : mode === "compact" ? 5.5 : 6.4
  const lineH = fontSize * 0.42
  const bottomY = pageH - MARGIN.bottom - 5
  const x0 = MARGIN.left

  const cols = fitColumns(
    [
      { key: "Producto", w: 92 },
      { key: "Código barra", w: 32 },
      { key: "Unidades", w: 18, align: "center" },
      { key: "Cajas", w: 16, align: "center" },
      { key: "Monto", w: 22, align: "right" },
    ],
    contentWidth(pageW),
  )

  const kpiLine = stablePickingKpiLine(params.header, [], params.items, formatClp)

  let y = drawBrandedHeader(
    doc,
    MARGIN.top,
    params.header,
    "Picking Producto",
    kpiLine,
    logo,
    logoState,
  )
  y = drawOperativePlanBlock(doc, params.header, y)

  const headH = mode === "compact" ? 4.5 : 5.5
  const drawHead = () => {
    doc.setFontSize(fontSize)
    y = drawTableHeader(doc, cols, x0, y + headH, fontSize, headH)
  }

  drawHead()

  let currentType = ""
  let totalUnits = 0
  let totalBoxes = 0
  let totalMonto = 0
  let rowIdx = 0

  for (const item of params.items) {
    const tipo = normalizePickingCategory(item.tipo_producto)
    if (tipo !== currentType) {
      if (y > bottomY - 12) {
        doc.addPage()
        y = drawBrandedHeader(
          doc,
          MARGIN.top,
          params.header,
          "Picking Producto (cont.)",
          kpiLine,
          logo,
          logoState,
        )
        drawHead()
      }
      currentType = tipo
      const stats = categoryStatsFromItems(params.items, tipo)
      doc.setFillColor(226, 232, 240)
      doc.rect(x0, y, contentWidth(pageW), 9, "F")
      doc.setDrawColor(180, 190, 200)
      doc.setLineWidth(0.2)
      doc.rect(x0, y, contentWidth(pageW), 9, "S")
      doc.setFont("helvetica", "bold")
      doc.setFontSize(fontSize + 0.8)
      doc.text(tipo.toUpperCase(), x0 + 1.5, y + 3.5)
      doc.setFont("helvetica", "normal")
      doc.setFontSize(fontSize)
      doc.text(
        `${stats.skus} SKU · ${stats.units} u · ${formatOperativeBoxes(stats.boxes)} cajas · ${formatClp(stats.monto)}`,
        x0 + 1.5,
        y + 7,
      )
      y += 9.5
    }

    const u = Number(item.unidades) || 0
    const boxes = effectiveBoxes(item)
    const m = Number(item.total_monto) || 0
    totalUnits += u
    totalBoxes += boxes
    totalMonto += m

    const label = productLineLabel(item)
    const boxesStr =
      item.sin_unidad_caja && boxes === 0 ? "—" : formatOperativeBoxes(boxes)
    const cellTexts = [
      label,
      item.codigo_barras || "—",
      String(Math.round(u)),
      boxesStr,
      formatClp(m),
    ]

    let maxRowH = lineH + 2.5
    for (let i = 0; i < cols.length; i++) {
      const lines = doc.splitTextToSize(cellTexts[i], cols[i].w - 2.5) as string[]
      maxRowH = Math.max(maxRowH, lineH * lines.length + 2.5)
    }

    if (y + maxRowH > bottomY) {
      doc.addPage()
      y = drawBrandedHeader(
        doc,
        MARGIN.top,
        params.header,
        "Picking Producto (cont.)",
        kpiLine,
        logo,
        logoState,
      )
      drawHead()
      doc.setFont("helvetica", "bold")
      doc.setFontSize(fontSize + 0.3)
      doc.text(`${tipo} (cont.)`, x0, y + 2.5)
      y += 4
    }

    drawRowGrid(doc, cols, x0, y, maxRowH, rowIdx % 2 === 1)
    doc.setFontSize(fontSize)
    let x = x0
    for (let i = 0; i < cols.length; i++) {
      drawCellText(doc, cellTexts[i], cols[i], x, y, fontSize, lineH)
      x += cols[i].w
    }
    y += maxRowH
    rowIdx += 1
  }

  doc.setFont("helvetica", "bold")
  doc.setFontSize(7.5)
  doc.text(
    `Totales: ${Math.round(totalUnits)} unidades · ${formatOperativeBoxes(totalBoxes)} cajas · ${formatClp(totalMonto)}`,
    MARGIN.left,
    Math.min(y + 4, bottomY),
  )

  applyFooters(doc, meta)
  const slug = slugFile(params.header.truck_name)
  doc.save(`picking_producto_${slug}_${params.header.delivery_date?.slice(0, 10) || "plan"}.pdf`)
}
