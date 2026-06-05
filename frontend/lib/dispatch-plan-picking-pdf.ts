import { formatClp } from "@/lib/ors-map-ui"
import {
  categoryStatsFromItems,
  clientDeliveryNotes,
  countDistinctClients,
  effectiveBoxes,
  formatOperativeBoxes,
  normalizePickingCategory,
  productLineLabel,
  stablePickingKpiLine,
} from "@/lib/picking-display"
import { loadQuillotanaLogoForPdf } from "@/lib/quillotana-logo-pdf"
import type {
  DispatchPlanPickingClientRow,
  DispatchPlanPickingHeader,
  DispatchPlanPickingProductRow,
} from "@/lib/api"

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
    doc.line(10, h - 14, w - 10, h - 14)
    doc.setFontSize(7)
    doc.setTextColor(70, 70, 70)
    doc.setFont("helvetica", "bold")
    doc.text("Grupo Quillotana ERP", 12, h - 9)
    doc.setFont("helvetica", "normal")
    doc.text(plan, 12, h - 5.5)
    doc.text(ver, w / 2, h - 9, { align: "center" })
    doc.text(`Página ${p} de ${total}`, w - 12, h - 7, { align: "right" })
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
  doc.rect(x0, y - rowH + 1.5, w, rowH, "F")
  doc.setTextColor(255, 255, 255)
  doc.setFont("helvetica", "bold")
  doc.setFontSize(fontSize)
  for (let i = 0; i < cols.length; i++) {
    const align = cols[i].align ?? "left"
    const tx = align === "right" ? xs[i + 1] - 2 : xs[i] + 2
    doc.text(cols[i].key, tx, y, { align: align === "right" ? "right" : "left" })
  }
  doc.setTextColor(0, 0, 0)
  doc.setFont("helvetica", "normal")
  return y + 1.5
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
  const pad = 1.5
  const maxW = col.w - pad * 2
  const lines = doc.splitTextToSize(text || "—", maxW) as string[]
  const align = col.align ?? "left"
  for (let i = 0; i < lines.length; i++) {
    const ty = y + pad + (i + 1) * lineH - 0.5
    const tx =
      align === "right" ? x + col.w - pad : align === "center" ? x + col.w / 2 : x + pad
    doc.text(lines[i], tx, ty, {
      align: align === "right" ? "right" : align === "center" ? "center" : "left",
    })
  }
  return Math.max(lineH * lines.length + pad * 2, lineH + pad)
}

async function drawBrandedHeader(
  doc: import("jspdf").jsPDF,
  y: number,
  header: DispatchPlanPickingHeader,
  title: string,
  kpiLine: string,
): Promise<number> {
  const pageW = doc.internal.pageSize.getWidth()
  const logo = await loadQuillotanaLogoForPdf()
  let titleX = 12
  if (logo) {
    try {
      const aspect = logo.widthPx / logo.heightPx
      const logoH = 12
      const logoW = Math.min(48, logoH * aspect)
      doc.addImage(logo.dataUrl, logo.format, 12, 8, logoW, logoH)
      titleX = 12 + logoW + 4
    } catch {
      /* fallback texto */
    }
  }
  doc.setFont("helvetica", "bold")
  doc.setFontSize(11)
  doc.setTextColor(30, 64, 120)
  if (!logo) {
    doc.text("GRUPO QUILLOTANA", titleX, 12)
  }
  doc.setFontSize(13)
  doc.text(title, titleX, 18)
  doc.setTextColor(0, 0, 0)
  doc.setFont("helvetica", "normal")
  doc.setFontSize(8)
  doc.text(
    `${header.planning_number} · ${header.delivery_date} · ${header.truck_name}`,
    titleX,
    23,
  )
  doc.text(
    `Chofer: ${header.driver_name || header.driver_label} · Peonetas: ${header.assistant_label}`,
    titleX,
    27,
  )
  doc.text(
    `${header.route_name}${header.communes ? ` · ${header.communes}` : ""}`,
    titleX,
    31,
  )

  doc.setDrawColor(30, 64, 120)
  doc.setLineWidth(0.35)
  doc.line(12, 34, pageW - 12, 34)

  y = Math.max(y, 38)
  doc.setFillColor(241, 245, 249)
  doc.rect(12, y, pageW - 24, 9, "F")
  doc.setFont("helvetica", "bold")
  doc.setFontSize(7.5)
  doc.text("Resumen de carga", 14, y + 3.5)
  doc.setFont("helvetica", "normal")
  doc.text(kpiLine, 14, y + 7, { maxWidth: pageW - 28 })
  return y + 12
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
  const meta: PdfMeta = {
    version: params.version,
    generatedAt: params.generatedAt,
    planningNumber: planLabel(params.header),
  }
  const mode = layoutForRowCount(params.clients.length)
  const fontSize = mode === "comfortable" ? 7 : mode === "compact" ? 5.5 : 6.5
  const lineH = fontSize * 0.42
  const bottomY = pageH - 18
  const x0 = 10

  const cols: PdfCol[] = [
    { key: "Orden", w: 9 },
    { key: "Ciudad", w: 18 },
    { key: "Cliente", w: 28 },
    { key: "Fantasía", w: 24 },
    { key: "Dirección", w: 58 },
    { key: "Documento", w: 16 },
    { key: "Pago", w: 12 },
    { key: "Vendedor", w: 14 },
    { key: "Observaciones", w: 52 },
    { key: "Total", w: 22, align: "right" },
  ]

  const kpiLine = stablePickingKpiLine(params.header, params.clients, [], formatClp)

  let y = await drawBrandedHeader(
    doc,
    14,
    params.header,
    "Picking Cliente",
    kpiLine,
  )

  if (params.warnings?.length) {
    doc.setTextColor(180, 83, 9)
    doc.setFontSize(7)
    for (const w of params.warnings.slice(0, 4)) {
      doc.text(`⚠ ${w}`, 12, y, { maxWidth: pageW - 24 })
      y += 4
    }
    doc.setTextColor(0, 0, 0)
    y += 2
  }

  const headH = mode === "compact" ? 5 : 6
  const drawHead = () => {
    doc.setFontSize(fontSize)
    y = drawTableHeader(doc, cols, x0, y + headH, fontSize, headH)
  }

  drawHead()

  let lastCity = ""
  let total = 0
  let rowIdx = 0

  for (const c of params.clients) {
    const city = c.city || ""
    if (city && city !== lastCity && mode !== "compact") {
      if (y > bottomY - 10) {
        doc.addPage()
        y = await drawBrandedHeader(
          doc,
          14,
          params.header,
          "Picking Cliente (cont.)",
          kpiLine,
        )
        drawHead()
      }
      doc.setFont("helvetica", "bold")
      doc.setFontSize(fontSize + 0.5)
      doc.text(city, x0, y + 3)
      doc.setFont("helvetica", "normal")
      doc.setFontSize(fontSize)
      y += 5
      lastCity = city
    }

    const notes = clientDeliveryNotes(c)
    const amt = Number(c.document_total) || 0
    total += amt
    const cellTexts = [
      String(c.route_order ?? ""),
      city,
      c.client_name || "",
      c.fantasy_name || "",
      c.address || "—",
      String(c.document_number ?? ""),
      c.payment_method || "—",
      c.seller_name || "—",
      notes,
      formatClp(amt),
    ]

    let maxRowH = lineH + 3
    const splitHeights: number[] = []
    for (let i = 0; i < cols.length; i++) {
      const lines = doc.splitTextToSize(cellTexts[i], cols[i].w - 3) as string[]
      splitHeights.push(Math.max(lineH * lines.length + 3, lineH + 3))
    }
    maxRowH = Math.max(...splitHeights, lineH + 3)

    if (y + maxRowH > bottomY) {
      doc.addPage()
      y = await drawBrandedHeader(
        doc,
        14,
        params.header,
        "Picking Cliente (cont.)",
        kpiLine,
      )
      drawHead()
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
  doc.setFontSize(8)
  doc.text(
    `Total: ${formatClp(total)} · ${countDistinctClients(params.clients)} clientes · ${params.clients.length} documentos`,
    12,
    Math.min(y + 5, bottomY),
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
  const doc = new jsPDF({ orientation: "landscape", unit: "mm", format: "letter" })
  const pageW = doc.internal.pageSize.getWidth()
  const pageH = doc.internal.pageSize.getHeight()
  const meta: PdfMeta = {
    version: params.version,
    generatedAt: params.generatedAt,
    planningNumber: planLabel(params.header),
  }
  const mode = layoutForRowCount(params.items.length)
  const fontSize = mode === "comfortable" ? 7 : mode === "compact" ? 5.5 : 6.5
  const lineH = fontSize * 0.42
  const bottomY = pageH - 18
  const x0 = 10

  const cols: PdfCol[] = [
    { key: "Producto", w: 118 },
    { key: "Código barra", w: 34 },
    { key: "Unidades", w: 22, align: "right" },
    { key: "Cajas", w: 20, align: "right" },
    { key: "Monto", w: 28, align: "right" },
  ]

  const kpiLine = stablePickingKpiLine(params.header, [], params.items, formatClp)

  let y = await drawBrandedHeader(
    doc,
    14,
    params.header,
    "Picking Producto",
    kpiLine,
  )

  const headH = mode === "compact" ? 5 : 6
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
      if (y > bottomY - 14) {
        doc.addPage()
        y = await drawBrandedHeader(
          doc,
          14,
          params.header,
          "Picking Producto (cont.)",
          kpiLine,
        )
        drawHead()
      }
      currentType = tipo
      const stats = categoryStatsFromItems(params.items, tipo)
      doc.setFillColor(226, 232, 240)
      doc.rect(x0, y, pageW - 20, 10, "F")
      doc.setDrawColor(180, 190, 200)
      doc.setLineWidth(0.2)
      doc.rect(x0, y, pageW - 20, 10, "S")
      doc.setFont("helvetica", "bold")
      doc.setFontSize(fontSize + 1)
      doc.text(tipo.toUpperCase(), x0 + 2, y + 4)
      doc.setFont("helvetica", "normal")
      doc.setFontSize(fontSize)
      doc.text(
        `${stats.skus} SKU · ${stats.units} u · ${formatOperativeBoxes(stats.boxes)} cajas · ${formatClp(stats.monto)}`,
        x0 + 2,
        y + 8,
      )
      y += 11
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

    let maxRowH = lineH + 3
    for (let i = 0; i < cols.length; i++) {
      const lines = doc.splitTextToSize(cellTexts[i], cols[i].w - 3) as string[]
      maxRowH = Math.max(maxRowH, lineH * lines.length + 3)
    }

    if (y + maxRowH > bottomY) {
      doc.addPage()
      y = await drawBrandedHeader(
        doc,
        14,
        params.header,
        "Picking Producto (cont.)",
        kpiLine,
      )
      drawHead()
      doc.setFont("helvetica", "bold")
      doc.setFontSize(fontSize + 0.5)
      doc.text(`${tipo} (cont.)`, x0, y + 3)
      y += 5
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
  doc.setFontSize(8)
  doc.text(
    `Totales: ${Math.round(totalUnits)} unidades · ${formatOperativeBoxes(totalBoxes)} cajas · ${formatClp(totalMonto)}`,
    12,
    Math.min(y + 5, bottomY),
  )

  applyFooters(doc, meta)
  const slug = slugFile(params.header.truck_name)
  doc.save(`picking_producto_${slug}_${params.header.delivery_date?.slice(0, 10) || "plan"}.pdf`)
}
