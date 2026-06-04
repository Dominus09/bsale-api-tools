import { formatClp } from "@/lib/ors-map-ui"
import {
  clientDeliveryNotes,
  countDistinctClients,
  effectiveBoxes,
  formatPickingGeneratedAt,
  productLineLabel,
  snapshotLoadKpis,
} from "@/lib/picking-display"
import { QUILLOTANA_LOGO_GRUPO_URL } from "@/lib/quillotana-brand"
import type {
  DispatchPlanPickingClientRow,
  DispatchPlanPickingHeader,
  DispatchPlanPickingProductRow,
} from "@/lib/api"

function slugFile(s: string): string {
  return s.replace(/[^\w\-]+/g, "_").slice(0, 40) || "plan"
}

let logoDataUrlCache: string | null | undefined

async function quillotanaLogoDataUrl(): Promise<string | null> {
  if (logoDataUrlCache !== undefined) return logoDataUrlCache
  try {
    const res = await fetch(QUILLOTANA_LOGO_GRUPO_URL)
    if (!res.ok) {
      logoDataUrlCache = null
      return null
    }
    const blob = await res.blob()
    logoDataUrlCache = await new Promise((resolve) => {
      const reader = new FileReader()
      reader.onload = () => resolve(typeof reader.result === "string" ? reader.result : null)
      reader.onerror = () => resolve(null)
      reader.readAsDataURL(blob)
    })
    return logoDataUrlCache
  } catch {
    logoDataUrlCache = null
    return null
  }
}

type PdfMeta = {
  version?: number | null
  generatedAt?: string | null
}

type LayoutMode = "comfortable" | "normal" | "compact"

function layoutForRowCount(n: number): LayoutMode {
  if (n <= 12) return "comfortable"
  if (n > 28) return "compact"
  return "normal"
}

function applyFooters(doc: import("jspdf").jsPDF, meta: PdfMeta) {
  const total = doc.getNumberOfPages()
  const w = doc.internal.pageSize.getWidth()
  const h = doc.internal.pageSize.getHeight()
  const stamp = formatPickingGeneratedAt(meta.generatedAt)
  const ver = meta.version != null ? `Snapshot v${meta.version}` : "Snapshot"
  for (let p = 1; p <= total; p++) {
    doc.setPage(p)
    doc.setDrawColor(200, 200, 200)
    doc.setLineWidth(0.2)
    doc.line(10, h - 12, w - 10, h - 12)
    doc.setFontSize(7)
    doc.setTextColor(90, 90, 90)
    doc.text("Generado por Quillotana ERP", 12, h - 7)
    doc.text(stamp, 12, h - 3.5)
    doc.text(ver, w - 12, h - 5, { align: "right" })
    doc.text(`Página ${p} / ${total}`, w - 12, h - 2, { align: "right" })
    doc.setTextColor(0, 0, 0)
  }
}

async function drawBrandedHeader(
  doc: import("jspdf").jsPDF,
  y: number,
  header: DispatchPlanPickingHeader,
  title: string,
  kpiLine: string,
): Promise<number> {
  const pageW = doc.internal.pageSize.getWidth()
  const logo = await quillotanaLogoDataUrl()
  if (logo) {
    try {
      doc.addImage(logo, "PNG", 12, 8, 38, 11)
    } catch {
      /* ignore */
    }
  }
  doc.setFont("helvetica", "bold")
  doc.setFontSize(11)
  doc.setTextColor(30, 64, 120)
  doc.text("GRUPO QUILLOTANA", 54, 12)
  doc.setFontSize(13)
  doc.text(title, 54, 18)
  doc.setTextColor(0, 0, 0)
  doc.setFont("helvetica", "normal")
  doc.setFontSize(8)
  doc.text(
    `${header.planning_number} · ${header.delivery_date} · ${header.truck_name}`,
    54,
    23,
  )
  doc.text(
    `Chofer: ${header.driver_name || header.driver_label} · Peonetas: ${header.assistant_label}`,
    54,
    27,
  )
  doc.text(
    `${header.route_name}${header.communes ? ` · ${header.communes}` : ""}`,
    54,
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

function buildKpiLine(
  header: DispatchPlanPickingHeader,
  clients: DispatchPlanPickingClientRow[],
  items: DispatchPlanPickingProductRow[],
): string {
  const fromSnapshot = snapshotLoadKpis(header, clients, items)
  const hk = header.load_kpis
  const k =
    items.length > 0
      ? fromSnapshot
      : {
          ...fromSnapshot,
          clients: hk?.clients ?? fromSnapshot.clients,
          documents: clients.length || hk?.documents || fromSnapshot.documents,
          sales_total_clp: hk?.sales_total_clp ?? fromSnapshot.sales_total_clp,
          distinct_products: hk?.distinct_products ?? fromSnapshot.distinct_products,
          total_units: hk?.total_units ?? fromSnapshot.total_units,
          estimated_boxes: hk?.estimated_boxes ?? fromSnapshot.estimated_boxes,
        }
  return (
    `${k.clients} clientes · ${k.documents} documentos · ${formatClp(k.sales_total_clp)} · ` +
    `${k.distinct_products} SKU · ${Math.round(k.total_units)} u · ${Math.round(k.estimated_boxes)} cajas`
  )
}

export async function exportDispatchPlanPickingClientePdf(params: {
  header: DispatchPlanPickingHeader
  clients: DispatchPlanPickingClientRow[]
  warnings?: string[]
  version?: number | null
  generatedAt?: string | null
}): Promise<void> {
  const { jsPDF } = await import("jspdf")
  const doc = new jsPDF({ orientation: "landscape", unit: "mm", format: "a4" })
  const pageW = doc.internal.pageSize.getWidth()
  const pageH = doc.internal.pageSize.getHeight()
  const meta: PdfMeta = { version: params.version, generatedAt: params.generatedAt }
  const mode = layoutForRowCount(params.clients.length)
  const fontSize = mode === "comfortable" ? 7.5 : mode === "compact" ? 6 : 7
  const rowH = mode === "comfortable" ? 5.2 : mode === "compact" ? 3.4 : 4
  const bottomY = pageH - 16

  const cols: { key: string; w: number }[] = [
    { key: "Ord", w: 9 },
    { key: "Ciudad", w: 20 },
    { key: "Cliente", w: 32 },
    { key: "Fantasía", w: 28 },
    { key: "Dirección", w: 48 },
    { key: "Documento", w: 16 },
    { key: "Pago", w: 20 },
    { key: "Vendedor", w: 24 },
    { key: "Observaciones", w: 36 },
    { key: "Total", w: 22 },
  ]

  let y = await drawBrandedHeader(
    doc,
    14,
    params.header,
    "Picking Cliente",
    buildKpiLine(params.header, params.clients, []),
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

  const drawTableHead = () => {
    let x = 10
    doc.setFontSize(fontSize)
    doc.setFont("helvetica", "bold")
    for (const col of cols) {
      doc.text(col.key, x, y)
      x += col.w
    }
    y += rowH * 0.85
    doc.setFont("helvetica", "normal")
  }

  drawTableHead()

  let lastCity = ""
  let total = 0
  for (const c of params.clients) {
    if (y > bottomY) {
      doc.addPage()
      y = await drawBrandedHeader(
        doc,
        14,
        params.header,
        "Picking Cliente (cont.)",
        buildKpiLine(params.header, params.clients, []),
      )
      drawTableHead()
    }
    const city = c.city || ""
    if (city && city !== lastCity && mode !== "compact") {
      doc.setFont("helvetica", "bold")
      doc.setFontSize(fontSize + 0.5)
      doc.text(city, 10, y)
      doc.setFont("helvetica", "normal")
      doc.setFontSize(fontSize)
      y += rowH * 0.75
      lastCity = city
    }

    const amt = Number(c.document_total) || 0
    total += amt
    const notes = clientDeliveryNotes(c)
    const cells = [
      String(c.route_order ?? ""),
      city.slice(0, 18),
      (c.client_name || "").slice(0, 26),
      (c.fantasy_name || "").slice(0, 22),
      (c.address || "—").slice(0, 38),
      String(c.document_number ?? ""),
      (c.payment_method || "—").slice(0, 14),
      (c.seller_name || "—").slice(0, 18),
      notes.slice(0, 32),
      formatClp(amt),
    ]
    let x = 10
    for (let i = 0; i < cols.length; i++) {
      doc.text(cells[i], x, y, { maxWidth: cols[i].w - 1 })
      x += cols[i].w
    }
    y += rowH
  }

  doc.setFont("helvetica", "bold")
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
  const doc = new jsPDF({ orientation: "landscape", unit: "mm", format: "a4" })
  const pageW = doc.internal.pageSize.getWidth()
  const pageH = doc.internal.pageSize.getHeight()
  const meta: PdfMeta = { version: params.version, generatedAt: params.generatedAt }
  const mode = layoutForRowCount(params.items.length)
  const fontSize = mode === "comfortable" ? 7.5 : mode === "compact" ? 6 : 7
  const rowH = mode === "comfortable" ? 5 : mode === "compact" ? 3.5 : 4.2
  const bottomY = pageH - 16

  const cols = [
    { key: "Producto", w: 95 },
    { key: "Código barras", w: 32 },
    { key: "Unidades", w: 22 },
    { key: "Cajas", w: 18 },
    { key: "Monto", w: 28 },
  ]

  let y = await drawBrandedHeader(
    doc,
    14,
    params.header,
    "Picking Producto",
    buildKpiLine(params.header, [], params.items),
  )

  const drawTableHead = () => {
    let x = 10
    doc.setFontSize(fontSize)
    doc.setFont("helvetica", "bold")
    for (const col of cols) {
      doc.text(col.key, x, y)
      x += col.w
    }
    y += rowH * 0.9
    doc.setFont("helvetica", "normal")
  }

  drawTableHead()

  let currentType = ""
  let totalUnits = 0
  let totalBoxes = 0
  let totalMonto = 0

  for (const item of params.items) {
    const tipo = item.tipo_producto || "Sin tipo"
    if (tipo !== currentType) {
      if (y > bottomY - 8) {
        doc.addPage()
        y = await drawBrandedHeader(
          doc,
          14,
          params.header,
          "Picking Producto (cont.)",
          buildKpiLine(params.header, [], params.items),
        )
        drawTableHead()
      }
      currentType = tipo
      doc.setFont("helvetica", "bold")
      doc.setFontSize(fontSize + 0.5)
      doc.setFillColor(226, 232, 240)
      doc.rect(10, y - 3, pageW - 20, 6, "F")
      doc.text(tipo, 12, y)
      y += rowH
      doc.setFont("helvetica", "normal")
      doc.setFontSize(fontSize)
    }
    if (y > bottomY) {
      doc.addPage()
      y = await drawBrandedHeader(
        doc,
        14,
        params.header,
        "Picking Producto (cont.)",
        buildKpiLine(params.header, [], params.items),
      )
      drawTableHead()
    }

    const u = Number(item.unidades) || 0
    const boxes = effectiveBoxes(item)
    const m = Number(item.total_monto) || 0
    totalUnits += u
    totalBoxes += boxes
    totalMonto += m

    const label = productLineLabel(item)
    const cells = [
      label.slice(0, 58),
      (item.codigo_barras || "—").slice(0, 16),
      String(Math.round(u)),
      item.sin_unidad_caja && boxes === 0 ? "—" : String(Math.round(boxes)),
      formatClp(m),
    ]
    let x = 10
    for (let i = 0; i < cols.length; i++) {
      doc.text(cells[i], x, y, { maxWidth: cols[i].w - 1 })
      x += cols[i].w
    }
    y += rowH
  }

  doc.setFont("helvetica", "bold")
  doc.setFontSize(8)
  doc.text(
    `Totales: ${Math.round(totalUnits)} unidades · ${Math.round(totalBoxes)} cajas · ${formatClp(totalMonto)}`,
    12,
    Math.min(y + 5, bottomY),
  )

  applyFooters(doc, meta)
  const slug = slugFile(params.header.truck_name)
  doc.save(`picking_producto_${slug}_${params.header.delivery_date?.slice(0, 10) || "plan"}.pdf`)
}
