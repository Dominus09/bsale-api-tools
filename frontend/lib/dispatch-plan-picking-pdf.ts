import { formatClp } from "@/lib/ors-map-ui"
import type {
  DispatchPlanPickingClientRow,
  DispatchPlanPickingHeader,
  DispatchPlanPickingProductRow,
} from "@/lib/api"

function slugFile(s: string): string {
  return s.replace(/[^\w\-]+/g, "_").slice(0, 40) || "plan"
}

function drawPickingHeader(
  doc: import("jspdf").jsPDF,
  y: number,
  header: DispatchPlanPickingHeader,
  title: string,
): number {
  doc.setFontSize(14)
  doc.text(title, 14, y)
  y += 7
  doc.setFontSize(9)
  const lines: [string, string][] = [
    ["N° Planificación", header.planning_number],
    ["Fecha entrega", header.delivery_date],
    ["Ruta / comunas", `${header.route_name} · ${header.communes || "—"}`],
    ["Camión", header.truck_name],
    ["Chofer", header.driver_label],
    ["Peoneta", header.assistant_label],
    ["Sello", header.sello || "—"],
  ]
  for (const [k, v] of lines) {
    doc.text(`${k}: ${v}`, 14, y)
    y += 4.5
  }
  return y + 4
}

export async function exportDispatchPlanPickingClientePdf(params: {
  header: DispatchPlanPickingHeader
  clients: DispatchPlanPickingClientRow[]
  warnings?: string[]
}): Promise<void> {
  const { jsPDF } = await import("jspdf")
  const doc = new jsPDF({ orientation: "landscape", unit: "mm", format: "a4" })
  let y = drawPickingHeader(doc, 14, params.header, "Picking por cliente")

  if (params.warnings?.length) {
    doc.setTextColor(180, 83, 9)
    doc.setFontSize(8)
    for (const w of params.warnings.slice(0, 6)) {
      doc.text(`⚠ ${w}`, 14, y, { maxWidth: 270 })
      y += 5
    }
    doc.setTextColor(0, 0, 0)
    y += 3
  }

  const cols = [
    ["Ord", 10],
    ["Ciudad", 22],
    ["Cliente", 38],
    ["Fantasía", 32],
    ["Doc.", 16],
    ["Tipo", 18],
    ["Pago", 22],
    ["Vendedor", 28],
    ["Total", 22],
  ] as const
  let x = 10
  doc.setFontSize(7)
  doc.setFont("helvetica", "bold")
  for (const [label, w] of cols) {
    doc.text(label, x, y)
    x += w
  }
  y += 4
  doc.setFont("helvetica", "normal")

  let lastCity = ""
  let total = 0
  for (const row of params.clients) {
    if (y > 190) {
      doc.addPage()
      y = 14
    }
    if (row.city && row.city !== lastCity) {
      doc.setFont("helvetica", "bold")
      doc.setFontSize(8)
      doc.text(`— ${row.city} —`, 10, y)
      doc.setFont("helvetica", "normal")
      doc.setFontSize(7)
      y += 5
      lastCity = row.city
    }
    x = 10
    const cells = [
      String(row.route_order ?? ""),
      (row.city || "").slice(0, 14),
      (row.client_name || "").slice(0, 20),
      (row.fantasy_name || "").slice(0, 18),
      String(row.document_number ?? ""),
      (row.document_type || "").slice(0, 10),
      (row.payment_method || "").slice(0, 12),
      (row.seller_name || "").slice(0, 16),
      formatClp(Number(row.document_total) || 0),
    ]
    for (let i = 0; i < cols.length; i++) {
      doc.text(cells[i] ?? "", x, y)
      x += cols[i][1]
    }
    total += Number(row.document_total) || 0
    if (row.is_probable_included) {
      doc.setTextColor(180, 83, 9)
      doc.text("probable", x, y)
      doc.setTextColor(0, 0, 0)
    }
    y += 4
  }

  y += 4
  doc.setFont("helvetica", "bold")
  doc.setFontSize(9)
  doc.text(
    `Total documentos: ${params.clients.length} · Monto: ${formatClp(total)}`,
    10,
    y,
  )

  const fname = `picking_cliente_${slugFile(params.header.truck_name)}.pdf`
  doc.save(fname)
}

export async function exportDispatchPlanPickingProductoPdf(params: {
  header: DispatchPlanPickingHeader
  items: DispatchPlanPickingProductRow[]
  warnings?: string[]
}): Promise<void> {
  const { jsPDF } = await import("jspdf")
  const doc = new jsPDF({ orientation: "portrait", unit: "mm", format: "a4" })
  let y = drawPickingHeader(doc, 14, params.header, "Picking por producto")

  if (params.warnings?.length) {
    doc.setTextColor(180, 83, 9)
    doc.setFontSize(8)
    for (const w of params.warnings.slice(0, 5)) {
      doc.text(`⚠ ${w}`, 14, y, { maxWidth: 180 })
      y += 5
    }
    doc.setTextColor(0, 0, 0)
    y += 3
  }

  doc.setFontSize(7)
  doc.setFont("helvetica", "bold")
  doc.text("Tipo", 10, y)
  doc.text("Producto", 32, y)
  doc.text("U", 118, y)
  doc.text("Cajas", 128, y)
  doc.text("Monto", 148, y)
  y += 4
  doc.setFont("helvetica", "normal")

  let lastTipo = ""
  let sumU = 0
  let sumC = 0
  let sumM = 0
  for (const row of params.items) {
    if (y > 275) {
      doc.addPage()
      y = 14
    }
    const tipo = row.tipo_producto || "Sin tipo"
    if (tipo !== lastTipo) {
      doc.setFont("helvetica", "bold")
      doc.text(`— ${tipo} —`, 10, y)
      doc.setFont("helvetica", "normal")
      y += 5
      lastTipo = tipo
    }
    const cajasLabel = row.sin_unidad_caja
      ? "sin caja"
      : String(row.cajas ?? 0)
    doc.text(tipo.slice(0, 12), 10, y)
    doc.text((row.producto_variante || "").slice(0, 52), 32, y)
    doc.text(String(row.unidades ?? ""), 118, y)
    doc.text(cajasLabel, 128, y)
    doc.text(formatClp(Number(row.total_monto) || 0), 148, y)
    sumU += Number(row.unidades) || 0
    sumC += row.sin_unidad_caja ? 0 : Number(row.cajas) || 0
    sumM += Number(row.total_monto) || 0
    y += 4
  }

  y += 5
  doc.setFont("helvetica", "bold")
  doc.text(
    `Resumen: ${params.items.length} líneas · ${sumU} u · ${sumC} cajas · ${formatClp(sumM)}`,
    10,
    y,
  )

  doc.save(`picking_producto_${slugFile(params.header.truck_name)}.pdf`)
}
