import { formatClp } from "@/lib/ors-map-ui"
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

async function drawPickingHeader(
  doc: import("jspdf").jsPDF,
  y: number,
  header: DispatchPlanPickingHeader,
  title: string,
): Promise<number> {
  const logo = await quillotanaLogoDataUrl()
  if (logo) {
    try {
      doc.addImage(logo, "PNG", 228, 6, 52, 14)
    } catch {
      /* ignore bad image */
    }
  }

  doc.setDrawColor(30, 64, 120)
  doc.setLineWidth(0.4)
  doc.line(14, 22, 282, 22)

  doc.setFontSize(14)
  doc.setTextColor(30, 64, 120)
  doc.text(title, 14, y)
  doc.setTextColor(0, 0, 0)
  y += 7
  doc.setFontSize(9)
  const lines: [string, string][] = [
    ["N° Planificación", header.planning_number],
    ["Fecha entrega", header.delivery_date],
    ["Ruta / comunas", `${header.route_name} · ${header.communes || "—"}`],
    ["Camión", header.truck_name],
    ["Chofer", header.driver_name || header.driver_label],
    ["Peoneta", header.assistant_label],
    ["Sello", header.sello || "—"],
  ]
  for (const [k, v] of lines) {
    doc.text(`${k}: ${v}`, 14, y)
    y += 4.5
  }

  const kpi = header.load_kpis
  if (kpi) {
    y += 2
    doc.setFontSize(8)
    doc.setFillColor(241, 245, 249)
    doc.rect(14, y - 3, 270, 10, "F")
    doc.setFont("helvetica", "bold")
    doc.text("Resumen de carga", 16, y + 2)
    doc.setFont("helvetica", "normal")
    doc.text(
      `${kpi.clients} clientes · ${kpi.documents} docs · ${formatClp(kpi.sales_total_clp)} · ` +
        `${kpi.distinct_products} SKU · ${Math.round(kpi.total_units)} u · ${Math.round(kpi.estimated_boxes)} cajas`,
      16,
      y + 6,
      { maxWidth: 265 },
    )
    y += 12
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
  let y = await drawPickingHeader(doc, 14, params.header, "Picking por cliente")

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
  for (const c of params.clients) {
    if (y > 185) {
      doc.addPage()
      y = await drawPickingHeader(doc, 14, params.header, "Picking por cliente (cont.)")
    }
    const city = c.city || ""
    if (city && city !== lastCity) {
      doc.setFont("helvetica", "bold")
      doc.setFontSize(8)
      doc.text(city, 10, y)
      doc.setFont("helvetica", "normal")
      doc.setFontSize(7)
      y += 4
      lastCity = city
    }
    x = 10
    const amt = Number(c.document_total) || 0
    total += amt
    const cells: (string | number)[] = [
      c.route_order ?? "",
      city,
      (c.client_name || "").slice(0, 28),
      (c.fantasy_name || "").slice(0, 24),
      c.document_number ?? "",
      (c.document_type || "").slice(0, 12),
      (c.payment_method || "").slice(0, 14),
      (c.seller_name || "").slice(0, 18),
      formatClp(amt),
    ]
    let cx = 10
    for (let i = 0; i < cols.length; i++) {
      doc.text(String(cells[i]), cx, y, { maxWidth: cols[i][1] - 1 })
      cx += cols[i][1]
    }
    y += 3.8
  }

  doc.setFont("helvetica", "bold")
  doc.text(`Total documentos: ${formatClp(total)}`, 14, y + 4)

  const slug = slugFile(params.header.truck_name)
  doc.save(`picking_cliente_${slug}_${params.header.delivery_date?.slice(0, 10) || "plan"}.pdf`)
}

export async function exportDispatchPlanPickingProductoPdf(params: {
  header: DispatchPlanPickingHeader
  items: DispatchPlanPickingProductRow[]
}): Promise<void> {
  const { jsPDF } = await import("jspdf")
  const doc = new jsPDF({ orientation: "portrait", unit: "mm", format: "a4" })
  let y = await drawPickingHeader(doc, 14, params.header, "Picking por producto")

  let currentType = ""
  let totalUnits = 0
  let totalBoxes = 0
  let totalMonto = 0

  doc.setFontSize(7)
  for (const item of params.items) {
    const tipo = item.tipo_producto || "Sin tipo"
    if (tipo !== currentType) {
      if (y > 260) {
        doc.addPage()
        y = await drawPickingHeader(doc, 14, params.header, "Picking producto (cont.)")
      }
      currentType = tipo
      doc.setFont("helvetica", "bold")
      doc.setFontSize(9)
      doc.setFillColor(226, 232, 240)
      doc.rect(14, y - 3, 182, 7, "F")
      doc.text(tipo, 16, y + 1)
      y += 8
      doc.setFont("helvetica", "normal")
      doc.setFontSize(7)
    }
    if (y > 275) {
      doc.addPage()
      y = await drawPickingHeader(doc, 14, params.header, "Picking producto (cont.)")
    }
    const u = Number(item.unidades) || 0
    const c = Number(item.cajas) || 0
    const m = Number(item.total_monto) || 0
    totalUnits += u
    totalBoxes += c
    totalMonto += m
    doc.text(
      `${(item.producto_variante || item.producto || "").slice(0, 42)} · ${item.codigo_barras || "—"} · ` +
        `${u} u · ${item.sin_unidad_caja ? "—" : c} cajas · ${formatClp(m)}`,
      16,
      y,
      { maxWidth: 175 },
    )
    y += 4.2
  }

  y += 4
  doc.setFont("helvetica", "bold")
  doc.setFontSize(9)
  doc.text(
    `Totales: ${Math.round(totalUnits)} u · ${Math.round(totalBoxes)} cajas · ${formatClp(totalMonto)}`,
    14,
    y,
  )

  const slug = slugFile(params.header.truck_name)
  doc.save(`picking_producto_${slug}_${params.header.delivery_date?.slice(0, 10) || "plan"}.pdf`)
}
