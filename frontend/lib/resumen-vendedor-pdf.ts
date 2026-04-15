import type { DistribuidoraResumenDiaJson, DistribuidoraResumenVendedorJson } from "@/lib/api"

const MM_MARGIN = 16
const PAGE_W_MM = 210
const PAGE_H_MM = 297
const CONTENT_W = PAGE_W_MM - MM_MARGIN * 2

/** Contenedor fuera de pantalla solo para rasterizar DOM sin tema Tailwind/oklch. */
const PDF_EXPORT_CONTAINER_ID = "distribuidora-pdf-export-container"

const TEXTO_METODOLOGIA =
  "Las rutas fueron optimizadas considerando distancia entre clientes, punto de partida y retorno al origen, buscando minimizar los kilómetros recorridos y el tiempo de traslado."

function formatClp(n: number): string {
  return Math.round(n).toLocaleString("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  })
}

function nombreCliente(c: Record<string, unknown>): string {
  const raw =
    (c.cliente_nombre as string) ||
    (c.nombre as string) ||
    (c.nombre_fantasia as string) ||
    ""
  const t = String(raw).trim()
  return t || "Cliente"
}

function clientesOrdenados(dia: DistribuidoraResumenDiaJson): string[] {
  const raw = dia.clientes
  if (!Array.isArray(raw)) return []
  const rows = raw as Record<string, unknown>[]
  const withIdx = rows.map((c, i) => ({
    c,
    ov: Number(c.orden_visita) || i + 1,
  }))
  withIdx.sort((a, b) => a.ov - b.ov)
  return withIdx.map(({ c }) => nombreCliente(c))
}

/** html2canvas 1.x no parsea oklch/oklab en reglas heredadas al clonar el mapa Leaflet. */
function colorUsesModernSpace(value: string): boolean {
  return /\boklch\s*\(|\boklab\s*\(/i.test(value)
}

function stripModernColorFunctionsFromCssText(css: string): string {
  return css
    .replace(/oklch\([^)]*\)/gi, "rgb(15, 23, 42)")
    .replace(/oklab\([^)]*\)/gi, "rgb(15, 23, 42)")
}

function kebabFromDomProp(prop: string): string {
  return prop.replace(/[A-Z]/g, (ch) => `-${ch.toLowerCase()}`)
}

function fallbackColorForCssProp(prop: string): string {
  if (prop === "backgroundColor") return "rgb(226, 232, 240)"
  if (prop.startsWith("border") && prop.endsWith("Color")) return "rgb(148, 163, 184)"
  if (prop === "outlineColor") return "rgb(148, 163, 184)"
  if (prop === "fill") return "rgb(51, 136, 255)"
  if (prop === "stroke") return "rgb(37, 99, 235)"
  if (prop === "caretColor" || prop === "columnRuleColor" || prop === "textDecorationColor") return "rgb(15, 23, 42)"
  return "rgb(15, 23, 42)"
}

function sanitizeCloneColorsForPdf(root: HTMLElement): void {
  const visit = (el: Element) => {
    const rawAttr = el.getAttribute("style")
    if (rawAttr && colorUsesModernSpace(rawAttr)) {
      el.setAttribute("style", stripModernColorFunctionsFromCssText(rawAttr))
    }
    if (el instanceof HTMLElement) {
      const cs = getComputedStyle(el)
      const textProps: (keyof CSSStyleDeclaration)[] = [
        "color",
        "backgroundColor",
        "borderTopColor",
        "borderRightColor",
        "borderBottomColor",
        "borderLeftColor",
        "outlineColor",
        "textDecorationColor",
        "columnRuleColor",
        "caretColor",
      ]
      for (const prop of textProps) {
        const raw = cs[prop] as string | undefined
        if (!raw || raw === "transparent" || raw === "rgba(0, 0, 0, 0)") continue
        if (colorUsesModernSpace(raw)) {
          el.style.setProperty(kebabFromDomProp(String(prop)), fallbackColorForCssProp(String(prop)), "important")
        }
      }
      const shadow = cs.boxShadow
      if (shadow && colorUsesModernSpace(shadow)) {
        el.style.setProperty("box-shadow", "none", "important")
      }
      const tshadow = cs.textShadow
      if (tshadow && colorUsesModernSpace(tshadow)) {
        el.style.setProperty("text-shadow", "none", "important")
      }
      const filt = cs.filter
      if (filt && filt !== "none" && colorUsesModernSpace(filt)) {
        el.style.setProperty("filter", "none", "important")
      }
    }
    if (el instanceof SVGElement) {
      const cs = getComputedStyle(el)
      for (const prop of ["fill", "stroke"] as const) {
        const raw = cs[prop]
        if (!raw || raw === "none") continue
        if (colorUsesModernSpace(raw)) {
          el.style.setProperty(prop, fallbackColorForCssProp(prop), "important")
        }
      }
    }
  }
  visit(root)
  root.querySelectorAll("*").forEach(visit)
}

/**
 * Fuerza colores seguros en un subárbol (p. ej. clon) para html2canvas sin oklch.
 * No usa variables de tema.
 */
function applyBrutalPdfStyles(root: HTMLElement): void {
  const nodes: Element[] = [root, ...Array.from(root.querySelectorAll("*"))]
  for (const el of nodes) {
    if (!(el instanceof HTMLElement)) continue
    el.style.setProperty("color", "#111827", "important")
    el.style.setProperty("background-color", "#ffffff", "important")
    el.style.setProperty("border-color", "#d1d5db", "important")
    el.style.setProperty("box-shadow", "none", "important")
    el.style.setProperty("text-shadow", "none", "important")
    el.style.setProperty("filter", "none", "important")
  }
  for (const el of nodes) {
    if (!(el instanceof SVGElement)) continue
    el.style.setProperty("fill", "#2563eb", "important")
    el.style.setProperty("stroke", "#1d4ed8", "important")
  }
}

function ensurePdfExportContainer(): HTMLElement {
  let el = document.getElementById(PDF_EXPORT_CONTAINER_ID) as HTMLElement | null
  if (!el) {
    el = document.createElement("div")
    el.id = PDF_EXPORT_CONTAINER_ID
    el.setAttribute("aria-hidden", "true")
    el.style.cssText =
      "position:fixed;left:-9999px;top:0;width:820px;max-width:100vw;pointer-events:none;z-index:-1;overflow:visible;margin:0;padding:0;"
    document.body.appendChild(el)
  }
  return el
}

function pdfEl(tag: string, style: string, text?: string): HTMLElement {
  const n = document.createElement(tag)
  n.style.cssText = style
  if (text != null) n.textContent = text
  return n
}

/**
 * DOM dedicado al PDF: solo estilos inline (#hex / rgb), sin clases Tailwind.
 * Incluye imagen del mapa ya rasterizada (JPEG data URL).
 */
function buildPdfExportSnapshot(
  resumen: DistribuidoraResumenVendedorJson,
  viaticoClp: number | null,
  mapImageDataUrl: string,
): HTMLElement {
  const root = document.createElement("div")
  root.style.cssText =
    "box-sizing:border-box;width:780px;padding:18px 20px;background:#ffffff;color:#111827;font-family:system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;font-size:13px;line-height:1.45;border:1px solid #e5e7eb;"

  const h = pdfEl(
    "h2",
    "margin:0 0 12px 0;padding:0;font-size:18px;font-weight:700;color:#0f172a;border:none;background:#ffffff;",
    `Ruta semanal — ${resumen.vendedor}`,
  )
  root.appendChild(h)

  const viaticoLine =
    viaticoClp != null && Number.isFinite(viaticoClp)
      ? formatClp(viaticoClp)
      : "No calculado (defina rendimiento y precio de combustible en pantalla)."

  const metrics = pdfEl(
    "div",
    "display:grid;grid-template-columns:1fr 1fr;gap:10px 16px;margin-bottom:14px;padding:12px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:4px;",
  )
  const lines = [
    `Km total semana: ${resumen.km_total_semana} km`,
    `Clientes (visitas semana): ${resumen.clientes_total_semana}`,
    `Tiempo estimado (conducción): ${resumen.min_total_semana} min`,
    `Viático estimado: ${viaticoLine}`,
    `Km día más largo: ${resumen.km_dia_mas_largo} km`,
    `Km día más corto: ${resumen.km_dia_mas_corto} km`,
    `Promedio km / día: ${resumen.promedio_km_por_dia}`,
  ]
  for (const line of lines) {
    metrics.appendChild(
      pdfEl("div", "margin:0;padding:0;color:#334155;background:transparent;border:none;font-size:12px;", line),
    )
  }
  root.appendChild(metrics)

  const mapTitle = pdfEl(
    "h3",
    "margin:16px 0 8px 0;font-size:14px;font-weight:700;color:#0f172a;background:#ffffff;border:none;",
    "Mapa semanal",
  )
  root.appendChild(mapTitle)

  const img = document.createElement("img")
  img.src = mapImageDataUrl
  img.alt = "Mapa semanal"
  img.style.cssText =
    "display:block;width:100%;max-width:740px;height:auto;margin:0 auto;border:1px solid #d1d5db;background:#dfe6ee;"
  root.appendChild(img)

  const leyenda = pdfEl(
    "h3",
    "margin:16px 0 8px 0;font-size:14px;font-weight:700;color:#0f172a;background:#ffffff;border:none;",
    "Leyenda (días)",
  )
  root.appendChild(leyenda)

  const ul = document.createElement("ul")
  ul.style.cssText = "margin:0 0 12px 18px;padding:0;color:#334155;background:#ffffff;border:none;"
  for (const d of resumen.dias) {
    const li = document.createElement("li")
    li.style.cssText =
      "margin-bottom:6px;padding:0;background:#ffffff;border:none;color:#334155;font-size:12px;"
    const c0 = typeof d.color === "string" ? d.color.trim() : ""
    const dot = /^#([0-9A-Fa-f]{3}|[0-9A-Fa-f]{6})$/.test(c0) ? c0 : "#2563eb"
    li.innerHTML = `<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${dot};margin-right:8px;vertical-align:middle;border:1px solid #cbd5e1;"></span><strong style="color:#0f172a;">${escapeHtml(
      d.dia,
    )}</strong> — ${escapeHtml(String(d.km_totales))} km · ${escapeHtml(String(d.clientes_count))} clientes`
    ul.appendChild(li)
  }
  root.appendChild(ul)

  const det = pdfEl(
    "h3",
    "margin:16px 0 8px 0;font-size:14px;font-weight:700;color:#0f172a;background:#ffffff;border:none;",
    "Detalle por día (clientes)",
  )
  root.appendChild(det)

  for (const d of resumen.dias) {
    const sub = pdfEl(
      "div",
      "margin-bottom:10px;padding:8px 10px;background:#f9fafb;border:1px solid #e5e7eb;border-radius:4px;color:#111827;",
    )
    sub.appendChild(
      pdfEl("div", "font-weight:700;margin-bottom:4px;color:#0f172a;background:transparent;border:none;", d.dia),
    )
    const nombres = clientesOrdenados(d)
    if (nombres.length === 0) {
      sub.appendChild(
        pdfEl("div", "font-size:12px;color:#64748b;background:transparent;border:none;", "Sin clientes en ruta."),
      )
    } else {
      const list = document.createElement("ul")
      list.style.cssText = "margin:4px 0 0 16px;padding:0;font-size:11px;color:#334155;background:transparent;"
      for (const n of nombres) {
        const li = document.createElement("li")
        li.style.cssText = "margin-bottom:2px;"
        li.textContent = n
        list.appendChild(li)
      }
      sub.appendChild(list)
    }
    root.appendChild(sub)
  }

  const exp = pdfEl(
    "p",
    "margin:14px 0 0 0;padding:10px;background:#f1f5f9;border:1px solid #e2e8f0;border-radius:4px;font-size:11px;color:#475569;line-height:1.5;",
    TEXTO_METODOLOGIA,
  )
  root.appendChild(exp)

  return root
}

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
}

async function loadImageDataUrl(path: string): Promise<string | null> {
  try {
    const res = await fetch(path)
    if (!res.ok) return null
    const blob = await res.blob()
    return await new Promise((resolve, reject) => {
      const fr = new FileReader()
      fr.onload = () => resolve(fr.result as string)
      fr.onerror = () => reject(new Error("FileReader"))
      fr.readAsDataURL(blob)
    })
  } catch {
    return null
  }
}

export type ExportResumenVendedorPdfParams = {
  resumen: DistribuidoraResumenVendedorJson
  mapElement: HTMLElement
  viaticoClp: number | null
  /** Ruta pública bajo / (ej. /placeholder-logo.png) */
  logoPublicPath?: string
}

/**
 * Genera y descarga un PDF del resumen semanal.
 * El mapa se captura del Leaflet real (con saneo oklch en onclone); el bloque visual del PDF
 * se arma en un contenedor off-screen solo con estilos inline y se vuelve a rasterizar ahí
 * para no depender del tema Tailwind.
 */
export async function exportResumenVendedorPdf(
  params: ExportResumenVendedorPdfParams,
): Promise<void> {
  const [{ jsPDF }, html2canvasMod] = await Promise.all([
    import("jspdf"),
    import("html2canvas"),
  ])
  const html2canvas = html2canvasMod.default
  const { resumen, mapElement, viaticoClp } = params
  const logoPath = params.logoPublicPath ?? "/placeholder-logo.png"

  mapElement.scrollIntoView({ block: "nearest", behavior: "instant" })
  await new Promise((r) => setTimeout(r, 350))

  const mapCanvas = await html2canvas(mapElement, {
    useCORS: true,
    allowTaint: true,
    scale: Math.min(2, Math.max(1, typeof window !== "undefined" ? window.devicePixelRatio : 1.5)),
    logging: false,
    backgroundColor: "#dfe6ee",
    windowWidth: mapElement.scrollWidth,
    windowHeight: mapElement.scrollHeight,
    onclone: (_clonedDoc, clonedElement) => {
      sanitizeCloneColorsForPdf(clonedElement)
      clonedElement.style.setProperty("background-color", "#dfe6ee", "important")
      clonedElement.style.setProperty("color", "#0f172a", "important")
    },
  })
  const mapImageDataUrl = mapCanvas.toDataURL("image/jpeg", 0.88)

  const pdfHost = ensurePdfExportContainer()
  pdfHost.replaceChildren()
  const snapshot = buildPdfExportSnapshot(resumen, viaticoClp, mapImageDataUrl)
  applyBrutalPdfStyles(snapshot)
  pdfHost.appendChild(snapshot)

  await new Promise<void>((resolve) => {
    requestAnimationFrame(() => requestAnimationFrame(() => resolve()))
  })

  let layoutCanvas: HTMLCanvasElement
  try {
    layoutCanvas = await html2canvas(snapshot, {
      useCORS: true,
      allowTaint: true,
      scale: Math.min(1.75, Math.max(1, typeof window !== "undefined" ? window.devicePixelRatio : 1.25)),
      logging: false,
      backgroundColor: "#ffffff",
      windowWidth: snapshot.scrollWidth,
      windowHeight: snapshot.scrollHeight,
    })
  } finally {
    pdfHost.replaceChildren()
  }

  const layoutImgData = layoutCanvas.toDataURL("image/jpeg", 0.88)

  const doc = new jsPDF({
    orientation: "portrait",
    unit: "mm",
    format: "a4",
    compress: true,
  })
  doc.setFont("helvetica")

  let y = MM_MARGIN

  const ensureSpace = (neededMm: number) => {
    if (y + neededMm > PAGE_H_MM - MM_MARGIN) {
      doc.addPage()
      y = MM_MARGIN
    }
  }

  const addParagraph = (text: string, fontSize: number, lineMm: number) => {
    doc.setFontSize(fontSize)
    const lines = doc.splitTextToSize(text, CONTENT_W)
    const blockH = lines.length * lineMm
    ensureSpace(blockH + 2)
    doc.text(lines, MM_MARGIN, y)
    y += blockH + 4
  }

  const addHeading = (text: string, size: number) => {
    ensureSpace(12)
    doc.setFont("helvetica", "bold")
    doc.setFontSize(size)
    doc.text(text, MM_MARGIN, y)
    doc.setFont("helvetica", "normal")
    y += size * 0.45 + 4
  }

  const logoData = await loadImageDataUrl(logoPath)
  if (logoData) {
    try {
      const logoW = 42
      const logoH = 14
      ensureSpace(logoH + 4)
      doc.addImage(logoData, "PNG", MM_MARGIN, y, logoW, logoH)
      y += logoH + 6
    } catch {
      /* sin logo */
    }
  }

  addHeading(`Ruta semanal vendedor ${resumen.vendedor}`, 16)

  addHeading("1. Resumen general", 12)
  doc.setFontSize(10)
  const viaticoLine =
    viaticoClp != null && Number.isFinite(viaticoClp)
      ? formatClp(viaticoClp)
      : "No calculado (defina rendimiento y precio de combustible en pantalla)."
  const resumenLines = [
    `Km total semana: ${resumen.km_total_semana} km`,
    `Clientes (visitas semana): ${resumen.clientes_total_semana}`,
    `Tiempo estimado (conducción): ${resumen.min_total_semana} min`,
    `Viático estimado: ${viaticoLine}`,
  ]
  for (const line of resumenLines) {
    addParagraph(line, 10, 4.5)
  }

  addParagraph(
    "La página siguiente contiene mapa, métricas visuales, leyenda y listado de clientes, rasterizado desde un bloque aislado sin colores oklch (solo hex/rgb inline).",
    9,
    4.2,
  )

  doc.addPage()
  y = MM_MARGIN
  addHeading("2. Mapa y resumen visual", 14)
  const imgW = CONTENT_W
  const imgH = (layoutCanvas.height / layoutCanvas.width) * imgW
  const maxH = PAGE_H_MM - MM_MARGIN * 2 - y
  const finalH = Math.min(imgH, maxH)
  const finalW = (layoutCanvas.width / layoutCanvas.height) * finalH
  doc.addImage(layoutImgData, "JPEG", MM_MARGIN, y, Math.min(finalW, CONTENT_W), finalH)
  y += finalH + 6

  if (y + 28 > PAGE_H_MM - MM_MARGIN) {
    doc.addPage()
    y = MM_MARGIN
  }

  addHeading("3. Explicación simple", 12)
  addParagraph(TEXTO_METODOLOGIA, 10, 4.8)

  const safe = resumen.vendedor.replace(/[^\w.\-]+/g, "_").slice(0, 72) || "vendedor"
  const fecha = new Date().toISOString().slice(0, 10)
  doc.save(`ruta-semanal-${safe}-${fecha}.pdf`)
}
