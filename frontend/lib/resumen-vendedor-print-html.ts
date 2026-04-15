"use client"

import type { DistribuidoraResumenDiaJson, DistribuidoraResumenVendedorJson } from "@/lib/api"

import { geometryToLatLngs } from "@/lib/distribuidora-resumen-geometry"

const TEXTO_METODOLOGIA =
  "Las rutas fueron optimizadas considerando distancia entre clientes, punto de partida y retorno al origen, buscando minimizar los kilómetros recorridos y el tiempo de traslado."

function formatClp(n: number): string {
  return Math.round(n).toLocaleString("es-CL", {
    style: "currency",
    currency: "CLP",
    maximumFractionDigits: 0,
  })
}

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
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

function hexStrokeForColor(c: string): string {
  const t = (c || "").trim()
  if (/^#([0-9A-Fa-f]{3}|[0-9A-Fa-f]{6})$/.test(t)) return t
  return "#2563eb"
}

function buildRoutesSvg(resumen: DistribuidoraResumenVendedorJson): string {
  const allPts: { lat: number; lon: number }[] = []
  for (const d of resumen.dias) {
    for (const [lat, lon] of geometryToLatLngs(d.geometry)) {
      allPts.push({ lat, lon })
    }
  }
  if (allPts.length < 2) {
    return `<p class="muted">Sin geometría suficiente para dibujar el esquema de rutas.</p>`
  }

  let minLat = Infinity
  let maxLat = -Infinity
  let minLon = Infinity
  let maxLon = -Infinity
  for (const { lat, lon } of allPts) {
    minLat = Math.min(minLat, lat)
    maxLat = Math.max(maxLat, lat)
    minLon = Math.min(minLon, lon)
    maxLon = Math.max(maxLon, lon)
  }
  const latPad = (maxLat - minLat) * 0.1 + 1e-6
  const lonPad = (maxLon - minLon) * 0.1 + 1e-6
  minLat -= latPad
  maxLat += latPad
  minLon -= lonPad
  maxLon += lonPad

  const sw = 820
  const sh = 360
  const project = (lat: number, lon: number): [number, number] => {
    const x = ((lon - minLon) / (maxLon - minLon)) * sw
    const y = sh - ((lat - minLat) / (maxLat - minLat)) * sh
    return [x, y]
  }

  let polylines = ""
  for (const d of resumen.dias) {
    const pts = geometryToLatLngs(d.geometry)
    if (pts.length < 2) continue
    const stroke = hexStrokeForColor(String(d.color))
    const dAttr = pts
      .map(([lat, lon]) => {
        const [x, y] = project(lat, lon)
        return `${x.toFixed(1)},${y.toFixed(1)}`
      })
      .join(" ")
    polylines += `<polyline fill="none" stroke="${stroke}" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round" points="${dAttr}"/>`
  }

  return `<div class="svg-block">
  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${sw} ${sh}" width="100%" height="360" role="img" aria-label="Esquema de rutas">
    ${polylines}
  </svg>
  <p class="hint">Esquema proporcional de polilíneas por día (sin mapa base ni calles). El mapa interactivo está en la pantalla principal.</p>
</div>`
}

function buildBodyInner(resumen: DistribuidoraResumenVendedorJson, viaticoClp: number | null): string {
  const viaticoLine =
    viaticoClp != null && Number.isFinite(viaticoClp)
      ? formatClp(viaticoClp)
      : "No calculado (defina rendimiento y precio de combustible en la pantalla de resumen)."

  const metrics = [
    `Km total semana: ${escapeHtml(String(resumen.km_total_semana))} km`,
    `Clientes (visitas semana): ${escapeHtml(String(resumen.clientes_total_semana))}`,
    `Tiempo estimado (conducción): ${escapeHtml(String(resumen.min_total_semana))} min`,
    `Viático estimado: ${escapeHtml(viaticoLine)}`,
    `Km día más largo: ${escapeHtml(String(resumen.km_dia_mas_largo))} km`,
    `Km día más corto: ${escapeHtml(String(resumen.km_dia_mas_corto))} km`,
    `Promedio km / día: ${escapeHtml(String(resumen.promedio_km_por_dia))}`,
  ]
    .map((line) => `<div class="metric">${line}</div>`)
    .join("")

  const leyenda = resumen.dias
    .map((d) => {
      const dot = hexStrokeForColor(String(d.color))
      const alert = d.alerta_calidad
        ? ` <span class="warn">(Alerta: ~${escapeHtml(String(d.km_por_cliente))} km/cliente)</span>`
        : ""
      return `<li><span class="dot" style="background:${dot}"></span><strong>${escapeHtml(d.dia)}</strong> — ${escapeHtml(
        String(d.km_totales),
      )} km · ${escapeHtml(String(d.clientes_count))} clientes${alert}</li>`
    })
    .join("")

  const detalleDias = resumen.dias
    .map((d) => {
      const nombres = clientesOrdenados(d)
      const lista =
        nombres.length === 0
          ? `<p class="muted">Sin clientes en ruta.</p>`
          : `<ol class="clientes">${nombres.map((n) => `<li>${escapeHtml(n)}</li>`).join("")}</ol>`
      return `<section class="dia-block"><h3>${escapeHtml(d.dia)}</h3>${lista}</section>`
    })
    .join("")

  return `
  <h1>Ruta semanal — ${escapeHtml(resumen.vendedor)}</h1>
  <p class="sub">Vista para imprimir. Use <strong>Imprimir</strong> del navegador y elija <strong>Guardar como PDF</strong> si lo desea.</p>
  <div class="metrics">${metrics}</div>
  <h2>Esquema de rutas</h2>
  ${buildRoutesSvg(resumen)}
  <h2>Leyenda (días)</h2>
  <ul class="leyenda">${leyenda}</ul>
  <h2>Detalle por día (clientes)</h2>
  ${detalleDias}
  <p class="metodo">${escapeHtml(TEXTO_METODOLOGIA)}</p>
  `
}

const PRINT_CSS = `
@page { size: A4; margin: 12mm; }
* { box-sizing: border-box; }
html, body { margin: 0; padding: 0; }
body {
  font-family: system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  font-size: 13px;
  line-height: 1.45;
  color: #111827;
  background: #ffffff;
  padding: 16px 20px 32px;
  max-width: 900px;
  margin: 0 auto;
}
h1 { font-size: 20px; margin: 0 0 8px 0; color: #0f172a; }
h2 { font-size: 15px; margin: 20px 0 8px 0; color: #0f172a; border-bottom: 1px solid #e5e7eb; padding-bottom: 4px; }
h3 { font-size: 13px; margin: 0 0 6px 0; color: #0f172a; }
.sub { margin: 0 0 16px 0; color: #475569; font-size: 12px; }
.metrics {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 8px 14px;
  padding: 12px;
  background: #f8fafc;
  border: 1px solid #e2e8f0;
  border-radius: 4px;
  margin-bottom: 8px;
}
.metric { font-size: 12px; color: #334155; margin: 0; }
.svg-block { margin: 8px 0 16px 0; }
.svg-block svg { display: block; border: 1px solid #cbd5e1; background: #e8eef5; }
.hint { font-size: 11px; color: #64748b; margin: 6px 0 0 0; }
.leyenda { margin: 0; padding-left: 18px; color: #334155; }
.leyenda li { margin-bottom: 6px; }
.dot {
  display: inline-block;
  width: 10px;
  height: 10px;
  border-radius: 50%;
  margin-right: 8px;
  vertical-align: middle;
  border: 1px solid #cbd5e1;
}
.warn { color: #b45309; font-size: 11px; }
.dia-block {
  margin-bottom: 12px;
  padding: 10px 12px;
  background: #f9fafb;
  border: 1px solid #e5e7eb;
  border-radius: 4px;
  break-inside: avoid;
}
.clientes { margin: 4px 0 0 0; padding-left: 18px; font-size: 11px; color: #334155; }
.clientes li { margin-bottom: 2px; }
.muted { color: #64748b; font-size: 12px; margin: 4px 0; }
.metodo {
  margin-top: 18px;
  padding: 10px 12px;
  background: #f1f5f9;
  border: 1px solid #e2e8f0;
  border-radius: 4px;
  font-size: 11px;
  color: #475569;
  line-height: 1.5;
}
.toolbar {
  position: sticky;
  top: 0;
  z-index: 10;
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  align-items: center;
  padding: 10px 0 14px;
  margin-bottom: 8px;
  background: #ffffff;
  border-bottom: 1px solid #e5e7eb;
}
.toolbar button {
  font: inherit;
  cursor: pointer;
  padding: 8px 14px;
  border-radius: 6px;
  border: 1px solid #2563eb;
  background: #2563eb;
  color: #ffffff;
}
.toolbar button.secondary {
  background: #ffffff;
  color: #1e293b;
  border-color: #cbd5e1;
}
@media print {
  .toolbar { display: none !important; }
  body { padding-top: 0; }
}
`

export function buildResumenVendedorPrintDocumentHtml(
  resumen: DistribuidoraResumenVendedorJson,
  viaticoClp: number | null,
): string {
  const title = `Resumen — ${resumen.vendedor}`
  const inner = buildBodyInner(resumen, viaticoClp)
  return `<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>${escapeHtml(title)}</title>
<style>${PRINT_CSS}</style>
</head>
<body>
<div class="toolbar">
  <button type="button" onclick="window.print()">Imprimir / Guardar como PDF</button>
  <button type="button" class="secondary" onclick="window.close()">Cerrar ventana</button>
</div>
${inner}
</body>
</html>`
}

/** Mensaje si el navegador bloquea la ventana (debe mostrarse fuera del `window.open`). */
export const RESUMEN_VENDEDOR_PRINT_POPUP_BLOCKED =
  "Debes permitir ventanas emergentes para descargar el análisis"

/**
 * Escribe el HTML de impresión en una ventana ya abierta con `window.open` (síncrono en el mismo tick que el click).
 * No llama a `window.open`: debe hacerlo el caller primero para evitar el bloqueador de popups.
 */
export function writeResumenVendedorPrintToWindow(
  targetWindow: Window,
  resumen: DistribuidoraResumenVendedorJson,
  viaticoClp: number | null,
): void {
  const html = buildResumenVendedorPrintDocumentHtml(resumen, viaticoClp)
  const doc = targetWindow.document
  doc.open()
  doc.write(html)
  doc.close()
  targetWindow.focus()
}
