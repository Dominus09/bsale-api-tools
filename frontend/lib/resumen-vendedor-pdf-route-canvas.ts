/**
 * Mapa raster solo para el PDF: polilíneas completas desde `geometry` (ORS / encoded polyline),
 * sin Leaflet. No importar desde componentes React.
 */

import type { DistribuidoraResumenDiaJson, DistribuidoraResumenVendedorJson } from "@/lib/api"
import { geometryToLatLngs, type LatLngTuple } from "@/lib/distribuidora-resumen-geometry"
import { diaSemanaSortKey } from "@/lib/resumen-vendedor-pdf-clientes-layout"

/** Decodifica geometría completa: polyline ORS, LineString, MultiLineString (concatena tramos en orden). */
export function extractFullRouteLatLngs(geometry: unknown): LatLngTuple[] {
  const decoded = geometryToLatLngs(geometry)
  if (decoded.length >= 2) return decoded

  if (!geometry || typeof geometry !== "object") return []
  const o = geometry as Record<string, unknown>

  if (o.type === "MultiLineString" && Array.isArray(o.coordinates)) {
    const out: LatLngTuple[] = []
    for (const line of o.coordinates as unknown[]) {
      if (!Array.isArray(line)) continue
      for (const pt of line) {
        if (!Array.isArray(pt) || pt.length < 2) continue
        const lon = Number(pt[0])
        const lat = Number(pt[1])
        if (Number.isFinite(lat) && Number.isFinite(lon)) out.push([lat, lon])
      }
    }
    return out
  }

  if (o.type === "GeometryCollection" && Array.isArray(o.geometries)) {
    const out: LatLngTuple[] = []
    for (const g of o.geometries as unknown[]) {
      out.push(...extractFullRouteLatLngs(g))
    }
    return out
  }

  return []
}

function appendBasePoints(d: DistribuidoraResumenDiaJson, into: LatLngTuple[]) {
  const base = d?.base as Record<string, unknown> | undefined
  if (!base) return
  const lat = Number(base.lat)
  const lon = Number(base.lon)
  if (Number.isFinite(lat) && Number.isFinite(lon)) into.push([lat, lon])
}

function collectBoundsPoints(resumen: DistribuidoraResumenVendedorJson): LatLngTuple[] {
  const pts: LatLngTuple[] = []
  for (const d of resumen.dias ?? []) {
    pts.push(...extractFullRouteLatLngs(d.geometry))
    const raw = d.clientes
    if (Array.isArray(raw)) {
      for (const c of raw as Record<string, unknown>[]) {
        const lat = Number(c.lat)
        const lon = Number(c.lon)
        if (Number.isFinite(lat) && Number.isFinite(lon)) pts.push([lat, lon])
      }
    }
    appendBasePoints(d, pts)
  }
  return pts
}

function expandBoundsIfPointlike(
  minLat: number,
  maxLat: number,
  minLon: number,
  maxLon: number,
): { minLat: number; maxLat: number; minLon: number; maxLon: number } {
  let dLat = maxLat - minLat
  let dLon = maxLon - minLon
  const minSpan = 0.012
  if (dLat < minSpan) {
    const pad = (minSpan - dLat) / 2
    minLat -= pad
    maxLat += pad
    dLat = maxLat - minLat
  }
  if (dLon < minSpan) {
    const pad = (minSpan - dLon) / 2
    minLon -= pad
    maxLon += pad
  }
  return { minLat, maxLat, minLon, maxLon }
}

function strokeColor(hex: string | undefined): string {
  const t = String(hex ?? "").trim()
  if (/^#([0-9A-Fa-f]{6}|[0-9A-Fa-f]{3})$/.test(t)) return t
  return "#2563eb"
}

function sortedDias(resumen: DistribuidoraResumenVendedorJson): DistribuidoraResumenDiaJson[] {
  return [...(resumen.dias ?? [])].sort((a, b) => {
    const ka = diaSemanaSortKey(String(a.dia ?? ""))
    const kb = diaSemanaSortKey(String(b.dia ?? ""))
    if (ka !== kb) return ka - kb
    return String(a.dia ?? "").localeCompare(String(b.dia ?? ""), "es")
  })
}

function sortedClientesForDia(dia: DistribuidoraResumenDiaJson): Record<string, unknown>[] {
  const raw = dia.clientes
  if (!Array.isArray(raw)) return []
  const rows = raw as Record<string, unknown>[]
  return [...rows].sort(
    (a, b) =>
      (Number(a.orden_manual ?? a.orden_visita) || 0) - (Number(b.orden_manual ?? b.orden_visita) || 0),
  )
}

/**
 * Raster del mapa semanal: una polilínea continua por día (toda la geometría decodificada),
 * marcadores numerados encima, vista tipo fitBounds con padding en píxeles.
 */
export async function buildPdfWeeklyRouteMapDataUrl(
  resumen: DistribuidoraResumenVendedorJson,
  options: { width: number; height: number; paddingPx?: number },
): Promise<string | null> {
  if (typeof document === "undefined") return null

  const { width: cw, height: ch } = options
  const pad = Math.max(16, Math.min(40, options.paddingPx ?? 28))

  const boundsPts = collectBoundsPoints(resumen)
  if (boundsPts.length === 0) return null

  let minLat = Infinity
  let maxLat = -Infinity
  let minLon = Infinity
  let maxLon = -Infinity
  for (const [lat, lon] of boundsPts) {
    minLat = Math.min(minLat, lat)
    maxLat = Math.max(maxLat, lat)
    minLon = Math.min(minLon, lon)
    maxLon = Math.max(maxLon, lon)
  }
  const b = expandBoundsIfPointlike(minLat, maxLat, minLon, maxLon)
  minLat = b.minLat
  maxLat = b.maxLat
  minLon = b.minLon
  maxLon = b.maxLon

  const spanLat = Math.max(maxLat - minLat, 1e-9)
  const spanLon = Math.max(maxLon - minLon, 1e-9)
  const innerW = cw - 2 * pad
  const innerH = ch - 2 * pad
  const scale = Math.min(innerW / spanLon, innerH / spanLat)
  const offX = pad + (innerW - spanLon * scale) / 2
  const offY = pad + (innerH - spanLat * scale) / 2

  const project = (lat: number, lon: number): [number, number] => {
    const x = offX + (lon - minLon) * scale
    const y = offY + (maxLat - lat) * scale
    return [x, y]
  }

  const canvas = document.createElement("canvas")
  canvas.width = cw
  canvas.height = ch
  const ctx = canvas.getContext("2d")
  if (!ctx) return null

  ctx.fillStyle = "#e8eef5"
  ctx.fillRect(0, 0, cw, ch)

  const lineW = Math.max(3.5, Math.min(6, cw / 320))
  ctx.lineCap = "round"
  ctx.lineJoin = "round"

  for (const d of sortedDias(resumen)) {
    const route = extractFullRouteLatLngs(d.geometry)
    if (route.length < 2) continue
    ctx.beginPath()
    const [x0, y0] = project(route[0][0], route[0][1])
    ctx.moveTo(x0, y0)
    for (let i = 1; i < route.length; i++) {
      const [x, y] = project(route[i][0], route[i][1])
      ctx.lineTo(x, y)
    }
    ctx.strokeStyle = strokeColor(typeof d.color === "string" ? d.color : undefined)
    ctx.lineWidth = lineW
    ctx.stroke()
  }

  const rPin = Math.max(9, Math.min(13, cw / 160))
  ctx.textAlign = "center"
  ctx.textBaseline = "middle"

  for (const d of sortedDias(resumen)) {
    const col = strokeColor(typeof d.color === "string" ? d.color : undefined)
    let idx = 0
    for (const c of sortedClientesForDia(d)) {
      const lat = Number(c.lat)
      const lon = Number(c.lon)
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue
      const ov = Number(c.orden_manual ?? c.orden_visita) || idx + 1
      idx += 1
      const [px, py] = project(lat, lon)
      ctx.beginPath()
      ctx.arc(px, py, rPin, 0, Math.PI * 2)
      ctx.fillStyle = "#ffffff"
      ctx.fill()
      ctx.strokeStyle = col
      ctx.lineWidth = Math.max(2.2, lineW * 0.55)
      ctx.stroke()
      ctx.font = `700 ${Math.round(rPin * 1.35)}px system-ui,Segoe UI,sans-serif`
      ctx.fillStyle = "#0f172a"
      ctx.fillText(String(ov), px, py)
    }
  }

  return canvas.toDataURL("image/jpeg", 0.92)
}
