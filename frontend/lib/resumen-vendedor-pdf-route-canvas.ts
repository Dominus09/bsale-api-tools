/**
 * Mapa raster solo para el PDF: basemap Carto Voyager (calles y etiquetas) + polilíneas ORS
 * y marcadores. Sin Leaflet. No importar desde componentes React.
 */

import type { DistribuidoraResumenDiaJson, DistribuidoraResumenVendedorJson } from "@/lib/api"
import { geometryToLatLngs, type LatLngTuple } from "@/lib/distribuidora-resumen-geometry"
import { diaSemanaSortKey } from "@/lib/resumen-vendedor-pdf-clientes-layout"

const CARTO_VOYAGER_TILE = (sub: string, z: number, x: number, y: number) =>
  `https://${sub}.basemaps.cartocdn.com/rastertiles/voyager/${z}/${x}/${y}.png`

const TILE_SUBS = ["a", "b", "c", "d"] as const
const MAX_TILES = 56

/** Píxeles mundo Web Mercator / esfera (igual que slippy map estándar). */
function latLonToWorldPixel(lat: number, lon: number, z: number): [number, number] {
  const scale = 256 * 2 ** z
  const x = ((lon + 180) / 360) * scale
  const sin = Math.sin((lat * Math.PI) / 180)
  const y = (0.5 - Math.log((1 + sin) / (1 - sin)) / (4 * Math.PI)) * scale
  return [x, y]
}

function worldBoundsForLatLonBox(
  minLat: number,
  maxLat: number,
  minLon: number,
  maxLon: number,
  z: number,
): { minWX: number; maxWX: number; minWY: number; maxWY: number } {
  const corners: [number, number][] = [
    [minLat, minLon],
    [minLat, maxLon],
    [maxLat, minLon],
    [maxLat, maxLon],
  ]
  let minWX = Infinity
  let maxWX = -Infinity
  let minWY = Infinity
  let maxWY = -Infinity
  for (const [la, lo] of corners) {
    const [wx, wy] = latLonToWorldPixel(la, lo, z)
    minWX = Math.min(minWX, wx)
    maxWX = Math.max(maxWX, wx)
    minWY = Math.min(minWY, wy)
    maxWY = Math.max(maxWY, wy)
  }
  return { minWX, maxWX, minWY, maxWY }
}

export type MercatorPick = {
  z: number
  minWX: number
  maxWX: number
  minWY: number
  maxWY: number
}

function pickZoomForBounds(
  minLat: number,
  maxLat: number,
  minLon: number,
  maxLon: number,
): MercatorPick | null {
  for (let z = 17; z >= 5; z--) {
    const b = worldBoundsForLatLonBox(minLat, maxLat, minLon, maxLon, z)
    const minTX = Math.floor(b.minWX / 256)
    const maxTX = Math.floor(b.maxWX / 256)
    const minTY = Math.floor(b.minWY / 256)
    const maxTY = Math.floor(b.maxWY / 256)
    const nx = maxTX - minTX + 1
    const ny = maxTY - minTY + 1
    if (nx >= 1 && ny >= 1 && nx * ny <= MAX_TILES) {
      return { z, ...b }
    }
  }
  return null
}

function mercatorScreenLayout(
  cw: number,
  ch: number,
  padPx: number,
  pick: MercatorPick,
): { s: number; ox: number; oy: number; sw: number; sh: number; minTX: number; minTY: number } {
  const { minWX, maxWX, minWY, maxWY } = pick
  const innerW = cw - 2 * padPx
  const innerH = ch - 2 * padPx
  const sw = Math.max(maxWX - minWX, 1)
  const sh = Math.max(maxWY - minWY, 1)
  const sx = innerW / sw
  const sy = innerH / sh
  const s = Math.min(sx, sy)
  const ox = padPx + (innerW - sw * s) / 2
  const oy = padPx + (innerH - sh * s) / 2
  const minTX = Math.floor(minWX / 256)
  const minTY = Math.floor(minWY / 256)
  return { s, ox, oy, sw, sh, minTX, minTY }
}

async function loadTileBitmap(url: string): Promise<ImageBitmap | null> {
  try {
    const ctrl = new AbortController()
    const t = window.setTimeout(() => ctrl.abort(), 12000)
    const res = await fetch(url, { mode: "cors", signal: ctrl.signal })
    window.clearTimeout(t)
    if (!res.ok) return null
    const blob = await res.blob()
    return await createImageBitmap(blob)
  } catch {
    return null
  }
}

async function drawCartoVoyagerBasemap(
  ctx: CanvasRenderingContext2D,
  cw: number,
  ch: number,
  padPx: number,
  pick: MercatorPick,
): Promise<boolean> {
  const { z, minWX, maxWX, minWY, maxWY } = pick
  const minTX = Math.floor(minWX / 256)
  const maxTX = Math.floor(maxWX / 256)
  const minTY = Math.floor(minWY / 256)
  const maxTY = Math.floor(maxWY / 256)

  const mosaicW = (maxTX - minTX + 1) * 256
  const mosaicH = (maxTY - minTY + 1) * 256
  const mosaic = document.createElement("canvas")
  mosaic.width = mosaicW
  mosaic.height = mosaicH
  const mctx = mosaic.getContext("2d")
  if (!mctx) return false

  mctx.fillStyle = "#dfe9f2"
  mctx.fillRect(0, 0, mosaicW, mosaicH)

  const jobs: Promise<void>[] = []
  for (let tx = minTX; tx <= maxTX; tx++) {
    for (let ty = minTY; ty <= maxTY; ty++) {
      const sub = TILE_SUBS[Math.abs(tx + ty) % 4]
      const url = CARTO_VOYAGER_TILE(sub, z, tx, ty)
      jobs.push(
        (async () => {
          const bmp = await loadTileBitmap(url)
          const dx = (tx - minTX) * 256
          const dy = (ty - minTY) * 256
          if (bmp) mctx.drawImage(bmp, dx, dy)
        })(),
      )
    }
  }
  await Promise.all(jobs)

  const { s, ox, oy, sw, sh, minTX: mtx0, minTY: mty0 } = mercatorScreenLayout(cw, ch, padPx, pick)
  const srcX = minWX - mtx0 * 256
  const srcY = minWY - mty0 * 256
  ctx.drawImage(mosaic, srcX, srcY, sw, sh, ox, oy, sw * s, sh * s)
  return true
}

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

function padLatLonBox(
  minLat: number,
  maxLat: number,
  minLon: number,
  maxLon: number,
  frac: number,
): { minLat: number; maxLat: number; minLon: number; maxLon: number } {
  const dLat = (maxLat - minLat) * frac
  const dLon = (maxLon - minLon) * frac
  return {
    minLat: minLat - dLat,
    maxLat: maxLat + dLat,
    minLon: minLon - dLon,
    maxLon: maxLon + dLon,
  }
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

type ProjectFn = (lat: number, lon: number) => [number, number]

function makeMercatorProjector(cw: number, ch: number, padPx: number, pick: MercatorPick): ProjectFn {
  const { z, minWX, maxWX, minWY, maxWY } = pick
  const { s, ox, oy } = mercatorScreenLayout(cw, ch, padPx, pick)
  return (lat: number, lon: number) => {
    const [wx, wy] = latLonToWorldPixel(lat, lon, z)
    return [ox + (wx - minWX) * s, oy + (wy - minWY) * s]
  }
}

function makeEquirectangularProjector(
  cw: number,
  ch: number,
  padPx: number,
  minLat: number,
  maxLat: number,
  minLon: number,
  maxLon: number,
): ProjectFn {
  const spanLat = Math.max(maxLat - minLat, 1e-9)
  const spanLon = Math.max(maxLon - minLon, 1e-9)
  const innerW = cw - 2 * padPx
  const innerH = ch - 2 * padPx
  const scale = Math.min(innerW / spanLon, innerH / spanLat)
  const offX = padPx + (innerW - spanLon * scale) / 2
  const offY = padPx + (innerH - spanLat * scale) / 2
  return (lat: number, lon: number) => {
    const x = offX + (lon - minLon) * scale
    const y = offY + (maxLat - lat) * scale
    return [x, y]
  }
}

/**
 * Raster del mapa semanal: basemap Carto Voyager (calles y etiquetas) + polilíneas completas
 * por día + marcadores numerados. Vista acotada tipo fitBounds con padding.
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
  const b0 = expandBoundsIfPointlike(minLat, maxLat, minLon, maxLon)
  minLat = b0.minLat
  maxLat = b0.maxLat
  minLon = b0.minLon
  maxLon = b0.maxLon

  const bbox = padLatLonBox(minLat, maxLat, minLon, maxLon, 0.08)

  const canvas = document.createElement("canvas")
  canvas.width = cw
  canvas.height = ch
  const ctx = canvas.getContext("2d")
  if (!ctx) return null

  const picked = pickZoomForBounds(bbox.minLat, bbox.maxLat, bbox.minLon, bbox.maxLon)
  let project: ProjectFn

  if (picked) {
    const ok = await drawCartoVoyagerBasemap(ctx, cw, ch, pad, picked)
    if (ok) {
      project = makeMercatorProjector(cw, ch, pad, picked)
    } else {
      ctx.fillStyle = "#e8eef5"
      ctx.fillRect(0, 0, cw, ch)
      project = makeEquirectangularProjector(cw, ch, pad, bbox.minLat, bbox.maxLat, bbox.minLon, bbox.maxLon)
    }
  } else {
    ctx.fillStyle = "#e8eef5"
    ctx.fillRect(0, 0, cw, ch)
    project = makeEquirectangularProjector(cw, ch, pad, bbox.minLat, bbox.maxLat, bbox.minLon, bbox.maxLon)
  }

  const lineW = Math.max(4.5, Math.min(6.2, cw / 300))
  ctx.lineCap = "round"
  ctx.lineJoin = "round"

  for (const d of sortedDias(resumen)) {
    const route = extractFullRouteLatLngs(d.geometry)
    if (route.length < 2) continue
    const col = strokeColor(typeof d.color === "string" ? d.color : undefined)

    ctx.beginPath()
    const [px0, py0] = project(route[0][0], route[0][1])
    ctx.moveTo(px0, py0)
    for (let i = 1; i < route.length; i++) {
      const [px, py] = project(route[i][0], route[i][1])
      ctx.lineTo(px, py)
    }
    ctx.strokeStyle = "rgba(255,255,255,0.92)"
    ctx.lineWidth = lineW + 4.5
    ctx.stroke()

    ctx.beginPath()
    ctx.moveTo(px0, py0)
    for (let i = 1; i < route.length; i++) {
      const [px, py] = project(route[i][0], route[i][1])
      ctx.lineTo(px, py)
    }
    ctx.strokeStyle = col
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
      ctx.lineWidth = Math.max(2.4, lineW * 0.55)
      ctx.stroke()
      ctx.font = `700 ${Math.round(rPin * 1.35)}px system-ui,Segoe UI,sans-serif`
      ctx.fillStyle = "#0f172a"
      ctx.fillText(String(ov), px, py)
    }
  }

  return canvas.toDataURL("image/jpeg", 0.92)
}
