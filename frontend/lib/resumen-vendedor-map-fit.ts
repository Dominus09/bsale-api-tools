import L from "leaflet"

import type { DistribuidoraResumenDiaJson, DistribuidoraResumenVendedorJson } from "@/lib/api"
import { geometryToLatLngs } from "@/lib/distribuidora-resumen-geometry"

type LatLngTuple = [number, number]

function appendBaseFromDia(d: DistribuidoraResumenDiaJson, points: LatLngTuple[]) {
  const base = d?.base as Record<string, unknown> | undefined
  if (!base) return
  const lat = Number(base.lat)
  const lon = Number(base.lon)
  if (Number.isFinite(lat) && Number.isFinite(lon)) points.push([lat, lon])
}

/**
 * Recoge coordenadas para ajustar el mapa: geometría de ruta, clientes y base (por día).
 * `visibleDias`: si se pasa, solo se consideran esos días; si es `null`, todos los días del resumen.
 */
export function collectResumenMapPoints(
  resumen: DistribuidoraResumenVendedorJson | null | undefined,
  visibleDias: Set<string> | null,
): LatLngTuple[] {
  const points: LatLngTuple[] = []
  for (const d of resumen?.dias ?? []) {
    const id = String(d.dia ?? "")
    if (visibleDias != null && !visibleDias.has(id)) continue
    try {
      points.push(...geometryToLatLngs(d.geometry))
    } catch {
      /* geometría inválida */
    }
    const raw = d.clientes
    if (Array.isArray(raw)) {
      for (const c of raw as Record<string, unknown>[]) {
        const lat = Number(c.lat)
        const lon = Number(c.lon)
        if (Number.isFinite(lat) && Number.isFinite(lon)) points.push([lat, lon])
      }
    }
    appendBaseFromDia(d, points)
  }
  return points
}

function maxZoomForPointCount(n: number): number {
  if (n <= 2) return 17
  if (n <= 10) return 16
  if (n <= 35) return 15
  return 14
}

/**
 * Ajusta zoom y centro del mapa a la operación real (rutas + visitas + bases).
 */
export function fitMapToResumenBounds(
  map: L.Map,
  resumen: DistribuidoraResumenVendedorJson,
  options?: {
    /** Días visibles en el mapa; `null` = todos los días del resumen (p. ej. PDF). */
    visibleDias?: Set<string> | null
    padding?: [number, number]
  },
): void {
  const visible = options?.visibleDias === undefined ? null : options.visibleDias
  const padding: [number, number] = options?.padding ?? [50, 50]
  const pts = collectResumenMapPoints(resumen, visible)
  if (pts.length === 0) return

  if (pts.length === 1) {
    map.setView(pts[0], 16, { animate: false })
    return
  }

  const b = L.latLngBounds(pts as L.LatLngExpression[])
  const sw = b.getSouthWest()
  const ne = b.getNorthEast()
  const latSpan = Math.abs(ne.lat - sw.lat)
  const lngSpan = Math.abs(ne.lng - sw.lng)
  const maxSpan = Math.max(latSpan, lngSpan, 1e-6)

  let maxZoom = maxZoomForPointCount(pts.length)
  if (maxSpan < 0.015) maxZoom = Math.max(maxZoom, 17)
  if (maxSpan > 2.5) maxZoom = Math.min(maxZoom, 12)

  map.fitBounds(b, {
    padding,
    maxZoom,
    animate: false,
  })
}

/** @deprecated Usar `fitMapToResumenBounds`; se mantiene para llamadas existentes. */
export function fitMapToResumenForPdf(map: L.Map, resumen: DistribuidoraResumenVendedorJson): void {
  fitMapToResumenBounds(map, resumen, { visibleDias: null, padding: [50, 50] })
}
