import L from "leaflet"

import type { DistribuidoraResumenVendedorJson } from "@/lib/api"
import { geometryToLatLngs } from "@/lib/distribuidora-resumen-geometry"

type LatLngTuple = [number, number]

/**
 * Ajusta zoom y centro del mapa Leaflet para exportar a PDF:
 * incluye geometrías de ruta y puntos de clientes del resumen.
 */
export function fitMapToResumenForPdf(map: L.Map, resumen: DistribuidoraResumenVendedorJson): void {
  const points: LatLngTuple[] = []
  for (const d of resumen.dias ?? []) {
    try {
      points.push(...geometryToLatLngs(d.geometry))
    } catch {
      /* omitir geometría inválida */
    }
    const raw = d.clientes
    if (!Array.isArray(raw)) continue
    for (const c of raw as Record<string, unknown>[]) {
      const lat = Number(c.lat)
      const lon = Number(c.lon)
      if (Number.isFinite(lat) && Number.isFinite(lon)) points.push([lat, lon])
    }
  }
  const base = resumen.dias?.[0]?.base as Record<string, unknown> | undefined
  if (base?.lat != null && base?.lon != null) {
    const lat = Number(base.lat)
    const lon = Number(base.lon)
    if (Number.isFinite(lat) && Number.isFinite(lon)) points.push([lat, lon])
  }

  if (points.length === 0) return
  if (points.length === 1) {
    map.setView(points[0], 14)
    return
  }
  const b = L.latLngBounds(points as L.LatLngExpression[])
  map.fitBounds(b, { padding: [28, 28], maxZoom: 16, animate: false })
}
