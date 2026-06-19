/** Detección de clientes aislados en ruta ORS (venta baja + lejos del grupo principal). */

import type { RouteClientRow } from "@/lib/ors-map-ui"

export const ISOLATED_MAX_VENTA_CLP = 50_000
export const ISOLATED_MIN_DISTANCE_KM = 10

const EARTH_RADIUS_KM = 6371

export function haversineKm(
  lat1: number,
  lng1: number,
  lat2: number,
  lng2: number,
): number {
  const toRad = (d: number) => (d * Math.PI) / 180
  const dLat = toRad(lat2 - lat1)
  const dLng = toRad(lng2 - lng1)
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2
  return EARTH_RADIUS_KM * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
}

type GeoPoint = { lat: number; lng: number }

function centroid(points: GeoPoint[]): GeoPoint | null {
  if (points.length === 0) return null
  let lat = 0
  let lng = 0
  for (const p of points) {
    lat += p.lat
    lng += p.lng
  }
  return { lat: lat / points.length, lng: lng / points.length }
}

type ClientGeo = Pick<RouteClientRow, "client_id" | "venta_total" | "lat" | "lng">

function geoPoint(c: ClientGeo): GeoPoint | null {
  if (c.lat == null || c.lng == null) return null
  if (!Number.isFinite(c.lat) || !Number.isFinite(c.lng)) return null
  return { lat: c.lat, lng: c.lng }
}

/**
 * Clientes con venta &lt; 50k y distancia haversine &gt; 10 km desde el centroide
 * del grupo principal (venta ≥ 50k) o, si no hay, desde el resto de paradas.
 */
export function detectIsolatedClientDistances(
  clientRows: ClientGeo[],
): Map<number, number> {
  const out = new Map<number, number>()
  const mainGroup = clientRows.filter(
    (c) => c.venta_total >= ISOLATED_MAX_VENTA_CLP && geoPoint(c),
  )

  for (const c of clientRows) {
    if (c.venta_total >= ISOLATED_MAX_VENTA_CLP) continue
    const point = geoPoint(c)
    if (!point) continue

    const referenceClients =
      mainGroup.length > 0
        ? mainGroup
        : clientRows.filter((x) => x.client_id !== c.client_id)

    const refPoints = referenceClients
      .map(geoPoint)
      .filter((p): p is GeoPoint => p != null)
    if (refPoints.length === 0) continue

    const center = centroid(refPoints)
    if (!center) continue

    const km = haversineKm(center.lat, center.lng, point.lat, point.lng)
    if (km > ISOLATED_MIN_DISTANCE_KM) {
      out.set(c.client_id, Math.round(km * 10) / 10)
    }
  }

  return out
}

export function countIsolatedClients(clientRows: Pick<RouteClientRow, "isolated">[]): number {
  return clientRows.filter((c) => c.isolated).length
}
