/** Distancia en línea recta (haversine). No es distancia de navegación. */

const R_M = 6371000

export function haversineMeters(
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
  return 2 * R_M * Math.asin(Math.min(1, Math.sqrt(a)))
}

export function formatDistanceM(meters: number | null | undefined): string | null {
  if (meters == null || !Number.isFinite(meters)) return null
  if (meters < 1000) return `${Math.round(meters)} m`
  return `${(meters / 1000).toFixed(meters < 10000 ? 1 : 0)} km`
}
