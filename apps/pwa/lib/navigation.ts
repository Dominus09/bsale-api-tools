/** Construcción de enlaces de navegación (destino = coords del cliente). */

export type NavigationLinks = {
  available: boolean
  destination_latitude: number
  destination_longitude: number
  destination_label?: string | null
  google_maps_place_url: string
  google_maps_directions_url: string
  /** Preferido en UI: place si no hay ruta vial fiable; si no, directions. */
  google_maps_url: string
  likely_no_road_route: boolean
  warning_es?: string | null
  note_es: string
}

/**
 * Solo destinos donde NO hay evidencia de ruta vehicular continua verificada.
 * Quellón, Castro, Ancud y el resto de Chiloé continental/insular con red
 * vial NO deben disparar advertencia por el solo hecho de estar en una isla.
 */
const NO_CONTINUOUS_ROAD_CITY_HINTS = [
  "melinka",
  "repollal",
  "isla Ascensión", // defensive; normalized lowercase below
  "ascensión",
  "ascension",
]

export function likelyUnverifiedRoadDestination(
  city?: string | null,
  lat?: number | null,
): boolean {
  const c = (city || "").trim().toLowerCase()
  if (c && NO_CONTINUOUS_ROAD_CITY_HINTS.some((h) => c.includes(h.toLowerCase()))) {
    return true
  }
  // Melinka ~ -43.89. Latitudes más australes que -43.85 (sur de Quellón ~ -43.1)
  // sin ciudad conocida: tratar como ruta no verificada (archipiélago austral).
  if (lat != null && Number.isFinite(lat) && lat < -43.85) return true
  return false
}

/** @deprecated Prefer likelyUnverifiedRoadDestination — nombre histórico. */
export function likelyIslandDestination(
  city?: string | null,
  lat?: number | null,
): boolean {
  return likelyUnverifiedRoadDestination(city, lat)
}

export function buildGoogleMapsNavigation(opts: {
  latitude: number
  longitude: number
  city?: string | null
  clientName?: string | null
}): NavigationLinks {
  const lat = Number(opts.latitude)
  const lng = Number(opts.longitude)
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
    throw new Error("Coordenadas de destino inválidas")
  }
  if (lat < -90 || lat > 90 || lng < -180 || lng > 180) {
    throw new Error("Coordenadas de destino fuera de rango")
  }

  const query = `${lat},${lng}`
  const encoded = encodeURIComponent(query)
  const place = `https://www.google.com/maps/search/?api=1&query=${encoded}`
  const directions = `https://www.google.com/maps/dir/?api=1&destination=${encoded}&travelmode=driving`
  const unverified = likelyUnverifiedRoadDestination(opts.city, lat)

  return {
    available: true,
    destination_latitude: lat,
    destination_longitude: lng,
    destination_label: opts.clientName || null,
    google_maps_place_url: place,
    google_maps_directions_url: directions,
    // Sin afirmación de “isla”: si no hay ruta verificada, abrir destino exacto.
    google_maps_url: unverified ? place : directions,
    likely_no_road_route: unverified,
    warning_es: unverified
      ? "Ruta vehicular continua no verificada hacia este destino (p. ej. Melinka). Se abre la ubicación exacta; la ruta de Google puede detenerse en el continente."
      : null,
    note_es:
      "Destino = coordenadas del cliente del despacho. No se usa tu GPS como destino.",
  }
}
