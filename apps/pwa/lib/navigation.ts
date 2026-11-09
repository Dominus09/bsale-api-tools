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

const ISLAND_CITY_HINTS = [
  "melinka",
  "achao",
  "quellón",
  "quellon",
  "castro",
  "dalcahue",
  "curaco",
  "quinchao",
  "chaulínec",
  "chaulinec",
]

export function likelyIslandDestination(city?: string | null, lat?: number | null): boolean {
  const c = (city || "").trim().toLowerCase()
  if (c && ISLAND_CITY_HINTS.some((h) => c.includes(h))) return true
  // Melinka ~ -43.89; Ancud ~ -41.87. Latitudes más australes que -43.5 suelen ser islas del archipiélago.
  if (lat != null && Number.isFinite(lat) && lat < -43.5) return true
  return false
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
  const island = likelyIslandDestination(opts.city, lat)

  return {
    available: true,
    destination_latitude: lat,
    destination_longitude: lng,
    destination_label: opts.clientName || null,
    google_maps_place_url: place,
    google_maps_directions_url: directions,
    google_maps_url: island ? place : directions,
    likely_no_road_route: island,
    warning_es: island
      ? "Destino insular o sin ruta terrestre continua. Se muestra la ubicación exacta; la ruta vehicular de Google puede detenerse en el continente (p. ej. Ancud)."
      : null,
    note_es:
      "El destino son las coordenadas del cliente del despacho. No se usa tu GPS como destino.",
  }
}
