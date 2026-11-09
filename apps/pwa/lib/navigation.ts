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
 * Destinos con tratamiento especial solo cuando el nombre identifica Melinka
 * (o localidades del archipiélago de las Guaitecas asociadas a esa ruta).
 * NO usar latitud sola: coords australes fuera de Melinka no deben clasificarse
 * como sin ruta terrestre continua.
 */
const MELINKA_CITY_HINTS = ["melinka", "repollal"]

export function isMelinkaDestination(city?: string | null): boolean {
  const c = (city || "").trim().toLowerCase()
  if (!c) return false
  return MELINKA_CITY_HINTS.some((h) => c.includes(h))
}

/**
 * True solo con identificación confiable de Melinka/Guaitecas por ciudad.
 * Latitud NO se usa como proxy de “isla / sin ruta”.
 */
export function likelyUnverifiedRoadDestination(
  city?: string | null,
  _lat?: number | null,
): boolean {
  return isMelinkaDestination(city)
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
  const melinka = isMelinkaDestination(opts.city)
  const cityKnown = Boolean((opts.city || "").trim())

  // Sin evidencia confiable de conectividad vial (p. ej. sin ciudad): abrir
  // ubicación exacta, sin afirmar que el destino sea insular o imposible.
  // Melinka (identificado por nombre): misma UX de ubicación exacta + aviso acotado.
  // Quellón/Castro/Ancud y resto con ciudad conocida no-Melinka: directions.
  const preferExactLocation = melinka || !cityKnown

  return {
    available: true,
    destination_latitude: lat,
    destination_longitude: lng,
    destination_label: opts.clientName || null,
    google_maps_place_url: place,
    google_maps_directions_url: directions,
    google_maps_url: preferExactLocation ? place : directions,
    likely_no_road_route: preferExactLocation,
    warning_es: melinka
      ? "Destino Melinka: no hay verificación de ruta vehicular continua. Se abre la ubicación exacta."
      : null,
    note_es:
      "Destino = coordenadas del cliente del despacho. No se usa tu GPS como destino.",
  }
}
