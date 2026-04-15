import polylineModule from "@mapbox/polyline"

export type LatLngTuple = [number, number]

type PolylineDecodeFn = (str: string, precision?: number) => [number, number][]

function getPolylineDecode(): PolylineDecodeFn | null {
  const m = polylineModule as unknown
  if (m && typeof m === "object" && "decode" in m && typeof (m as { decode: unknown }).decode === "function") {
    return (m as { decode: PolylineDecodeFn }).decode.bind(m) as PolylineDecodeFn
  }
  const d = (m as { default?: unknown })?.default
  if (d && typeof d === "object" && "decode" in d && typeof (d as { decode: unknown }).decode === "function") {
    return (d as { decode: PolylineDecodeFn }).decode.bind(d) as PolylineDecodeFn
  }
  return null
}

function decodeEncodedPolylineToLatLngs(encoded: string): LatLngTuple[] {
  const decode = getPolylineDecode()
  if (!decode || !encoded.trim()) return []
  const trimmed = encoded.trim()
  for (const precision of [5, 6] as const) {
    try {
      const decoded = decode(trimmed, precision)
      if (Array.isArray(decoded) && decoded.length >= 2) {
        return decoded.map(([lat, lon]) => [lat, lon] as LatLngTuple)
      }
    } catch {
      /* siguiente precisión */
    }
  }
  try {
    const decoded = decode(trimmed)
    return decoded.map(([lat, lon]) => [lat, lon] as LatLngTuple)
  } catch {
    return []
  }
}

export function geometryToLatLngs(geometry: unknown): LatLngTuple[] {
  if (geometry == null) return []
  if (typeof geometry === "string") {
    return decodeEncodedPolylineToLatLngs(geometry)
  }
  if (typeof geometry === "object" && geometry !== null) {
    const o = geometry as Record<string, unknown>
    if (typeof o.coordinates === "string" && o.coordinates.trim()) {
      return decodeEncodedPolylineToLatLngs(o.coordinates as string)
    }
    if (o.type === "LineString" && Array.isArray(o.coordinates)) {
      const coords = o.coordinates as [number, number][]
      return coords.map(([lon, lat]) => [lat, lon] as LatLngTuple)
    }
  }
  return []
}
