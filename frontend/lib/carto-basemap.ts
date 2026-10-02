/**
 * URLs de basemaps raster CARTO con API key (`NEXT_PUBLIC_CARTO_BASEMAPS_KEY`).
 *
 * La key es pública por diseño (viaja en cada request de tile desde el navegador); se protege
 * restringiéndola por dominio en el dashboard de CARTO. Nunca se loguea su valor.
 * `NEXT_PUBLIC_*` se incrusta en tiempo de build: debe existir al construir el frontend.
 */

export type CartoBasemapStyle = "voyager" | "light_all"

const STYLE_PATH: Record<CartoBasemapStyle, string> = {
  voyager: "rastertiles/voyager",
  light_all: "light_all",
}

export const CARTO_SUBDOMAINS = ["a", "b", "c", "d"] as const

export const CARTO_ATTRIBUTION_TEXT = "© OpenStreetMap contributors © CARTO"

export const CARTO_ATTRIBUTION_HTML =
  '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'

const MISSING_KEY_MESSAGE =
  "Falta NEXT_PUBLIC_CARTO_BASEMAPS_KEY: los mapas CARTO mostrarán la marca 'API KEY REQUIRED'. " +
  "Defínela en el entorno de build (Coolify) o en frontend/.env.local para desarrollo."

let warnedMissingKey = false

function cartoKeyQuery(): string {
  const key = (process.env.NEXT_PUBLIC_CARTO_BASEMAPS_KEY ?? "").trim()
  if (key) return `?key=${encodeURIComponent(key)}`
  if (process.env.NODE_ENV === "development") {
    throw new Error(MISSING_KEY_MESSAGE)
  }
  if (!warnedMissingKey) {
    warnedMissingKey = true
    console.warn(MISSING_KEY_MESSAGE)
  }
  return ""
}

/** Plantilla Leaflet (`{s}`, `{z}`, `{x}`, `{y}` y opcionalmente `{r}`) con la key incluida. */
export function cartoTileUrlTemplate(
  style: CartoBasemapStyle,
  options: { retina?: boolean } = {},
): string {
  const retina = options.retina ?? true
  return `https://{s}.basemaps.cartocdn.com/${STYLE_PATH[style]}/{z}/{x}/{y}${retina ? "{r}" : ""}.png${cartoKeyQuery()}`
}

/** URL concreta de un tile (para dibujar en canvas sin Leaflet). */
export function cartoTileUrl(
  style: CartoBasemapStyle,
  sub: string,
  z: number,
  x: number,
  y: number,
): string {
  return `https://${sub}.basemaps.cartocdn.com/${STYLE_PATH[style]}/${z}/${x}/${y}.png${cartoKeyQuery()}`
}
