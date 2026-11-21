/**
 * Identidad de build para navegación offline (HTML/chunks).
 * Independiente de los datos de negocio en IndexedDB.
 */

export type OfflineBuildInfo = {
  navBuildId: string
  swVersionPrefix: string
}

const FALLBACK: OfflineBuildInfo = {
  navBuildId: "dev",
  swVersionPrefix: "lq-pwa-v9-safari-no-redirect",
}

let cached: OfflineBuildInfo | null = null

export async function loadOfflineBuildInfo(): Promise<OfflineBuildInfo> {
  if (cached) return cached
  if (typeof fetch === "undefined") {
    cached = FALLBACK
    return cached
  }
  try {
    const res = await fetch("/offline-build.json", { cache: "no-store" })
    if (!res.ok) {
      cached = FALLBACK
      return cached
    }
    const json = (await res.json()) as Partial<OfflineBuildInfo>
    cached = {
      navBuildId: String(json.navBuildId || FALLBACK.navBuildId),
      swVersionPrefix: String(json.swVersionPrefix || FALLBACK.swVersionPrefix),
    }
    return cached
  } catch {
    cached = FALLBACK
    return cached
  }
}

/** Solo tests: reinicia cache de módulo. */
export function resetOfflineBuildCacheForTests() {
  cached = null
}

/**
 * ¿La UI puede afirmar "listo sin conexión" para este ready + build actual?
 * Conserva dataComplete aunque el build haya cambiado (no borra fichas).
 */
export function isOfflineUiReady(
  ready: {
    complete?: boolean
    dataComplete?: boolean
    navReady?: boolean
    navBuildId?: string | null
  } | null
  | undefined,
  currentBuildId: string,
): boolean {
  if (!ready) return false
  const dataOk = ready.dataComplete ?? ready.complete
  const navOk = ready.navReady ?? ready.complete
  const buildOk =
    typeof ready.navBuildId === "string" &&
    ready.navBuildId.length > 0 &&
    ready.navBuildId === currentBuildId
  return Boolean(dataOk && navOk && buildOk)
}
