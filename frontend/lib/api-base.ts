/**
 * Base URL del backend. En producción Coolify define NEXT_PUBLIC_API_URL si el API
 * no es el dominio por defecto (evita fetch a localhost o al host equivocado).
 *
 * En el **navegador**, si el API es otro origen (p. ej. test.quillotana.cl → api.quillotana.cl),
 * se usa el prefijo `/api-upstream` (rewrite en next.config.mjs) para que el fetch sea same-origin
 * y no dependa de CORS ni de cabeceras del API detrás de proxies.
 *
 * Desactivar: NEXT_PUBLIC_API_NO_PROXY=1
 */
export const DEFAULT_API_URL = "https://api.quillotana.cl"

/** Prefijo servido por Next; reescribe a NEXT_PUBLIC_API_URL (o default). */
export const BROWSER_API_PROXY_PREFIX = "/api-upstream"

export function getApiBaseUrl(): string {
  const noProxy = process.env.NEXT_PUBLIC_API_NO_PROXY === "1"
  const envUrl = process.env.NEXT_PUBLIC_API_URL?.trim().replace(/\/$/, "")
  const fallback = DEFAULT_API_URL.replace(/\/$/, "")
  const target = envUrl || fallback

  if (typeof window !== "undefined" && !noProxy) {
    try {
      const apiOrigin = new URL(target).origin
      if (apiOrigin !== window.location.origin) {
        return BROWSER_API_PROXY_PREFIX
      }
      return target
    } catch {
      return target
    }
  }

  if (envUrl) return envUrl
  return fallback
}

export function getApiOrigin(): string {
  try {
    return new URL(getApiBaseUrl()).origin
  } catch {
    return new URL(DEFAULT_API_URL).origin
  }
}
