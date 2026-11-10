/**
 * Upstream FastAPI usado SOLO en el servidor Next (rewrite/proxy).
 * Preferir URL interna Coolify/Docker: http://<servicio-api>:8000
 * Nunca hardcodear nombres de contenedor aquí.
 */
export function getApiProxyTarget(): string {
  const internal = (process.env.API_PROXY_TARGET || "").trim().replace(/\/$/, "")
  if (internal) return internal
  const publicUrl = (process.env.NEXT_PUBLIC_API_URL || "").trim().replace(/\/$/, "")
  return publicUrl || "https://api.quillotana.cl"
}

export function isLikelyPublicApiHost(url: string): boolean {
  try {
    const host = new URL(url).hostname.toLowerCase()
    return host === "api.quillotana.cl" || host.endsWith(".quillotana.cl")
  } catch {
    return false
  }
}

export const CONNECT_ERROR_ES =
  "No fue posible conectar con el servidor. Intenta nuevamente."
