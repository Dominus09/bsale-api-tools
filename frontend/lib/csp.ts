import { getApiOrigin } from "@/lib/api-base"

/**
 * Una sola definición de CSP para middleware (y documentación alineada con public/_headers).
 * Incluye el origin del API vía NEXT_PUBLIC_API_URL en build.
 */
export function buildContentSecurityPolicy(): string {
  const apiOrigin = getApiOrigin()
  const vercel = Boolean(process.env.VERCEL)
  const vercelScript = vercel ? " https://va.vercel-scripts.com" : ""
  const vercelConnect = vercel
    ? " https://vitals.vercel-insights.com https://va.vercel-scripts.com"
    : ""

  return [
    "default-src 'self'",
    "base-uri 'self'",
    "form-action 'self'",
    "frame-ancestors 'self'",
    `script-src 'self' 'unsafe-inline' 'unsafe-eval' https://static.cloudflareinsights.com${vercelScript}`,
    // RUM/Web Analytics envía beacons a cloudflareinsights.com/cdn-cgi/rum (no solo static.*)
    `connect-src 'self' https://static.cloudflareinsights.com https://cloudflareinsights.com ${apiOrigin}${vercelConnect}`,
    "img-src 'self' data: blob: https://hebbkx1anhila5yf.public.blob.vercel-storage.com",
    "style-src 'self' 'unsafe-inline'",
    "font-src 'self' data:",
  ].join("; ")
}
