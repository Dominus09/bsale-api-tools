import { getApiProxyTarget } from "./api-proxy-target"

/** Base URL for API calls: same-origin proxy when FE ≠ API host. */
export function getApiBase(): string {
  // Servidor Next: preferir upstream interno (API_PROXY_TARGET) para no depender
  // del DNS público de api.quillotana.cl (EAI_AGAIN intermitente).
  if (typeof window === "undefined") {
    return getApiProxyTarget()
  }

  const configured = (process.env.NEXT_PUBLIC_API_URL || "").trim().replace(/\/$/, "")
  if (!configured) return "/api-upstream"
  try {
    const apiOrigin = new URL(configured).origin
    if (apiOrigin !== window.location.origin) return "/api-upstream"
  } catch {
    return "/api-upstream"
  }
  return configured
}
