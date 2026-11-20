import { getApiProxyTarget } from "./api-proxy-target"

/** Base URL for API calls: same-origin proxy when FE ≠ API host. */
export function getApiBase(): string {
  // Servidor Next: preferir upstream interno (API_PROXY_TARGET) para no depender
  // del DNS público de api.quillotana.cl (EAI_AGAIN intermitente).
  if (typeof window === "undefined") {
    return getApiProxyTarget()
  }

  // Laboratorio 2A.5: API aislada en otro origen con CORS (evita multipart vía proxy).
  // Nunca apunta a api.quillotana.cl. Solo si la env está definida en build/dev lab.
  const labDirect = (process.env.NEXT_PUBLIC_LAB_API_DIRECT || "").trim().replace(/\/$/, "")
  if (labDirect) {
    try {
      const host = new URL(labDirect).hostname.toLowerCase()
      if (
        host === "127.0.0.1" ||
        host === "localhost" ||
        host.endsWith(".lq-ops.dev")
      ) {
        return labDirect
      }
    } catch {
      /* ignore */
    }
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
