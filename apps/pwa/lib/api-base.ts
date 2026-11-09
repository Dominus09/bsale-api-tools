/** Base URL for API calls: same-origin proxy when FE ≠ API host. */
export function getApiBase(): string {
  const configured = (process.env.NEXT_PUBLIC_API_URL || "").trim().replace(/\/$/, "")
  if (typeof window === "undefined") {
    return configured || "https://api.quillotana.cl"
  }
  if (!configured) return "/api-upstream"
  try {
    const apiOrigin = new URL(configured).origin
    if (apiOrigin !== window.location.origin) return "/api-upstream"
  } catch {
    return "/api-upstream"
  }
  return configured
}
