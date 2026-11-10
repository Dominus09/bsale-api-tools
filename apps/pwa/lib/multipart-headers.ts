/**
 * Headers seguros para POST multipart (FormData).
 * Nunca incluir Content-Type: el browser debe poner boundary.
 */
export function multipartAuthHeaders(
  auth: HeadersInit | Record<string, string>,
): Record<string, string> {
  const raw =
    auth instanceof Headers
      ? Object.fromEntries(auth.entries())
      : Array.isArray(auth)
        ? Object.fromEntries(auth)
        : { ...auth }
  const out: Record<string, string> = {}
  for (const [k, v] of Object.entries(raw)) {
    if (k.toLowerCase() === "content-type") continue
    out[k] = String(v)
  }
  if (!out.Accept && !out.accept) out.Accept = "application/json"
  return out
}

export function hasJsonContentType(headers: Record<string, string>): boolean {
  const ct = headers["Content-Type"] || headers["content-type"] || ""
  return ct.toLowerCase().includes("application/json")
}
