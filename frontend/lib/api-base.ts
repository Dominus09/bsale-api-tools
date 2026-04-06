/**
 * Base URL del backend. En producción Coolify define NEXT_PUBLIC_API_URL si el API
 * no es el dominio por defecto (evita fetch a localhost o al host equivocado).
 */
export const DEFAULT_API_URL = "https://api.quillotana.cl"

export function getApiBaseUrl(): string {
  const v = process.env.NEXT_PUBLIC_API_URL?.trim()
  if (v) return v.replace(/\/$/, "")
  return DEFAULT_API_URL
}

export function getApiOrigin(): string {
  try {
    return new URL(getApiBaseUrl()).origin
  } catch {
    return new URL(DEFAULT_API_URL).origin
  }
}
