const TOKEN_KEY = "pwa_token"
const EMAIL_KEY = "pwa_email"
const USER_ID_KEY = "pwa_user_id"

export type AuthMe = {
  id: number | null
  email: string
  role?: string | null
  apps: string[]
  profiles: string[]
  has_erp_access: boolean
  has_pwa_access: boolean
}

export function getToken(): string | null {
  if (typeof window === "undefined") return null
  return localStorage.getItem(TOKEN_KEY)
}

export function setSession(token: string, email: string, userId: number | string) {
  localStorage.setItem(TOKEN_KEY, token)
  localStorage.setItem(EMAIL_KEY, email)
  localStorage.setItem(USER_ID_KEY, String(userId))
}

export function clearSession() {
  localStorage.removeItem(TOKEN_KEY)
  localStorage.removeItem(EMAIL_KEY)
  localStorage.removeItem(USER_ID_KEY)
}

export function authHeaders(): HeadersInit {
  const token = getToken()
  const headers: Record<string, string> = {
    Accept: "application/json",
    "Content-Type": "application/json",
  }
  if (token) headers.Authorization = `Bearer ${token}`
  return headers
}

export function isUnauthorized(status: number): boolean {
  return status === 401
}
