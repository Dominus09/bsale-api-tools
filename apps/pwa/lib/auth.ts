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

export function getSessionEmail(): string | null {
  if (typeof window === "undefined") return null
  return localStorage.getItem(EMAIL_KEY)
}

export function getSessionUserId(): string | null {
  if (typeof window === "undefined") return null
  return localStorage.getItem(USER_ID_KEY)
}

export function setSession(token: string, email: string, userId: number | string) {
  localStorage.setItem(TOKEN_KEY, token)
  localStorage.setItem(EMAIL_KEY, email)
  localStorage.setItem(USER_ID_KEY, String(userId))
}

/**
 * Limpia credenciales PWA en el dispositivo.
 *
 * Auth PWA = Bearer JWT en localStorage (no cookies HttpOnly).
 * No existe endpoint PWA de logout/revocación inmediata: el JWT es
 * stateless hasta expirar o invalidarse por token_version en admin.
 * No se llama a bump_token_version aquí para no cerrar ERP u otras apps
 * que compartan el mismo token staff.
 */
export function clearSession() {
  if (typeof window === "undefined") return
  const uid = localStorage.getItem(USER_ID_KEY)
  localStorage.removeItem(TOKEN_KEY)
  localStorage.removeItem(EMAIL_KEY)
  localStorage.removeItem(USER_ID_KEY)
  try {
    sessionStorage.clear()
  } catch {
    /* private mode / quota */
  }
  if (uid) {
    try {
      localStorage.removeItem(`lq:ops:activePlan:${uid}`)
    } catch {
      /* ignore */
    }
    void import("./offline-store")
      .then((m) => m.clearOfflineForUser(uid))
      .catch(() => {
        /* ignore */
      })
  }
}

/**
 * Cierre de sesión local + redirección a login.
 * Usa replace para reducir historial con pantallas privadas (Safari / PWA).
 */
export function logoutAndRedirect(loginPath = "/login") {
  clearSession()
  if (typeof window === "undefined") return
  void clearAppCachesBestEffort()
  window.location.replace(loginPath)
}

async function clearAppCachesBestEffort() {
  try {
    if (!("caches" in window)) return
    const keys = await caches.keys()
    await Promise.all(keys.map((k) => caches.delete(k)))
  } catch {
    /* best effort */
  }
}

/** Auth sin Content-Type: obligatorio para FormData (multipart + boundary del browser). */
export function authHeaders(): HeadersInit {
  const token = getToken()
  const headers: Record<string, string> = {
    Accept: "application/json",
  }
  if (token) headers.Authorization = `Bearer ${token}`
  return headers
}

/** Auth + JSON: solo para cuerpos application/json. */
export function jsonAuthHeaders(): HeadersInit {
  return {
    ...authHeaders(),
    "Content-Type": "application/json",
  }
}

export function isUnauthorized(status: number): boolean {
  return status === 401
}
