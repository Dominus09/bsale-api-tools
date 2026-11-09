import { authHeaders, clearSession, isUnauthorized, type AuthMe } from "./auth"
import { getApiBase } from "./api-base"

export class ApiError extends Error {
  status: number
  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

async function parseError(res: Response): Promise<string> {
  const raw = await res.text().catch(() => "")
  try {
    const j = JSON.parse(raw) as { detail?: unknown }
    const d = j.detail
    if (typeof d === "string") return d
    if (d && typeof d === "object" && !Array.isArray(d) && "message" in d) {
      return String((d as { message?: string }).message || raw)
    }
  } catch {
    /* raw */
  }
  return raw.slice(0, 240) || `Error ${res.status}`
}

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${getApiBase()}${path}`, {
    ...init,
    headers: { ...authHeaders(), ...(init?.headers || {}) },
  })
  if (isUnauthorized(res.status)) {
    clearSession()
    if (typeof window !== "undefined") {
      window.location.href = "/login?expired=1"
    }
    throw new ApiError(401, "Sesión expirada")
  }
  if (!res.ok) {
    throw new ApiError(res.status, await parseError(res))
  }
  return res.json() as Promise<T>
}

export type LoginResponse = {
  token: string
  email: string
  role?: string
  id: number
}

export async function login(email: string, password: string): Promise<LoginResponse> {
  const res = await fetch(`${getApiBase()}/login`, {
    method: "POST",
    headers: { Accept: "application/json", "Content-Type": "application/json" },
    body: JSON.stringify({ email, password }),
  })
  if (!res.ok) throw new ApiError(res.status, await parseError(res))
  return res.json()
}

export async function fetchMe(): Promise<AuthMe> {
  return apiFetch<AuthMe>("/auth/me")
}

export type DriverDespacho = {
  id: number
  planning_code?: string | null
  planning_name?: string | null
  planning_date?: string | null
  truck_id?: number | null
  truck_name?: string | null
  truck_plate?: string | null
  route_name?: string | null
  status?: string | null
  driver_user_id?: number | null
  driver_name?: string | null
  confirmed_at?: string | null
  order_count?: number | null
  client_count?: number | null
}

export async function listMyDespachos(): Promise<{
  items: DriverDespacho[]
  count: number
  driver_user_id: number
}> {
  return apiFetch("/distribuidora/pwa/driver/despachos")
}

export async function getMyDespacho(planId: number): Promise<{
  plan: DriverDespacho
  driver_user_id: number
  read_only: boolean
  nav_placeholders?: { mapa?: boolean; clientes?: boolean; note_es?: string }
}> {
  return apiFetch(`/distribuidora/pwa/driver/despachos/${planId}`)
}
