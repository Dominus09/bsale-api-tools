import { getApiBaseUrl } from "@/lib/api-base"
import { getAuthHeaders, logout } from "@/lib/api"

const API_URL = getApiBaseUrl()

export type AdminCompanyRef = {
  company_id: number
  name: string | null
  active?: boolean
}

export type AdminUserRow = {
  id: number
  email: string
  role: string
  active: boolean
  created_at: string | null
  nombre_usuario: string | null
  companies: AdminCompanyRef[]
  last_login_at: string | null
  delivery?: AdminDeliveryPayload
}

export type AdminDeliveryPayload = {
  token_id: number
  purpose: string
  expires_at: string
  one_time_token: string
  delivery_hint: string
}

export type AdminAuthEvent = {
  id: number
  event_type: string
  user_id: number | null
  email: string | null
  success: boolean | null
  ip: string | null
  user_agent: string | null
  actor_user_id: number | null
  detail: Record<string, unknown>
  created_at: string | null
}

export type RolesCatalog = {
  assignable_roles: string[]
  module_matrix: Array<{
    role: string
    label: string
    modules: Record<string, boolean>
  }>
  notes: string[]
}

class AdminUsersApiError extends Error {
  status: number
  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

async function parseJson<T>(res: Response): Promise<T> {
  const text = await res.text()
  if (res.status === 401 && typeof window !== "undefined") {
    logout()
    window.location.href = "/login"
  }
  let data: unknown = null
  try {
    data = text ? JSON.parse(text) : null
  } catch {
    data = null
  }
  if (!res.ok) {
    const detail =
      data && typeof data === "object" && data !== null && "detail" in data
        ? String((data as { detail: unknown }).detail)
        : text || `Error HTTP ${res.status}`
    throw new AdminUsersApiError(res.status, detail)
  }
  return data as T
}

export async function fetchAdminUsers(params?: {
  q?: string
  active?: boolean | null
}): Promise<{ items: AdminUserRow[]; total: number }> {
  const qs = new URLSearchParams()
  if (params?.q?.trim()) qs.set("q", params.q.trim())
  if (params?.active === true || params?.active === false) qs.set("active", String(params.active))
  const res = await fetch(`${API_URL}/admin/users?${qs}`, { headers: getAuthHeaders() })
  return parseJson(res)
}

export async function fetchAdminUser(id: number): Promise<AdminUserRow> {
  const res = await fetch(`${API_URL}/admin/users/${id}`, { headers: getAuthHeaders() })
  return parseJson(res)
}

export async function fetchAdminRolesCatalog(): Promise<RolesCatalog> {
  const res = await fetch(`${API_URL}/admin/users/meta/roles`, { headers: getAuthHeaders() })
  return parseJson(res)
}

export async function fetchAdminCompaniesCatalog(): Promise<{ items: AdminCompanyRef[] }> {
  const res = await fetch(`${API_URL}/admin/users/meta/companies`, { headers: getAuthHeaders() })
  return parseJson(res)
}

export async function createAdminUser(body: {
  email: string
  nombre_usuario?: string | null
  role: string
  company_ids: number[]
  password?: string | null
  invite: boolean
}): Promise<AdminUserRow> {
  const res = await fetch(`${API_URL}/admin/users`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(body),
  })
  return parseJson(res)
}

export async function updateAdminUser(
  id: number,
  body: {
    email?: string
    nombre_usuario?: string | null
    role?: string
    active?: boolean
    company_ids?: number[]
  },
): Promise<AdminUserRow> {
  const res = await fetch(`${API_URL}/admin/users/${id}`, {
    method: "PATCH",
    headers: getAuthHeaders(),
    body: JSON.stringify(body),
  })
  return parseJson(res)
}

export async function resetAdminUserPassword(id: number): Promise<{
  user_id: number
  email: string
  delivery: AdminDeliveryPayload
}> {
  const res = await fetch(`${API_URL}/admin/users/${id}/reset-password`, {
    method: "POST",
    headers: getAuthHeaders(),
  })
  return parseJson(res)
}

export async function fetchAdminAuthEvents(params?: {
  limit?: number
  offset?: number
  user_id?: number
  event_type?: string
}): Promise<{
  items: AdminAuthEvent[]
  total: number
  meta: { available: boolean; message?: string }
}> {
  const qs = new URLSearchParams()
  if (params?.limit != null) qs.set("limit", String(params.limit))
  if (params?.offset != null) qs.set("offset", String(params.offset))
  if (params?.user_id != null) qs.set("user_id", String(params.user_id))
  if (params?.event_type) qs.set("event_type", params.event_type)
  const res = await fetch(`${API_URL}/admin/users/auth-events?${qs}`, {
    headers: getAuthHeaders(),
  })
  return parseJson(res)
}

export async function consumePasswordReset(token: string, newPassword: string): Promise<{ ok: boolean; email: string }> {
  const res = await fetch(`${API_URL}/auth/password-reset/consume`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ token, new_password: newPassword }),
  })
  return parseJson(res)
}
