import { getApiBaseUrl } from "@/lib/api-base"
import { getAuthHeaders, logout } from "@/lib/api"

const API_URL = getApiBaseUrl()

export type AdminCompanyRef = {
  company_id: number
  name: string | null
  active?: boolean
}

export type AdminOfficeRef = {
  company_id: number
  office_id: number
  name: string | null
}

export type AdminAppRef = { code: string; label: string }
export type AdminProfileRef = { code: string; label: string }

export type AdminUserRow = {
  id: number
  email: string
  role: string
  active: boolean
  created_at: string | null
  nombre_usuario: string | null
  companies: AdminCompanyRef[]
  last_login_at: string | null
  /** Marcado por backend si el id está en AUTH_OWNER_USER_IDS. */
  is_owner?: boolean
  delivery?: AdminDeliveryPayload
  apps?: string[]
  profiles?: string[]
  offices?: AdminOfficeRef[]
  multiapp_ready?: boolean
  apps_legacy_erp?: boolean
  has_erp_access?: boolean
  has_pwa_access?: boolean
}

export type AdminDeliveryPayload = {
  token_id: number
  purpose: string
  expires_at: string
  one_time_token: string
  /** Ruta relativa con token; el cliente antepone origin. */
  one_time_path?: string
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
  app?: string | null
  profile?: string | null
}): Promise<{ items: AdminUserRow[]; total: number }> {
  const qs = new URLSearchParams()
  if (params?.q?.trim()) qs.set("q", params.q.trim())
  if (params?.active === true || params?.active === false) qs.set("active", String(params.active))
  if (params?.app?.trim()) qs.set("app", params.app.trim())
  if (params?.profile?.trim()) qs.set("profile", params.profile.trim())
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

export async function fetchAdminAppsCatalog(): Promise<{
  items: AdminAppRef[]
  ready: boolean
  tables: Record<string, boolean>
}> {
  const res = await fetch(`${API_URL}/admin/users/meta/apps`, { headers: getAuthHeaders() })
  return parseJson(res)
}

export async function fetchAdminProfilesCatalog(): Promise<{
  items: AdminProfileRef[]
  ready: boolean
  tables: Record<string, boolean>
}> {
  const res = await fetch(`${API_URL}/admin/users/meta/profiles`, { headers: getAuthHeaders() })
  return parseJson(res)
}

export async function fetchAdminOfficesCatalog(params: {
  companyIds: number[]
}): Promise<{ items: AdminOfficeRef[] }> {
  const qs = new URLSearchParams()
  if (params.companyIds.length === 1) {
    qs.set("company_id", String(params.companyIds[0]))
  } else if (params.companyIds.length > 1) {
    qs.set("company_ids", params.companyIds.join(","))
  } else {
    return { items: [] }
  }
  const res = await fetch(`${API_URL}/admin/users/meta/offices?${qs}`, {
    headers: getAuthHeaders(),
  })
  return parseJson(res)
}

export async function createAdminUser(body: {
  email: string
  nombre_usuario?: string | null
  role: string
  company_ids: number[]
  password?: string | null
  invite: boolean
  apps?: string[]
  profiles?: string[]
  office_scopes?: Array<{ company_id: number; office_id: number }>
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
    apps?: string[]
    profiles?: string[]
    office_scopes?: Array<{ company_id: number; office_id: number }>
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

/** Consumo público: no forzar logout ante 401 (el titular aún no tiene sesión). */
export async function consumePasswordReset(
  token: string,
  newPassword: string,
): Promise<{ ok: boolean; email: string }> {
  let res: Response
  try {
    res = await fetch(`${API_URL}/auth/password-reset/consume`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ token, new_password: newPassword }),
    })
  } catch {
    throw new AdminUsersApiError(0, "No se pudo conectar con el servidor. Intente nuevamente.")
  }
  const text = await res.text()
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
  return data as { ok: boolean; email: string }
}
