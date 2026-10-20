import { logout } from "@/lib/api"
import { getApiBaseUrl } from "@/lib/api-base"

import type {
  CatalogResponse,
  GondolaCompany,
  GondolaOptions,
  GondolaSession,
  GondolaSessionDetail,
  ReplenishmentItem,
  ReplenishmentStatus,
  ScanResponse,
} from "./types"

const API_URL = getApiBaseUrl()

export class GondolasError extends Error {
  status: number
  constructor(status: number, message: string) {
    super(message)
    this.name = "GondolasError"
    this.status = status
  }
}

function authHeaders(): HeadersInit {
  const token = typeof window !== "undefined" ? window.localStorage.getItem("token") : null
  return {
    "Content-Type": "application/json",
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
  }
}

function messageFor(status: number): string {
  if (status === 401) return "Sesión expirada. Vuelva a iniciar sesión."
  if (status === 403) return "No tiene permiso para Control de Góndolas en esta empresa."
  if (status === 404) return "Registro no encontrado en la empresa seleccionada."
  if (status === 409) return "Ya existe un recorrido en curso para esa sucursal y tipo de producto."
  if (status === 422) return "Datos inválidos."
  if (status === 503) return "Control de Góndolas aún no está activado en esta base. Falta aplicar la migración revisada."
  return "Error del servidor. Intente nuevamente."
}

type QueryValue = string | number | boolean | null | undefined

function buildQuery(params: Record<string, QueryValue>): string {
  const qs = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value === null || value === undefined || value === "") continue
    qs.set(key, String(value))
  }
  return qs.toString()
}

async function detailFrom(res: Response): Promise<string | null> {
  try {
    const body = (await res.json()) as { detail?: unknown }
    return typeof body.detail === "string" && body.detail.length <= 500 ? body.detail : null
  } catch {
    return null
  }
}

async function request<T>(
  method: "GET" | "POST" | "PATCH",
  path: string,
  params: Record<string, QueryValue>,
  body?: unknown,
  signal?: AbortSignal,
): Promise<T> {
  let res: Response
  try {
    res = await fetch(`${API_URL}${path}?${buildQuery(params)}`, {
      method,
      headers: authHeaders(),
      body: body === undefined ? undefined : JSON.stringify(body),
      signal,
    })
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") throw error
    throw new GondolasError(0, "No se pudo conectar con el servidor.")
  }
  if (res.status === 401 && typeof window !== "undefined") {
    logout()
    window.location.assign("/login")
  }
  if (!res.ok) {
    const detail = [403, 404, 409, 422, 503].includes(res.status) ? await detailFrom(res) : null
    throw new GondolasError(res.status, detail ?? messageFor(res.status))
  }
  return (await res.json()) as T
}

export function getGondolaCompanies(signal?: AbortSignal) {
  return request<{ companies: GondolaCompany[] }>("GET", "/gondolas/companies", {}, undefined, signal)
}

export function getGondolaOptions(companyId: number, signal?: AbortSignal) {
  return request<GondolaOptions>("GET", "/gondolas/options", { company_id: companyId }, undefined, signal)
}

export function getGondolaCatalog(
  params: {
    company_id: number
    office_id: number
    product_type_id: number
    q?: string
    stock_filter?: string
    exhibition_filter?: string
    session_id?: number | null
    sort?: string
    page?: number
    page_size?: number
  },
  signal?: AbortSignal,
) {
  return request<CatalogResponse>("GET", "/gondolas/catalog", { ...params }, undefined, signal)
}

export function getGondolaSessions(companyId: number, signal?: AbortSignal) {
  return request<{ items: GondolaSession[] }>("GET", "/gondolas/sessions", { company_id: companyId }, undefined, signal)
}

export function createGondolaSession(companyId: number, officeId: number, productTypeId: number) {
  return request<GondolaSessionDetail>("POST", "/gondolas/sessions", { company_id: companyId }, {
    office_id: officeId,
    product_type_id: productTypeId,
  })
}

export function getGondolaSession(companyId: number, sessionId: number, signal?: AbortSignal) {
  return request<GondolaSessionDetail>(
    "GET",
    `/gondolas/sessions/${sessionId}`,
    { company_id: companyId },
    undefined,
    signal,
  )
}

export function registerGondolaScan(
  companyId: number,
  sessionId: number,
  body: { bar_code: string; device?: string; variant_id?: number },
) {
  return request<ScanResponse>("POST", `/gondolas/sessions/${sessionId}/scans`, { company_id: companyId }, body)
}

export function closeGondolaSession(companyId: number, sessionId: number) {
  return request<GondolaSessionDetail>("POST", `/gondolas/sessions/${sessionId}/close`, { company_id: companyId }, {})
}

export function cancelGondolaSession(companyId: number, sessionId: number, reason?: string) {
  return request<GondolaSession>("POST", `/gondolas/sessions/${sessionId}/cancel`, { company_id: companyId }, { reason })
}

export function updateGondolaReplenishment(
  companyId: number,
  sessionId: number,
  variantId: number,
  status: ReplenishmentStatus,
  note?: string | null,
) {
  return request<ReplenishmentItem>(
    "PATCH",
    `/gondolas/sessions/${sessionId}/replenishment/${variantId}`,
    { company_id: companyId },
    { status, note },
  )
}
