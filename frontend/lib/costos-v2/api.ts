import { getApiBaseUrl } from "@/lib/api-base"

import type {
  CostV2DetailParams,
  CostV2ListParams,
  CostV2ProductDetailResponse,
  CostV2ProductListParams,
  CostV2ProductsResponse,
  CostV2ProductsSummaryResponse,
  CostV2ReceptionDetailResponse,
  CostV2ReceptionsResponse,
  CostV2SummaryParams,
  CostV2SummaryResponse,
} from "./types"
import { COST_V2_DEFAULT_LIMIT, COST_V2_MAX_LIMIT } from "./types"

const API_URL = getApiBaseUrl()

/** Misma auth ERP (Bearer localStorage); evita import circular con lib/api.ts. */
export function getCostV2AuthHeaders(): HeadersInit {
  const token =
    typeof window !== "undefined" ? window.localStorage.getItem("token") : null
  return {
    "Content-Type": "application/json",
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
  }
}

export class CostV2ApiError extends Error {
  status: number
  constructor(status: number, message: string) {
    super(message)
    this.name = "CostV2ApiError"
    this.status = status
  }
}

function userFacingMessage(status: number, body: string): string {
  if (status === 401) return "Sesión expirada. Vuelva a iniciar sesión."
  if (status === 403) return "No tiene permiso para ver Costos V2."
  if (status === 404) return "Recepción no encontrada en el alcance autorizado."
  if (status === 422) return "Filtros inválidos. Revise oficina y fechas."
  if (status >= 500) return "Error del servidor. Intente nuevamente."
  if (body && body.length < 200 && !/sql|traceback|psycopg|exception/i.test(body)) {
    return body
  }
  return "No se pudo completar la solicitud."
}

async function parseError(res: Response): Promise<CostV2ApiError> {
  const body = await res.text().catch(() => "")
  return new CostV2ApiError(res.status, userFacingMessage(res.status, body))
}

/** Construye querystring V2. No envía filtros vacíos ni cursor editable. */
export function buildCostV2Query(params: {
  company_id: number
  office_id: number
  date_from?: string
  date_to?: string
  status?: string | null
  warning?: string | null
  barcode?: string | null
  search?: string | null
  limit?: number
  cursor?: string | null
}): URLSearchParams {
  if (!params.company_id || params.company_id < 1) {
    throw new Error("company_id es obligatorio")
  }
  if (!params.office_id || params.office_id < 1) {
    throw new Error("office_id es obligatorio")
  }
  const qs = new URLSearchParams({
    company_id: String(params.company_id),
    office_id: String(params.office_id),
  })
  if (params.date_from) qs.set("date_from", params.date_from)
  if (params.date_to) qs.set("date_to", params.date_to)
  if (params.status?.trim()) qs.set("status", params.status.trim())
  if (params.warning?.trim()) qs.set("warning", params.warning.trim())
  if (params.barcode != null && String(params.barcode).trim() !== "") {
    qs.set("barcode", String(params.barcode).trim())
  }
  if (params.search?.trim()) qs.set("search", params.search.trim())
  if (params.limit != null) {
    const lim = Math.min(Math.max(1, Math.floor(params.limit)), COST_V2_MAX_LIMIT)
    qs.set("limit", String(lim))
  }
  if (params.cursor?.trim()) qs.set("cursor", params.cursor.trim())
  return qs
}

export function assertDateRange(dateFrom: string, dateTo: string): void {
  if (!dateFrom || !dateTo) throw new Error("Fechas obligatorias")
  if (dateFrom > dateTo) throw new Error("La fecha desde debe ser ≤ fecha hasta")
}

export async function getCostV2Receptions(
  params: CostV2ListParams,
): Promise<CostV2ReceptionsResponse> {
  assertDateRange(params.date_from, params.date_to)
  const qs = buildCostV2Query({
    company_id: params.company_id,
    office_id: params.office_id,
    date_from: params.date_from,
    date_to: params.date_to,
    status: params.status,
    warning: params.warning,
    barcode: params.barcode,
    search: params.search,
    limit: params.limit ?? COST_V2_DEFAULT_LIMIT,
    cursor: params.cursor,
  })
  // Keyset only — never OFFSET
  if (qs.has("offset")) qs.delete("offset")

  const res = await fetch(`${API_URL}/cost-analytics/v2/receptions?${qs}`, {
    headers: getCostV2AuthHeaders(),
    signal: params.signal,
  })
  if (!res.ok) throw await parseError(res)
  return res.json() as Promise<CostV2ReceptionsResponse>
}

export async function getCostV2ReceptionDetail(
  params: CostV2DetailParams,
): Promise<CostV2ReceptionDetailResponse> {
  const qs = buildCostV2Query({
    company_id: params.company_id,
    office_id: params.office_id,
  })
  const res = await fetch(
    `${API_URL}/cost-analytics/v2/receptions/${params.history_id}?${qs}`,
    {
      headers: getCostV2AuthHeaders(),
      signal: params.signal,
    },
  )
  if (!res.ok) throw await parseError(res)
  return res.json() as Promise<CostV2ReceptionDetailResponse>
}

export async function getCostV2Summary(
  params: CostV2SummaryParams,
): Promise<CostV2SummaryResponse> {
  assertDateRange(params.date_from, params.date_to)
  const qs = buildCostV2Query({
    company_id: params.company_id,
    office_id: params.office_id,
    date_from: params.date_from,
    date_to: params.date_to,
    status: params.status,
    warning: params.warning,
    barcode: params.barcode,
    search: params.search,
  })
  const res = await fetch(`${API_URL}/cost-analytics/v2/summary?${qs}`, {
    headers: getCostV2AuthHeaders(),
    signal: params.signal,
  })
  if (!res.ok) throw await parseError(res)
  return res.json() as Promise<CostV2SummaryResponse>
}

export async function getCostV2Products(
  params: CostV2ProductListParams,
): Promise<CostV2ProductsResponse> {
  assertDateRange(params.date_from, params.date_to)
  const qs = buildCostV2Query({
    company_id: params.company_id,
    office_id: params.office_id,
    date_from: params.date_from,
    date_to: params.date_to,
    status: params.status,
    warning: params.warning,
    barcode: params.barcode,
    search: params.search,
    limit: params.limit ?? COST_V2_DEFAULT_LIMIT,
    cursor: params.cursor,
  })
  if (params.sort) qs.set("sort", params.sort)
  if (params.only_with_changes) qs.set("only_with_changes", "true")
  if (params.only_needs_review) qs.set("only_needs_review", "true")
  if (
    params.min_abs_change_percent != null &&
    String(params.min_abs_change_percent).trim() !== ""
  ) {
    qs.set("min_abs_change_percent", String(params.min_abs_change_percent))
  }
  if (qs.has("offset")) qs.delete("offset")

  const res = await fetch(`${API_URL}/cost-analytics/v2/products?${qs}`, {
    headers: getCostV2AuthHeaders(),
    signal: params.signal,
  })
  if (!res.ok) throw await parseError(res)
  return res.json() as Promise<CostV2ProductsResponse>
}

export async function getCostV2ProductDetail(params: {
  company_id: number
  office_id: number
  variant_id: number
  date_from: string
  date_to: string
  history_limit?: number
  signal?: AbortSignal
}): Promise<CostV2ProductDetailResponse> {
  assertDateRange(params.date_from, params.date_to)
  const qs = buildCostV2Query({
    company_id: params.company_id,
    office_id: params.office_id,
    date_from: params.date_from,
    date_to: params.date_to,
  })
  if (params.history_limit != null) {
    qs.set("history_limit", String(params.history_limit))
  }
  const res = await fetch(
    `${API_URL}/cost-analytics/v2/products/${params.variant_id}?${qs}`,
    {
      headers: getCostV2AuthHeaders(),
      signal: params.signal,
    },
  )
  if (!res.ok) throw await parseError(res)
  return res.json() as Promise<CostV2ProductDetailResponse>
}

export async function getCostV2ProductsSummary(params: {
  company_id: number
  office_id: number
  date_from: string
  date_to: string
  change_threshold_percent?: number | string | null
  status?: string | null
  warning?: string | null
  barcode?: string | null
  search?: string | null
  signal?: AbortSignal
}): Promise<CostV2ProductsSummaryResponse> {
  assertDateRange(params.date_from, params.date_to)
  const qs = buildCostV2Query({
    company_id: params.company_id,
    office_id: params.office_id,
    date_from: params.date_from,
    date_to: params.date_to,
    status: params.status,
    warning: params.warning,
    barcode: params.barcode,
    search: params.search,
  })
  if (params.change_threshold_percent != null) {
    qs.set("change_threshold_percent", String(params.change_threshold_percent))
  }
  const res = await fetch(`${API_URL}/cost-analytics/v2/products-summary?${qs}`, {
    headers: getCostV2AuthHeaders(),
    signal: params.signal,
  })
  if (!res.ok) throw await parseError(res)
  return res.json() as Promise<CostV2ProductsSummaryResponse>
}

export function mergeReceptionItemsByHistoryId<T extends { history_id: number }>(
  existing: T[],
  incoming: T[],
): T[] {
  const seen = new Set(existing.map((r) => r.history_id))
  const out = [...existing]
  for (const row of incoming) {
    if (seen.has(row.history_id)) continue
    seen.add(row.history_id)
    out.push(row)
  }
  return out
}

export function mergeProductsByVariantId<T extends { variant_id: number }>(
  existing: T[],
  incoming: T[],
): T[] {
  const seen = new Set(existing.map((r) => r.variant_id))
  const out = [...existing]
  for (const row of incoming) {
    if (seen.has(row.variant_id)) continue
    seen.add(row.variant_id)
    out.push(row)
  }
  return out
}

export function defaultCostV2DateRange(days = 30): { from: string; to: string } {
  const to = new Date()
  const from = new Date()
  from.setDate(from.getDate() - days)
  const iso = (d: Date) => d.toISOString().slice(0, 10)
  return { from: iso(from), to: iso(to) }
}
