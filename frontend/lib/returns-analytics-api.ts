import { getAuthHeaders } from "@/lib/api"
import { getApiBaseUrl } from "@/lib/api-base"

const API_URL = getApiBaseUrl()

export type ReturnsAnalyticsParams = {
  date_from?: string
  date_to?: string
  grain?: "day" | "week" | "month" | "year"
  signal?: AbortSignal
}

function qs(params: ReturnsAnalyticsParams): string {
  const sp = new URLSearchParams()
  if (params.date_from) sp.set("date_from", params.date_from)
  if (params.date_to) sp.set("date_to", params.date_to)
  if (params.grain) sp.set("grain", params.grain)
  const s = sp.toString()
  return s ? `?${s}` : ""
}

export type ReturnsSyncStatus = {
  company_id: number
  office_id: number
  bootstrap: {
    date_from: string
    date_to: string
    completed: boolean
    completed_at: string | null
    records_processed: number
    resumable: boolean
    resumable_sync_id: number | null
    pages_processed: number
  }
  cursor: {
    last_sync_at: string | null
    records_total: number
    last_return_ts: number | null
  }
  recent_runs: {
    id: number
    sync_type: string
    status: string
    date_from: string | null
    date_to: string | null
    pages_processed: number
    records_processed: number
    started_at: string | null
    finished_at: string | null
    duration_ms: number | null
    error_message: string | null
  }[]
}

export type ReturnsDashboardResponse = {
  scope: {
    company_id: number
    company_name: string
    office_id: number
    office_name: string
    module_version: string
  }
  period: { from: string; to: string }
  kpis: {
    total_nc: number
    total_amount: number
    pct_over_sales: number
    sales_net_period: number
    ticket_promedio_nc: number
    clients_affected: number
    products_affected: number
  }
  sync: ReturnsSyncStatus
}

export type ReturnsRankingsResponse = {
  motives: {
    motive: string
    quantity: number
    amount: number
    pct: number
    trend_delta: number
    trend_pct: number
  }[]
  sellers: {
    seller_id: number
    seller: string
    quantity: number
    amount: number
    pct_over_sales: number
    motives: string[]
  }[]
  clients: {
    client_id: number
    client: string
    quantity: number
    amount: number
    last_return: string | null
  }[]
  products: {
    variant_id: number
    product: string
    quantity: number
    amount: number
    return_count: number
  }[]
}

export type ReturnsDetailResponse = {
  return_id: number
  header: {
    number: number | string | null
    return_date: string | null
    motive: string | null
    amount: number
    client: string | null
    seller: string | null
    municipality: string | null
    reference_document: {
      id: number | null
      number: number | null
      type_id: number | null
      emission: string | null
    }
    credit_note: {
      id: number | null
      number: number | null
      emission: string | null
      url: string | null
    }
  }
  lines: {
    variant_id: number
    product: string | null
    quantity: number
    unit_value: number
    total_amount: number
    unit_cost: number
    margin_estimated: number
  }[]
  margin_estimated_total: number
}

export type ReturnsInsight = {
  type: string
  severity: string
  title: string
  description: string
  impact: number
}

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_URL}${path}`, {
    ...init,
    headers: { ...getAuthHeaders(), ...(init?.headers || {}) },
  })
  if (!res.ok) {
    const msg = await res.text().catch(() => "")
    throw new Error(msg || `Error ${path}`)
  }
  return res.json()
}

export function getReturnsDashboard(params: ReturnsAnalyticsParams = {}) {
  return fetchJson<ReturnsDashboardResponse>(`/returns-analytics/dashboard${qs(params)}`, {
    signal: params.signal,
  })
}

export function getReturnsRankings(params: ReturnsAnalyticsParams = {}) {
  return fetchJson<ReturnsRankingsResponse>(`/returns-analytics/rankings${qs(params)}`, {
    signal: params.signal,
  })
}

export type ReturnsListItem = {
  return_id: number
  credit_note_number: number | string | null
  return_date: string | null
  motive: string | null
  amount: number
  client: string | null
  seller: string | null
  municipality: string | null
}

export function getReturnsList(params: ReturnsAnalyticsParams & { limit?: number } = {}) {
  const sp = new URLSearchParams()
  if (params.date_from) sp.set("date_from", params.date_from)
  if (params.date_to) sp.set("date_to", params.date_to)
  if (params.limit != null) sp.set("limit", String(params.limit))
  const q = sp.toString()
  return fetchJson<ReturnsListItem[]>(`/returns-analytics/returns${q ? `?${q}` : ""}`, {
    signal: params.signal,
  })
}

export function getReturnsDetail(returnId: number) {
  return fetchJson<ReturnsDetailResponse>(`/returns-analytics/returns/${returnId}`)
}

export function getReturnsMap(params: ReturnsAnalyticsParams = {}) {
  return fetchJson<{ municipality: string; quantity: number; amount: number; top_motive: string }[]>(
    `/returns-analytics/map${qs(params)}`,
    { signal: params.signal },
  )
}

export function getReturnsTimeline(params: ReturnsAnalyticsParams = {}) {
  return fetchJson<{ bucket: string; quantity: number; amount: number }[]>(
    `/returns-analytics/timeline${qs(params)}`,
    { signal: params.signal },
  )
}

export function getReturnsInsights(params: ReturnsAnalyticsParams = {}) {
  return fetchJson<{ insights: ReturnsInsight[]; recommendations: string[] }>(
    `/returns-analytics/insights${qs(params)}`,
    { signal: params.signal },
  )
}

export function syncReturnsHistory(resume = false) {
  const q = resume ? "?resume=true" : ""
  return fetchJson<Record<string, unknown>>(`/returns-analytics/sync/history${q}`, {
    method: "POST",
  })
}

export function syncReturnsIncremental() {
  return fetchJson<Record<string, unknown>>("/returns-analytics/sync/incremental", {
    method: "POST",
  })
}

export function getReturnsSyncStatus() {
  return fetchJson<ReturnsSyncStatus>("/returns-analytics/sync/status")
}
