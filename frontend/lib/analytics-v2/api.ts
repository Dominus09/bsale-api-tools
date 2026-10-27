import { logout } from "@/lib/api"
import { getApiBaseUrl } from "@/lib/api-base"

import type { CatalogMeta, CatalogParams, CatalogRowsResponse } from "./catalog-types"
import type { DashboardResponse } from "./dashboard-types"
import type {
  MarginRow,
  MarginRowsParams,
  MarginRowsResponse,
  MarginsFilters,
  MarginsSummary,
  PolicyReviewResponse,
} from "./margins-types"
import type { PackDetailResponse, PackRowsParams, PackRowsResponse, PacksSummary } from "./packs-types"

import type {
  CampaignApproval,
  CampaignDetail,
  CampaignItem,
  CampaignKind,
  CampaignSummaryRow,
  CampaignTimeline,
  ClearanceReason,
  HistoryEvent,
  ItemWriteResponse,
  LegacyResponse,
  Paged,
  ProductSearchItem,
  PromotionSection,
  PromotionsFilters,
  PromotionsSummary,
} from "./promotions-types"

import type {
  AlertsResponse,
  AnomalyCode,
  CostFilters,
  CostProductResponse,
  CostProductsResponse,
  CostReviewCloseBody,
  CostReviewCreateBody,
  CostReviewCreateResponse,
  CostReviewDetailResponse,
  CostReviewItem,
  CostReviewsListResponse,
  CostSummary,
  FreshnessResponse,
  HistoryResponse,
  ProductListParams,
  ReceptionAnalysisResponse,
  ReceptionDetail,
  Severity,
} from "./types"
import { isStockFilter } from "./stock-filter"

const API_URL = getApiBaseUrl()

export class AnalyticsV2Error extends Error {
  status: number
  constructor(status: number, message: string) {
    super(message)
    this.name = "AnalyticsV2Error"
    this.status = status
  }
}

function authHeaders(opts?: { jsonBody?: boolean }): HeadersInit {
  const token = typeof window !== "undefined" ? window.localStorage.getItem("token") : null
  // No enviar Content-Type en GET/blob: algunos proxies/WAF responden 422
  // al ver application/json sin cuerpo.
  return {
    ...(opts?.jsonBody ? { "Content-Type": "application/json" } : {}),
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
  }
}

function messageFor(status: number): string {
  if (status === 401) return "Sesión expirada. Vuelva a iniciar sesión."
  if (status === 403) return "No tiene permiso para este módulo de Analítica V2 en esta empresa."
  if (status === 404) return "Registro no encontrado en la empresa seleccionada."
  if (status === 409)
    return "Esta empresa aún no tiene BSALE RAW completo. Analítica V2 está disponible solo para empresas con RAW validado."
  if (status === 422) return "Filtros inválidos."
  if (status === 503) return "La consulta demoró demasiado. Intente nuevamente."
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

async function getJson<T>(path: string, params: Record<string, QueryValue>, signal?: AbortSignal): Promise<T> {
  let res: Response
  try {
    res = await fetch(`${API_URL}${path}?${buildQuery(params)}`, { headers: authHeaders(), signal })
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") throw error
    throw new AnalyticsV2Error(0, "No se pudo conectar con el servidor.")
  }
  if (res.status === 401 && typeof window !== "undefined") {
    logout()
    window.location.assign("/login")
  }
  if (!res.ok) {
    // Exponer el detail del backend en 422/503 para no ocultar la causa real
    // detrás del mensaje genérico «Filtros inválidos».
    const detail = [422, 503].includes(res.status) ? await detailFrom(res) : null
    throw new AnalyticsV2Error(res.status, detail ?? messageFor(res.status))
  }
  return (await res.json()) as T
}

export function getCostSummary(
  companyId: number,
  signalOrStock?: AbortSignal | { stock_filter?: string | null },
  maybeSignal?: AbortSignal,
) {
  const opts =
    signalOrStock && typeof signalOrStock === "object" && !("aborted" in signalOrStock)
      ? (signalOrStock as { stock_filter?: string | null })
      : {}
  const signal =
    signalOrStock && typeof signalOrStock === "object" && "aborted" in signalOrStock
      ? (signalOrStock as AbortSignal)
      : maybeSignal
  const stock = isStockFilter(opts.stock_filter) ? opts.stock_filter : "with"
  return getJson<CostSummary>(
    "/analytics-v2/costs/summary",
    { company_id: companyId, stock_filter: stock },
    signal,
  )
}

export function getCostFilters(companyId: number, signal?: AbortSignal) {
  return getJson<CostFilters>("/analytics-v2/costs/filters", { company_id: companyId }, signal)
}

export function getRawFreshness(companyId: number, signal?: AbortSignal) {
  return getJson<FreshnessResponse>("/analytics-v2/freshness", { company_id: companyId }, signal)
}

export function getCostProducts(params: ProductListParams, signal?: AbortSignal) {
  const stock_filter = isStockFilter(params.stock_filter) ? params.stock_filter : "with"
  return getJson<CostProductsResponse>(
    "/analytics-v2/costs/products",
    { ...params, stock_filter },
    signal,
  )
}

export function getReceptionAnalysis(
  params: {
    company_id: number
    date_from: string
    date_to: string
    product_type_id?: number | null
    office_id?: number | null
    direction?: "all" | "up" | "down"
    min_abs_change_pct?: number | null
    q?: string | null
    stock_filter?: string | null
    page?: number
    page_size?: number
  },
  signal?: AbortSignal,
) {
  const stock_filter = isStockFilter(params.stock_filter) ? params.stock_filter : "with"
  return getJson<ReceptionAnalysisResponse>(
    "/analytics-v2/costs/receptions/analysis",
    { ...params, stock_filter },
    signal,
  )
}

export function getCostProduct(companyId: number, variantId: number, signal?: AbortSignal) {
  return getJson<CostProductResponse>(
    `/analytics-v2/costs/products/${variantId}`,
    { company_id: companyId },
    signal,
  )
}

export function getCostProductHistory(
  params: {
    company_id: number
    variant_id: number
    office_id?: number | null
    date_from?: string | null
    date_to?: string | null
    page?: number
    page_size?: number
  },
  signal?: AbortSignal,
) {
  const { variant_id, ...rest } = params
  return getJson<HistoryResponse>(`/analytics-v2/costs/products/${variant_id}/history`, rest, signal)
}

export function getCostAlerts(
  params: {
    company_id: number
    date_from?: string | null
    date_to?: string | null
    anomaly_code?: AnomalyCode | null
    severity?: Severity | null
    office_id?: number | null
    stock_filter?: "with" | "without" | "all" | null
    page?: number
    page_size?: number
  },
  signal?: AbortSignal,
) {
  return getJson<AlertsResponse>("/analytics-v2/costs/alerts", params, signal)
}

export function getCostReviews(
  params: {
    company_id: number
    status?: string | null
    q?: string | null
    assignee_user_id?: number | null
    priority?: string | null
    sort?: string | null
    page?: number
    page_size?: number
  },
  signal?: AbortSignal,
) {
  return getJson<CostReviewsListResponse>("/analytics-v2/costs/reviews", params, signal)
}

export function getCostReview(companyId: number, reviewId: number, signal?: AbortSignal) {
  return getJson<CostReviewDetailResponse>(
    `/analytics-v2/costs/reviews/${reviewId}`,
    { company_id: companyId },
    signal,
  )
}

export function createCostReview(companyId: number, body: CostReviewCreateBody) {
  return sendJson<CostReviewCreateResponse>("POST", "/analytics-v2/costs/reviews", { company_id: companyId }, body)
}

export function startCostReview(companyId: number, reviewId: number) {
  return sendJson<CostReviewItem>("POST", `/analytics-v2/costs/reviews/${reviewId}/start`, { company_id: companyId }, {})
}

export function updateCostReviewObservations(companyId: number, reviewId: number, observations: string) {
  return sendJson<CostReviewItem>(
    "PATCH",
    `/analytics-v2/costs/reviews/${reviewId}/observations`,
    { company_id: companyId },
    { observations },
  )
}

export function updateCostReviewPriority(companyId: number, reviewId: number, priority: string) {
  return sendJson<CostReviewItem>(
    "PATCH",
    `/analytics-v2/costs/reviews/${reviewId}/priority`,
    { company_id: companyId },
    { priority },
  )
}

export function resolveCostReview(companyId: number, reviewId: number, body: CostReviewCloseBody) {
  return sendJson<CostReviewItem>(
    "POST",
    `/analytics-v2/costs/reviews/${reviewId}/resolve`,
    { company_id: companyId },
    body,
  )
}

export function discardCostReview(companyId: number, reviewId: number, body: CostReviewCloseBody) {
  return sendJson<CostReviewItem>(
    "POST",
    `/analytics-v2/costs/reviews/${reviewId}/discard`,
    { company_id: companyId },
    body,
  )
}

export function getMarginsSummary(companyId: number, costBasis: string, signal?: AbortSignal) {
  return getJson<MarginsSummary>(
    "/analytics-v2/margins/summary",
    { company_id: companyId, cost_basis: costBasis },
    signal,
  )
}

export function getMarginsFilters(companyId: number, signal?: AbortSignal) {
  return getJson<MarginsFilters>("/analytics-v2/margins/filters", { company_id: companyId }, signal)
}

export function getMarginRows(params: MarginRowsParams, signal?: AbortSignal) {
  return getJson<MarginRowsResponse>("/analytics-v2/margins/rows", { ...params }, signal)
}

export function getPolicyReview(companyId: number, onlyIssues: boolean, signal?: AbortSignal) {
  return getJson<PolicyReviewResponse>(
    "/analytics-v2/margins/policy-review",
    { company_id: companyId, only_issues: onlyIssues },
    signal,
  )
}

async function detailFrom(res: Response): Promise<string | null> {
  try {
    const body = (await res.json()) as { detail?: unknown }
    const detail = body.detail
    if (typeof detail === "string" && detail.length > 0 && detail.length <= 500) return detail
    // FastAPI RequestValidationError: detail es lista de {loc, msg, type}
    if (Array.isArray(detail) && detail.length > 0) {
      const parts = detail.slice(0, 3).map((item) => {
        if (!item || typeof item !== "object") return String(item)
        const row = item as { loc?: unknown[]; msg?: string }
        const loc = Array.isArray(row.loc)
          ? row.loc.filter((x) => x !== "query" && x !== "body").join(".")
          : ""
        const msg = typeof row.msg === "string" ? row.msg : "inválido"
        return loc ? `${loc}: ${msg}` : msg
      })
      const text = parts.join("; ")
      return text.length <= 500 ? text : text.slice(0, 497) + "…"
    }
    return null
  } catch {
    return null
  }
}

async function sendJson<T>(
  method: "POST" | "PATCH",
  path: string,
  params: Record<string, QueryValue>,
  body: unknown,
): Promise<T> {
  let res: Response
  try {
    res = await fetch(`${API_URL}${path}?${buildQuery(params)}`, {
      method,
      headers: authHeaders({ jsonBody: true }),
      body: JSON.stringify(body ?? {}),
    })
  } catch {
    throw new AnalyticsV2Error(0, "No se pudo conectar con el servidor.")
  }
  if (res.status === 401 && typeof window !== "undefined") {
    logout()
    window.location.assign("/login")
  }
  if (!res.ok) {
    const detail = [403, 404, 409, 422, 503].includes(res.status) ? await detailFrom(res) : null
    throw new AnalyticsV2Error(res.status, detail ?? (res.status === 422 ? "Datos inválidos." : messageFor(res.status)))
  }
  return (await res.json()) as T
}

async function getPromotionsJson<T>(path: string, params: Record<string, QueryValue>, signal?: AbortSignal): Promise<T> {
  try {
    return await getJson<T>(path, params, signal)
  } catch (error) {
    if (error instanceof AnalyticsV2Error && error.status === 422) {
      throw new AnalyticsV2Error(422, "Filtros inválidos o empresa sin alcance comercial definido.")
    }
    throw error
  }
}

export function getPromotionsSummary(companyId: number, month: string, signal?: AbortSignal) {
  return getPromotionsJson<PromotionsSummary>("/analytics-v2/promotions/summary", { company_id: companyId, month }, signal)
}

export function getPromotionsFilters(companyId: number, signal?: AbortSignal) {
  return getPromotionsJson<PromotionsFilters>("/analytics-v2/promotions/filters", { company_id: companyId }, signal)
}

export type PromotionListParams = {
  company_id: number
  month?: string
  kind?: CampaignKind | ""
  approval?: CampaignApproval | ""
  timeline?: CampaignTimeline | ""
  price_list_id?: number | ""
  q?: string
  page?: number
  page_size?: number
}

export function getPromotionCampaigns(params: PromotionListParams, signal?: AbortSignal) {
  return getPromotionsJson<Paged<CampaignSummaryRow>>("/analytics-v2/promotions/campaigns", { ...params }, signal)
}

export function getPromotionItems(
  params: Omit<PromotionListParams, "timeline"> & { section: PromotionSection },
  signal?: AbortSignal,
) {
  return getPromotionsJson<Paged<CampaignItem>>("/analytics-v2/promotions/items", { ...params }, signal)
}

export function getPromotionCampaign(companyId: number, campaignId: number, signal?: AbortSignal) {
  return getPromotionsJson<CampaignDetail>(
    `/analytics-v2/promotions/campaigns/${campaignId}`,
    { company_id: companyId },
    signal,
  )
}

export function getPromotionsHistory(
  params: { company_id: number; month: string; page?: number; page_size?: number },
  signal?: AbortSignal,
) {
  return getPromotionsJson<Paged<HistoryEvent>>("/analytics-v2/promotions/history", params, signal)
}

export function getLegacyPromotions(companyId: number, signal?: AbortSignal) {
  return getPromotionsJson<LegacyResponse>("/analytics-v2/promotions/legacy", { company_id: companyId }, signal)
}

export function searchPromotionProducts(companyId: number, q: string, signal?: AbortSignal) {
  return getPromotionsJson<{ items: ProductSearchItem[] }>(
    "/analytics-v2/promotions/products/search",
    { company_id: companyId, q },
    signal,
  )
}

export function createCampaign(
  companyId: number,
  body: { kind: CampaignKind; name: string; starts_on: string; ends_on: string; responsible?: string | null; notes?: string | null },
) {
  return sendJson<CampaignDetail>("POST", "/analytics-v2/promotions/campaigns", { company_id: companyId }, body)
}

export function updateCampaign(
  companyId: number,
  campaignId: number,
  body: { version: number; name?: string; starts_on?: string; ends_on?: string; responsible?: string | null; notes?: string | null },
) {
  return sendJson<CampaignDetail>("PATCH", `/analytics-v2/promotions/campaigns/${campaignId}`, { company_id: companyId }, body)
}

export function addCampaignItem(
  companyId: number,
  campaignId: number,
  body: {
    version: number
    variant_id: number
    price_list_id: number
    promo_gross_price: string
    clearance_reason?: ClearanceReason | null
    notes?: string | null
  },
) {
  return sendJson<ItemWriteResponse>(
    "POST",
    `/analytics-v2/promotions/campaigns/${campaignId}/items`,
    { company_id: companyId },
    body,
  )
}

export function updateCampaignItem(
  companyId: number,
  campaignId: number,
  itemId: number,
  body: { version: number; promo_gross_price?: string; clearance_reason?: ClearanceReason | null; notes?: string | null; reason?: string | null },
) {
  return sendJson<ItemWriteResponse>(
    "PATCH",
    `/analytics-v2/promotions/campaigns/${campaignId}/items/${itemId}`,
    { company_id: companyId },
    body,
  )
}

export function removeCampaignItem(companyId: number, campaignId: number, itemId: number, body: { version: number; reason: string }) {
  return sendJson<ItemWriteResponse>(
    "POST",
    `/analytics-v2/promotions/campaigns/${campaignId}/items/${itemId}/remove`,
    { company_id: companyId },
    body,
  )
}

export function transitionCampaign(
  companyId: number,
  campaignId: number,
  action: "submit" | "approve" | "reject" | "cancel",
  body: { version: number; reason?: string | null },
) {
  return sendJson<CampaignDetail>(
    "POST",
    `/analytics-v2/promotions/campaigns/${campaignId}/${action}`,
    { company_id: companyId },
    body,
  )
}

export function importLegacyPromotion(companyId: number, promotionId: number) {
  return sendJson<{ campaign: CampaignDetail; imported: number; skipped: unknown[] }>(
    "POST",
    `/analytics-v2/promotions/legacy/${promotionId}/import`,
    { company_id: companyId },
    {},
  )
}

export function getDashboard(companyId: number, costBasis: string, refresh: boolean, signal?: AbortSignal) {
  return getJson<DashboardResponse>(
    "/analytics-v2/dashboard",
    { company_id: companyId, cost_basis: costBasis, refresh: refresh || undefined },
    signal,
  )
}

export function getMarginProduct(companyId: number, variantId: number, costBasis: string, signal?: AbortSignal) {
  return getJson<{ variant_id: number; rows: MarginRow[]; cost_basis: string }>(
    `/analytics-v2/margins/products/${variantId}`,
    { company_id: companyId, cost_basis: costBasis },
    signal,
  )
}

export function getPacksSummary(companyId: number, costBasis: string, signal?: AbortSignal) {
  return getJson<PacksSummary>("/analytics-v2/packs/summary", { company_id: companyId, cost_basis: costBasis }, signal)
}

export function getPackRows(params: PackRowsParams, signal?: AbortSignal) {
  return getJson<PackRowsResponse>("/analytics-v2/packs/rows", { ...params }, signal)
}

export function getPackDetail(companyId: number, variantId: number, costBasis: string, signal?: AbortSignal) {
  return getJson<PackDetailResponse>(
    `/analytics-v2/packs/${variantId}`,
    { company_id: companyId, cost_basis: costBasis },
    signal,
  )
}

export function getReception(companyId: number, receptionId: number, signal?: AbortSignal) {
  return getJson<ReceptionDetail>(
    `/analytics-v2/costs/receptions/${receptionId}`,
    { company_id: companyId },
    signal,
  )
}

/** Descarga un .xlsx del backend con la sesión actual y lo guarda en el equipo. */
export async function downloadXlsx(path: string, params: Record<string, QueryValue>, filename: string): Promise<void> {
  let res: Response
  try {
    res = await fetch(`${API_URL}${path}?${buildQuery(params)}`, { headers: authHeaders() })
  } catch {
    throw new AnalyticsV2Error(0, "No se pudo conectar con el servidor.")
  }
  if (res.status === 401 && typeof window !== "undefined") {
    logout()
    window.location.assign("/login")
  }
  if (!res.ok) {
    const detail = [422, 503, 504].includes(res.status) ? await detailFrom(res) : null
    throw new AnalyticsV2Error(res.status, detail ?? messageFor(res.status))
  }
  const contentType = (res.headers.get("content-type") || "").toLowerCase()
  const blob = await res.blob()
  // Proxy puede devolver JSON de error con content-type incorrecto.
  if (contentType.includes("json") || blob.type.includes("json")) {
    const text = await blob.text()
    try {
      const parsed = JSON.parse(text) as { detail?: unknown }
      if (typeof parsed.detail === "string") {
        throw new AnalyticsV2Error(res.status || 500, parsed.detail)
      }
      if (Array.isArray(parsed.detail) && parsed.detail.length) {
        throw new AnalyticsV2Error(res.status || 422, "Filtros inválidos en la descarga.")
      }
    } catch (e) {
      if (e instanceof AnalyticsV2Error) throw e
    }
    throw new AnalyticsV2Error(res.status || 500, messageFor(res.status || 500))
  }
  const url = URL.createObjectURL(blob)
  const a = document.createElement("a")
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  a.remove()
  URL.revokeObjectURL(url)
}

export function getCatalogMeta(companyId: number, signal?: AbortSignal) {
  return getJson<CatalogMeta>("/analytics-v2/catalog/meta", { company_id: companyId }, signal)
}

export function getCatalogRows(params: CatalogParams, signal?: AbortSignal) {
  return getJson<CatalogRowsResponse>("/analytics-v2/catalog/rows", { ...params }, signal)
}
