/**
 * Dedupe, debug y medición de payload para planificación / dispatch-plans.
 */

import {
  getDispatchPlanDashboard,
  getDispatchPlanHeader,
  getDispatchPlansBySession,
  listDispatchPlansRecent,
  type DispatchPlanDashboard,
  type DispatchPlanSummary,
} from "@/lib/api"

const callCounts = new Map<string, number>()
const renderCounts = new Map<string, number>()

const sessionInflight = new Map<string, Promise<{ items: DispatchPlanSummary[] }>>()
let recentInflightRef: Promise<{ items: DispatchPlanSummary[] }> | null = null
const dashboardInflight = new Map<string, Promise<DispatchPlanDashboard>>()

let lastSessionFetchAt = 0
const SESSION_MIN_INTERVAL_MS = 1500

export function logFrontendPlanDebug(
  endpoint: string,
  extra?: Record<string, unknown>,
): void {
  const n = (callCounts.get(endpoint) ?? 0) + 1
  callCounts.set(endpoint, n)
  if (typeof window !== "undefined" && process.env.NODE_ENV !== "production") {
    console.info(`[FRONTEND_PLAN_DEBUG] endpoint=${endpoint} calls=${n}`, extra ?? "")
  }
}

export function trackPlanPageRender(page: string): void {
  const n = (renderCounts.get(page) ?? 0) + 1
  renderCounts.set(page, n)
  if (typeof window !== "undefined" && process.env.NODE_ENV !== "production") {
    console.info(`[FRONTEND_PLAN_DEBUG] render page=${page} count=${n}`)
  }
}

function logPayloadSize(endpoint: string, data: unknown): void {
  try {
    const bytes = new TextEncoder().encode(JSON.stringify(data)).length
    logFrontendPlanDebug(endpoint, { payload_bytes: bytes })
  } catch {
    logFrontendPlanDebug(endpoint, { payload_bytes: "unknown" })
  }
}

/** Historial: una sola petición concurrente global (listado liviano en backend). */
export async function fetchDispatchPlansRecentDeduped(params?: {
  limit?: number
}): Promise<{ items: DispatchPlanSummary[] }> {
  logFrontendPlanDebug("dispatch-plans-list", {
    limit: params?.limit ?? 50,
    trigger: "fetch",
  })
  if (recentInflightRef) return recentInflightRef
  recentInflightRef = listDispatchPlansRecent(params)
    .then((res) => {
      logPayloadSize("dispatch-plans-list", res)
      return res
    })
    .finally(() => {
      recentInflightRef = null
    })
  return recentInflightRef
}

/** Una petición in-flight por sessionId. */
export async function fetchDispatchPlansBySessionDeduped(
  planSessionId: string,
  options?: { force?: boolean },
): Promise<{ items: DispatchPlanSummary[] }> {
  const key = planSessionId.trim()
  if (!key) {
    logFrontendPlanDebug("by-session", { plan_session_id: null, skipped: "empty" })
    return { items: [] }
  }

  const inflight = sessionInflight.get(key)
  if (inflight && !options?.force) {
    logFrontendPlanDebug("by-session", { plan_session_id: key, deduped: "inflight" })
    return inflight
  }

  const now = Date.now()
  if (!options?.force && now - lastSessionFetchAt < SESSION_MIN_INTERVAL_MS && inflight) {
    return inflight
  }

  lastSessionFetchAt = now
  logFrontendPlanDebug("by-session", { plan_session_id: key, trigger: "fetch" })

  const promise = getDispatchPlansBySession(key)
    .then((res) => {
      logPayloadSize("by-session", res)
      return res
    })
    .finally(() => {
      sessionInflight.delete(key)
    })
  sessionInflight.set(key, promise)
  return promise
}

/** Cabecera liviana (rápida). */
export async function fetchDispatchPlanHeaderDeduped(
  planId: number,
  signal?: AbortSignal,
): Promise<{ plan: DispatchPlanSummary }> {
  logFrontendPlanDebug("dispatch-plans-header", { planning_id: planId, trigger: "fetch" })
  const res = await getDispatchPlanHeader(planId, signal)
  logPayloadSize("dispatch-plans-header", res)
  return res
}

/** Dashboard pesado: on-demand, dedupe por planId. */
export async function fetchDispatchPlanDashboardDeduped(
  planId: number,
  signal?: AbortSignal,
  opts?: { include_margin?: boolean; force?: boolean },
): Promise<DispatchPlanDashboard> {
  logFrontendPlanDebug("dispatch-plans-dashboard", {
    planning_id: planId,
    trigger: "fetch",
    include_margin: opts?.include_margin ?? false,
  })
  const cacheKey = `${planId}:${opts?.include_margin ? "m" : "s"}`
  const existing = !opts?.force ? dashboardInflight.get(cacheKey) : undefined
  if (existing) return existing

  const promise = getDispatchPlanDashboard(planId, signal, {
    include_margin: opts?.include_margin,
  })
    .then((res) => {
      logPayloadSize("dispatch-plans-dashboard", res)
      return res
    })
    .finally(() => {
      dashboardInflight.delete(cacheKey)
    })
  dashboardInflight.set(cacheKey, promise)
  return promise
}

export function invalidateSessionPlansCache(planSessionId?: string): void {
  if (planSessionId?.trim()) sessionInflight.delete(planSessionId.trim())
  else sessionInflight.clear()
  lastSessionFetchAt = 0
}

export function invalidateDashboardCache(planId?: number): void {
  if (planId != null) {
    for (const k of [...dashboardInflight.keys()]) {
      if (k.startsWith(`${planId}:`)) dashboardInflight.delete(k)
    }
  } else dashboardInflight.clear()
}
