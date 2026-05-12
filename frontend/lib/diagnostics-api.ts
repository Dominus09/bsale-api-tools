/**
 * Cliente para el panel interno de diagnóstico (GET /diagnostics/*).
 * Requiere JWT staff con rol de administración en el backend.
 */

import { getApiBaseUrl } from "@/lib/api-base"
import { getAuthHeaders } from "@/lib/api"

const BASE = () => getApiBaseUrl().replace(/\/$/, "")

export type DiagnosticsHealth = {
  status: string
  backend: string
  database: string
  databaseError?: string | null
  environment: string
  uptime: number
  serverTime: string
  version: string
  recentRequestCount?: number
  recentErrorCount?: number
  avgResponseTimeMs?: number | null
  diagnosticsApiEnabled?: boolean
}

export type DiagnosticsRequestRow = {
  timestamp: string
  method: string
  path: string
  statusCode: number
  durationMs: number
  user?: string | null
  clientIp?: string | null
  origin?: string | null
  userAgent?: string | null
  error?: string | null
}

export type DiagnosticsLogRow = {
  timestamp: string
  level: string
  module: string
  message: string
  detail?: string | null
}

export type DiagnosticsErrorRow = {
  timestamp: string
  endpoint?: string | null
  statusCode?: number | null
  message: string
  detail?: string | null
}

export type RegisteredRoute = {
  method: string
  path: string
  name: string
  description: string
}

export type ObservedRoute = {
  method: string
  path: string
  description: string
  status: string
  lastCall?: string | null
  avgDurationMs: number
  recentErrors: number
  callCount: number
}

async function parseJsonOrThrow(res: Response, path: string): Promise<unknown> {
  const text = await res.text()
  if (!res.ok) {
    let detail = text.slice(0, 300)
    try {
      const j = JSON.parse(text) as { detail?: unknown }
      if (typeof j?.detail === "string") detail = j.detail
    } catch {
      /* ignore */
    }
    throw new Error(`${res.status} ${path}: ${detail}`)
  }
  try {
    return JSON.parse(text) as unknown
  } catch {
    throw new Error(`Respuesta no JSON en ${path}`)
  }
}

export async function fetchDiagnosticsHealth(): Promise<DiagnosticsHealth> {
  const path = "/diagnostics/health"
  const res = await fetch(`${BASE()}${path}`, { headers: getAuthHeaders(), cache: "no-store" })
  const data = await parseJsonOrThrow(res, path)
  return data as DiagnosticsHealth
}

export async function fetchDiagnosticsRequests(limit = 200): Promise<{ items: DiagnosticsRequestRow[] }> {
  const path = `/diagnostics/requests?limit=${limit}`
  const res = await fetch(`${BASE()}${path}`, { headers: getAuthHeaders(), cache: "no-store" })
  const data = await parseJsonOrThrow(res, path)
  return data as { items: DiagnosticsRequestRow[] }
}

export async function fetchDiagnosticsLogs(limit = 200): Promise<{ items: DiagnosticsLogRow[] }> {
  const path = `/diagnostics/logs?limit=${limit}`
  const res = await fetch(`${BASE()}${path}`, { headers: getAuthHeaders(), cache: "no-store" })
  const data = await parseJsonOrThrow(res, path)
  return data as { items: DiagnosticsLogRow[] }
}

export async function fetchDiagnosticsErrors(limit = 100): Promise<{ items: DiagnosticsErrorRow[] }> {
  const path = `/diagnostics/errors?limit=${limit}`
  const res = await fetch(`${BASE()}${path}`, { headers: getAuthHeaders(), cache: "no-store" })
  const data = await parseJsonOrThrow(res, path)
  return data as { items: DiagnosticsErrorRow[] }
}

export async function fetchDiagnosticsEndpoints(): Promise<{
  registered: RegisteredRoute[]
  observed: ObservedRoute[]
}> {
  const path = "/diagnostics/endpoints"
  const res = await fetch(`${BASE()}${path}`, { headers: getAuthHeaders(), cache: "no-store" })
  const data = await parseJsonOrThrow(res, path)
  return data as {
    registered: RegisteredRoute[]
    observed: ObservedRoute[]
  }
}
