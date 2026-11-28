import { getApiBaseUrl } from "@/lib/api-base"
import { getAuthHeaders } from "@/lib/api"

export interface SystemFeature {
  key: string
  title: string
  description: string
  enabled_override: boolean | null
  environment_default: boolean
  configured_enabled: boolean
  effective_enabled: boolean
  source: "erp" | "environment"
  requirements_ready: boolean
  blocked_reason: string | null
  row_version: number
  updated_by: number | null
  updated_at: string | null
}

export interface SystemFeatureEvent {
  id: number
  key: string
  actor_user_id: number
  before_override: boolean | null
  after_override: boolean | null
  before_configured: boolean
  after_configured: boolean
  row_version: number
  created_at: string
}

export interface SystemFeatureState {
  schema_ready: boolean
  items: SystemFeature[]
  history: SystemFeatureEvent[]
}

async function request(suffix = "", body?: { enabled_override: boolean | null; expected_version: number }, signal?: AbortSignal): Promise<SystemFeatureState> {
  const response = await fetch(getApiBaseUrl() + "/admin/system-features" + suffix, {
    method: body ? "PATCH" : "GET", headers: { ...getAuthHeaders(), "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
    cache: "no-store", signal,
  })
  const result = await response.json().catch(() => null)
  if (!response.ok) throw new Error(typeof result?.detail === "string" ? result.detail : "Error HTTP " + response.status)
  return result as SystemFeatureState
}

export function getSystemFeatures(signal?: AbortSignal) { return request("", undefined, signal) }
export function updateSystemFeature(item: SystemFeature, value: boolean | null) {
  return request("/" + encodeURIComponent(item.key), { enabled_override: value, expected_version: item.row_version })
}
