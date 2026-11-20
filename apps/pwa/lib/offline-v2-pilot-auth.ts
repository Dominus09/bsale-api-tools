/**
 * Consulta al servidor si el usuario/despacho están en el piloto Offline V2.
 * Fuente de verdad: API. No usa localStorage para permisos.
 */

import { authHeaders, getToken } from "./auth.ts"
import { getApiBase } from "./api-base.ts"
import {
  type OfflineV2PilotAuth,
  getCachedOfflineV2PilotAuth,
  setCachedOfflineV2PilotAuth,
} from "./offline-outbox-policy.ts"

type PilotStatusResponse = {
  writes_enabled?: boolean
  user_allowed?: boolean
  authorized_plan_ids?: number[]
  plan_id?: number | null
  plan_allowed?: boolean | null
  offline_v2_enabled?: boolean
  can_use_offline_v2?: boolean
}

function normalize(raw: PilotStatusResponse): OfflineV2PilotAuth {
  const plans = Array.isArray(raw.authorized_plan_ids)
    ? raw.authorized_plan_ids.map((n) => Number(n)).filter((n) => Number.isFinite(n) && n > 0)
    : []
  return {
    writes_enabled: Boolean(raw.writes_enabled),
    user_allowed: Boolean(raw.user_allowed),
    authorized_plan_ids: plans,
    plan_id: raw.plan_id == null ? null : Number(raw.plan_id),
    plan_allowed: raw.plan_allowed == null ? null : Boolean(raw.plan_allowed),
    offline_v2_enabled: Boolean(raw.offline_v2_enabled),
    can_use_offline_v2: Boolean(raw.can_use_offline_v2),
    fetched_at: Date.now(),
  }
}

export async function refreshOfflineV2PilotAuthorization(
  planId?: number | null,
): Promise<OfflineV2PilotAuth | null> {
  if (!getToken()) {
    setCachedOfflineV2PilotAuth(null)
    return null
  }
  const qs =
    planId != null && Number.isFinite(Number(planId)) && Number(planId) > 0
      ? `?plan_id=${Number(planId)}`
      : ""
  const path =
    planId != null && Number.isFinite(Number(planId)) && Number(planId) > 0
      ? `/distribuidora/pwa/driver/despachos/${Number(planId)}/offline-ops/pilot-status`
      : `/distribuidora/pwa/driver/offline-ops/pilot-status${qs}`
  try {
    const res = await fetch(`${getApiBase()}${path}`, {
      method: "GET",
      headers: { ...authHeaders(), Accept: "application/json" },
    })
    if (res.status === 401) {
      setCachedOfflineV2PilotAuth(null)
      return null
    }
    if (!res.ok) {
      // Conservar cache previa si el GET falla (p.ej. offline breve).
      return getCachedOfflineV2PilotAuth()
    }
    const json = (await res.json()) as PilotStatusResponse
    const auth = normalize(json)
    setCachedOfflineV2PilotAuth(auth)
    if (typeof window !== "undefined") {
      window.dispatchEvent(new Event("lq-offline-v2-pilot-changed"))
    }
    return auth
  } catch {
    return getCachedOfflineV2PilotAuth()
  }
}

export function clearOfflineV2PilotAuthorization(): void {
  setCachedOfflineV2PilotAuth(null)
}
