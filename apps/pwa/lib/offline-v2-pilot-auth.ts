/**
 * Autorización Offline V2 confirmada por API.
 * - Online: consulta pilot-status (timeout corto) y persiste grant en IndexedDB.
 * - Offline: usa grant previo + plan descargado; NO inventa permisos.
 * - localStorage NUNCA otorga permisos de servidor.
 */

import { authHeaders, getSessionUserId, getToken } from "./auth.ts"
import { getApiBase } from "./api-base.ts"
import {
  type OfflineV2PilotAuth,
  getCachedOfflineV2PilotAuth,
  setCachedOfflineV2PilotAuth,
} from "./offline-outbox-policy.ts"
import {
  clearOfflineV2PlanGrant,
  clearOfflineV2PlanGrantsForUser,
  getOfflinePlanReady,
  getOfflineV2PlanGrant,
  isDeviceOffline,
  saveOfflineV2PlanGrant,
  type OfflineV2PlanGrant,
} from "./offline-store.ts"

type PilotStatusResponse = {
  writes_enabled?: boolean
  user_allowed?: boolean
  authorized_plan_ids?: number[]
  plan_id?: number | null
  plan_allowed?: boolean | null
  offline_v2_enabled?: boolean
  can_use_offline_v2?: boolean
  scope?: string
}

const PILOT_FETCH_TIMEOUT_MS = 2500

/** Grants hidratados en memoria (planId → grant). */
const memoryGrants = new Map<number, OfflineV2PlanGrant>()

function normalize(raw: PilotStatusResponse): OfflineV2PilotAuth {
  const plans = Array.isArray(raw.authorized_plan_ids)
    ? raw.authorized_plan_ids
        .map((n) => Number(n))
        .filter((n) => Number.isFinite(n) && n > 0)
    : []
  return {
    writes_enabled: Boolean(raw.writes_enabled),
    user_allowed: Boolean(raw.user_allowed),
    authorized_plan_ids: plans,
    plan_id: raw.plan_id == null ? null : Number(raw.plan_id),
    plan_allowed: raw.plan_allowed == null ? null : Boolean(raw.plan_allowed),
    offline_v2_enabled: Boolean(raw.offline_v2_enabled),
    can_use_offline_v2: Boolean(raw.can_use_offline_v2),
    scope: typeof raw.scope === "string" ? raw.scope : undefined,
    fetched_at: Date.now(),
  }
}

function emitChanged() {
  if (typeof window !== "undefined") {
    window.dispatchEvent(new Event("lq-offline-v2-pilot-changed"))
  }
}

function grantAllowsUi(grant: OfflineV2PlanGrant | null | undefined): boolean {
  return Boolean(grant?.writesEnabled && grant.planComplete)
}

async function persistGrantFromServer(
  userId: string,
  planId: number,
  auth: OfflineV2PilotAuth,
): Promise<void> {
  const enabled =
    auth.offline_v2_enabled === true &&
    (auth.plan_allowed == null || auth.plan_allowed === true) &&
    (auth.can_use_offline_v2 === true || auth.plan_id == null)

  if (!enabled) {
    memoryGrants.delete(planId)
    await clearOfflineV2PlanGrant(userId, planId)
    return
  }

  const ready = await getOfflinePlanReady(userId, planId)
  const planComplete = Boolean(
    ready && (ready.dataComplete === true || ready.complete === true),
  )
  if (!planComplete) {
    // ACK de writes sí, pero sin descarga íntegra no habilitamos captura offline.
    memoryGrants.delete(planId)
    await clearOfflineV2PlanGrant(userId, planId)
    return
  }

  const pickingId =
    ready?.pickingId != null && Number.isFinite(Number(ready.pickingId))
      ? Number(ready.pickingId)
      : null
  const pickingVersion =
    ready?.pickingVersion != null && String(ready.pickingVersion).trim()
      ? String(ready.pickingVersion)
      : null

  const grant: OfflineV2PlanGrant = {
    userId: String(userId),
    planId: Number(planId),
    writesEnabled: true,
    pickingId,
    pickingVersion,
    planComplete: true,
    confirmedAt: new Date().toISOString(),
  }
  memoryGrants.set(planId, grant)
  await saveOfflineV2PlanGrant(grant)
}

/** Hidrata grants IDB → memoria (arranque / ficha offline). */
export async function hydrateOfflineV2AuthFromIdb(
  planId?: number | null,
): Promise<OfflineV2PlanGrant | null> {
  const uid = getSessionUserId()
  if (!uid) return null
  if (planId != null && Number.isFinite(Number(planId)) && Number(planId) > 0) {
    const grant = await getOfflineV2PlanGrant(uid, Number(planId))
    if (grant && String(grant.userId) === String(uid) && grantAllowsUi(grant)) {
      // Validar picking vs ready actual (si cambió, no inventar permiso).
      const ready = await getOfflinePlanReady(uid, Number(planId))
      if (!ready || !(ready.dataComplete === true || ready.complete === true)) {
        return null
      }
      const readyVer =
        ready.pickingVersion != null ? String(ready.pickingVersion) : null
      if (
        grant.pickingVersion &&
        readyVer &&
        grant.pickingVersion !== readyVer
      ) {
        return null
      }
      memoryGrants.set(Number(planId), grant)
      return grant
    }
    return null
  }
  return null
}

/**
 * ¿UI Offline V2 para este plan?
 * Sync: memoria / session cache / lab hint.
 * Preferir `resolveOfflineV2UiEnabledForPlan` (async) en páginas.
 */
export function isOfflineV2UiEnabledForPlanSync(planId: number): boolean {
  const g = memoryGrants.get(Number(planId))
  if (g && grantAllowsUi(g)) return true
  const cached = getCachedOfflineV2PilotAuth()
  if (cached) {
    if (!cached.offline_v2_enabled) return false
    if (cached.plan_id === Number(planId) && cached.plan_allowed != null) {
      return cached.plan_allowed === true
    }
    return cached.offline_v2_enabled === true
  }
  return false
}

export async function resolveOfflineV2UiEnabledForPlan(
  planId: number,
): Promise<boolean> {
  if (isOfflineV2UiEnabledForPlanSync(planId)) return true
  const grant = await hydrateOfflineV2AuthFromIdb(planId)
  return grantAllowsUi(grant)
}

async function fetchPilotStatus(
  planId?: number | null,
): Promise<OfflineV2PilotAuth | null> {
  if (!getToken()) return null
  const path =
    planId != null && Number.isFinite(Number(planId)) && Number(planId) > 0
      ? `/distribuidora/pwa/driver/despachos/${Number(planId)}/offline-ops/pilot-status`
      : `/distribuidora/pwa/driver/offline-ops/pilot-status`
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), PILOT_FETCH_TIMEOUT_MS)
  try {
    const res = await fetch(`${getApiBase()}${path}`, {
      method: "GET",
      headers: { ...authHeaders(), Accept: "application/json" },
      signal: controller.signal,
    })
    if (res.status === 401) {
      setCachedOfflineV2PilotAuth(null)
      return null
    }
    if (!res.ok) return null
    return normalize((await res.json()) as PilotStatusResponse)
  } catch {
    return null
  } finally {
    clearTimeout(timer)
  }
}

/**
 * Online: refresca desde API y persiste grant.
 * Offline: solo hidrata grant IDB; no bloquea ni inventa.
 */
export async function refreshOfflineV2PilotAuthorization(
  planId?: number | null,
): Promise<OfflineV2PilotAuth | null> {
  if (!getToken()) {
    setCachedOfflineV2PilotAuth(null)
    return null
  }

  const uid = getSessionUserId()
  if (planId != null && Number.isFinite(Number(planId))) {
    await hydrateOfflineV2AuthFromIdb(Number(planId))
  }

  if (isDeviceOffline()) {
    const cached = getCachedOfflineV2PilotAuth()
    if (cached) return cached
    // Sintetizar cache de lectura desde grant durable (solo UI local).
    if (uid && planId != null && memoryGrants.has(Number(planId))) {
      const g = memoryGrants.get(Number(planId))!
      const synth: OfflineV2PilotAuth = {
        writes_enabled: true,
        user_allowed: true,
        authorized_plan_ids: [],
        plan_id: Number(planId),
        plan_allowed: true,
        offline_v2_enabled: true,
        can_use_offline_v2: true,
        scope: "cached_plan_grant",
        fetched_at: Date.parse(g.confirmedAt) || Date.now(),
      }
      setCachedOfflineV2PilotAuth(synth)
      emitChanged()
      return synth
    }
    return getCachedOfflineV2PilotAuth()
  }

  const auth = await fetchPilotStatus(planId)
  if (!auth) {
    // Fallo de red / timeout: conservar cache + grants; no degradar a disabled.
    return getCachedOfflineV2PilotAuth()
  }

  setCachedOfflineV2PilotAuth(auth)

  if (uid) {
    if (!auth.offline_v2_enabled) {
      memoryGrants.clear()
      await clearOfflineV2PlanGrantsForUser(uid)
    } else if (planId != null && Number.isFinite(Number(planId))) {
      await persistGrantFromServer(uid, Number(planId), auth)
    }
  }

  emitChanged()
  return auth
}

export function clearOfflineV2PilotAuthorization(): void {
  memoryGrants.clear()
  setCachedOfflineV2PilotAuth(null)
  const uid = getSessionUserId()
  if (uid) {
    void clearOfflineV2PlanGrantsForUser(uid)
  }
}
