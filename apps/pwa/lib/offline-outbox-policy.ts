/**
 * Política Offline V2 outbox.
 * Separada de snapshots regenerables Offline V1.
 *
 * Sync de escrituras: OFF por defecto (kill switch del servidor).
 * Con writes ON: todo chofer autenticado en sus despachos asignados.
 * NEXT_PUBLIC_* / localStorage NUNCA otorgan permisos en *.quillotana.cl
 * y no anulan un servidor con writes deshabilitados (503).
 */

export const OFFLINE_OUTBOX_RETENTION = {
  /** Snapshots V1 sí pueden borrarse; outbox NUNCA por estos eventos. */
  autoDeleteUnconfirmed: false as const,
  onLogout: "warn_and_retain" as const,
  on401: "retain_outbox" as const,
  onJwtExpired: "retain_outbox" as const,
  onOtherUserLogin: "isolate_only_never_wipe_other_outbox" as const,
  onBuildIdOrSwUpdate: "retain_outbox" as const,
  onReloadOrClose: "retain_outbox" as const,
  /**
   * Default de compilación: false.
   * Habilitación runtime: ver isOutboxSyncWritesEnabled().
   */
  syncWritesEnabled: false as const,
  encryptionClaim: "user_isolation_only_no_app_level_encryption" as const,
  authorizedDelete: "explicit_user_or_admin_procedure_only" as const,
} as const

/** Bloqueos / invariantes de publicación. */
export const OFFLINE_V2_PUBLICATION_BLOCKERS = [
  "sync_writes_disabled_by_default_until_server_flag",
  "no_client_flags_grant_backend_writes",
  "lab_route_404_on_quillotana",
  "whatsapp_only_after_server_ack",
] as const

const LAB_FLAG_KEY = "lq:offline:syncWrites"

export type OfflineV2PilotAuth = {
  writes_enabled: boolean
  user_allowed: boolean
  /** Vacío = todos los despachos asignados (sin allowlist). */
  authorized_plan_ids: number[]
  plan_id: number | null
  plan_allowed: boolean | null
  offline_v2_enabled: boolean
  can_use_offline_v2: boolean
  scope?: string
  fetched_at: number
}

/** Cache en memoria (+ sessionStorage) del estado del servidor. */
let serverPilotAuth: OfflineV2PilotAuth | null = null

const PILOT_CACHE_KEY = "lq:offline:v2:pilotAuth"

export function isProductionQuillotanaHost(
  hostname?: string | null,
): boolean {
  const h = String(hostname || "").toLowerCase()
  if (!h) return false
  return h === "api.quillotana.cl" || h.endsWith(".quillotana.cl") || h.includes("quillotana.cl")
}

function readCachedPilotFromSession(): OfflineV2PilotAuth | null {
  if (typeof window === "undefined") return null
  try {
    const raw = window.sessionStorage.getItem(PILOT_CACHE_KEY)
    if (!raw) return null
    const parsed = JSON.parse(raw) as OfflineV2PilotAuth
    if (!parsed || typeof parsed !== "object") return null
    if (typeof parsed.offline_v2_enabled !== "boolean") return null
    if (!Array.isArray(parsed.authorized_plan_ids)) {
      parsed.authorized_plan_ids = []
    }
    return parsed
  } catch {
    return null
  }
}

export function getCachedOfflineV2PilotAuth(): OfflineV2PilotAuth | null {
  if (serverPilotAuth) return serverPilotAuth
  const fromSession = readCachedPilotFromSession()
  if (fromSession) {
    serverPilotAuth = fromSession
    return fromSession
  }
  return null
}

export function setCachedOfflineV2PilotAuth(auth: OfflineV2PilotAuth | null): void {
  serverPilotAuth = auth
  if (typeof window === "undefined") return
  try {
    if (!auth) {
      window.sessionStorage.removeItem(PILOT_CACHE_KEY)
      return
    }
    window.sessionStorage.setItem(PILOT_CACHE_KEY, JSON.stringify(auth))
  } catch {
    /* private mode */
  }
}

/** Lab-only hints — nunca en hosts Quillotana. */
function labClientWritesHintEnabled(): boolean {
  if (typeof window !== "undefined" && isProductionQuillotanaHost(window.location.hostname)) {
    return false
  }
  if (process.env.NEXT_PUBLIC_OFFLINE_SYNC_WRITES === "1") {
    return true
  }
  if (typeof window !== "undefined") {
    try {
      if (window.localStorage.getItem(LAB_FLAG_KEY) === "1") return true
    } catch {
      /* private mode */
    }
  }
  return OFFLINE_OUTBOX_RETENTION.syncWritesEnabled === true
}

/**
 * Sync de escrituras habilitado en el cliente:
 * 1) Si hay estado de servidor → solo ese (kill switch).
 * 2) Sin cache → hints de laboratorio fuera de Quillotana.
 */
export function isOutboxSyncWritesEnabled(): boolean {
  const cached = getCachedOfflineV2PilotAuth()
  if (cached) {
    return cached.offline_v2_enabled === true
  }
  return labClientWritesHintEnabled()
}

/** ¿Mostrar UI de registro Offline V2 para este despacho? */
export function isOfflineV2UiEnabledForPlan(planId: number): boolean {
  const cached = getCachedOfflineV2PilotAuth()
  if (cached) {
    if (!cached.offline_v2_enabled) return false
    if (cached.plan_id === Number(planId) && cached.plan_allowed != null) {
      return cached.plan_allowed === true
    }
    // Sin allowlist: writes ON ⇒ UI en despachos del chofer (acceso ya filtrado).
    if (
      Array.isArray(cached.authorized_plan_ids) &&
      cached.authorized_plan_ids.length > 0
    ) {
      return cached.authorized_plan_ids.includes(Number(planId))
    }
    return true
  }
  return labClientWritesHintEnabled()
}

/** Solo laboratorio fuera de Quillotana. No usar en prod. */
export function setOutboxSyncWritesLabFlag(enabled: boolean): void {
  if (typeof window === "undefined") return
  if (isProductionQuillotanaHost(window.location.hostname)) return
  try {
    if (enabled) window.localStorage.setItem(LAB_FLAG_KEY, "1")
    else window.localStorage.removeItem(LAB_FLAG_KEY)
  } catch {
    /* ignore */
  }
}
