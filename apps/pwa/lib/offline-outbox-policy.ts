/**
 * Política Offline V2 outbox (2A.2/2A.3/2A.4).
 * Separada de snapshots regenerables Offline V1.
 *
 * Sync de escrituras: OFF por defecto. Nunca en hosts productivos Quillotana.
 *
 * Autoridad: el backend (`OFFLINE_SYNC_WRITES_ENABLED`) es la fuente de verdad.
 * NEXT_PUBLIC_* / localStorage lab solo abren el cliente en hosts no productivos;
 * NUNCA anulan un servidor con writes deshabilitados (503).
 *
 * Piloto productivo futuro (NO implementado aún):
 * 1) Aplicar 061 con aprobación explícita.
 * 2) Backend: OFFLINE_SYNC_WRITES_ENABLED=1 solo en workers autorizados.
 * 3) Allowlist de user_id / plan en servidor (no flag pública).
 * 4) PWA build lab o allowlist server-driven; jamás localStorage en *.quillotana.cl.
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

/** Bloqueos de publicación hasta aprobación separada. */
export const OFFLINE_V2_PUBLICATION_BLOCKERS = [
  "sync_writes_disabled_by_default_until_explicit_lab_flag",
  "no_production_sql_061_until_approved",
  "no_writes_to_api_quillotana_cl",
  "whatsapp_only_after_server_ack",
] as const

const LAB_FLAG_KEY = "lq:offline:syncWrites"

export function isProductionQuillotanaHost(
  hostname?: string | null,
): boolean {
  const h = String(hostname || "").toLowerCase()
  if (!h) return false
  return h === "api.quillotana.cl" || h.endsWith(".quillotana.cl") || h.includes("quillotana.cl")
}

/**
 * Sync de escrituras:
 * - Nunca en hosts *.quillotana.cl
 * - Requiere NEXT_PUBLIC_OFFLINE_SYNC_WRITES=1 O localStorage lab flag
 * - Default: false
 */
export function isOutboxSyncWritesEnabled(): boolean {
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

/** Solo laboratorio: activa flag local. No usar en prod. */
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
