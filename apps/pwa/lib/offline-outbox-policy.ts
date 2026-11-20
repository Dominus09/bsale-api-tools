/**
 * Política Offline V2 outbox (2A.2).
 * Separada de snapshots regenerables Offline V1.
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
   * 2A.2: no enviar escrituras a API productiva.
   * Activar solo con aprobación explícita 2A.3+.
   */
  syncWritesEnabled: false as const,
  /**
   * IndexedDB no cifra por sí mismo. Aislamos por userId y no exponemos
   * ops ajenas en UI. No afirmar cifrado aplicativo.
   */
  encryptionClaim: "user_isolation_only_no_app_level_encryption" as const,
  authorizedDelete: "explicit_user_or_admin_procedure_only" as const,
} as const

/** Bloqueo de publicación 2A.1→prod / 2A.3 hasta validadores reales. */
export const OFFLINE_V2_PUBLICATION_BLOCKERS = [
  "picking_docs_validator_still_injectable_not_wired_to_ficha",
  "sync_writes_disabled_until_2a3_approval",
  "no_production_sql_061_until_approved",
] as const

export function isOutboxSyncWritesEnabled(): boolean {
  return OFFLINE_OUTBOX_RETENTION.syncWritesEnabled === true
}
