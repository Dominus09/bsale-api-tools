/**
 * Fingerprint del snapshot offline (picking + volumen).
 * No basta con fecha ni solo conteo de clientes.
 */

export type OfflineSnapshotFingerprint = {
  v: 1
  planId: number
  pickingId: number | null
  pickingVersion: string | null
  clients: number
  documents: number
  truncated: boolean
}

export function normalizePickingVersion(
  value: unknown,
): string | null {
  if (value == null || value === "") return null
  return String(value)
}

export function normalizePickingId(value: unknown): number | null {
  if (value == null || value === "") return null
  const n = Number(value)
  return Number.isFinite(n) ? n : null
}

export function buildOfflineFingerprint(input: {
  planId: number
  pickingId?: unknown
  pickingVersion?: unknown
  clients: number
  documents: number
  truncated?: boolean
}): OfflineSnapshotFingerprint {
  return {
    v: 1,
    planId: Number(input.planId),
    pickingId: normalizePickingId(input.pickingId),
    pickingVersion: normalizePickingVersion(input.pickingVersion),
    clients: Math.max(0, Math.floor(Number(input.clients) || 0)),
    documents: Math.max(0, Math.floor(Number(input.documents) || 0)),
    truncated: Boolean(input.truncated),
  }
}

/** Canonical string for equality / dismiss keys. */
export function fingerprintKey(fp: OfflineSnapshotFingerprint | null | undefined): string {
  if (!fp) return ""
  return [
    `v${fp.v}`,
    `p${fp.planId}`,
    `pk${fp.pickingId ?? "null"}`,
    `pv${fp.pickingVersion ?? "null"}`,
    `c${fp.clients}`,
    `d${fp.documents}`,
    `t${fp.truncated ? 1 : 0}`,
  ].join(":")
}

export function fingerprintsEqual(
  a: OfflineSnapshotFingerprint | null | undefined,
  b: OfflineSnapshotFingerprint | null | undefined,
): boolean {
  if (!a || !b) return false
  return fingerprintKey(a) === fingerprintKey(b)
}

/**
 * Identidad de picking usable para declarar snapshot vigente.
 * Sin picking_id + version reales no se puede saltar solo por conteos.
 */
export function hasReliablePickingIdentity(
  fp: OfflineSnapshotFingerprint | null | undefined,
): boolean {
  return (
    fp != null &&
    fp.pickingId != null &&
    Number.isFinite(fp.pickingId) &&
    fp.pickingVersion != null &&
    String(fp.pickingVersion).length > 0
  )
}

/** True if picking identity changed (requires re-download of fichas). */
export function pickingIdentityChanged(
  prev: OfflineSnapshotFingerprint | null | undefined,
  next: OfflineSnapshotFingerprint | null | undefined,
): boolean {
  if (!prev || !next) return true
  if (!hasReliablePickingIdentity(prev) || !hasReliablePickingIdentity(next)) {
    return true
  }
  if (prev.planId !== next.planId) return true
  if (prev.pickingId !== next.pickingId) return true
  if (prev.pickingVersion !== next.pickingVersion) return true
  if (prev.clients !== next.clients) return true
  if (prev.documents !== next.documents) return true
  if (prev.truncated !== next.truncated) return true
  return false
}

/** ¿Se puede reutilizar el snapshot local sin re-descargar fichas? */
export function canReuseOfflineSnapshot(
  prev: OfflineSnapshotFingerprint | null | undefined,
  next: OfflineSnapshotFingerprint | null | undefined,
): boolean {
  if (!hasReliablePickingIdentity(prev) || !hasReliablePickingIdentity(next)) {
    return false
  }
  return fingerprintsEqual(prev, next) && !pickingIdentityChanged(prev, next)
}
