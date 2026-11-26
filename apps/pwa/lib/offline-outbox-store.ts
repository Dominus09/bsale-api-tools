/**
 * IndexedDB outbox V2 — DB separada de snapshots Offline V1.
 * Nunca se limpia desde clearOfflineForUser / logout / 401 / BUILD_ID.
 */

import { canClaimOutboxSync, type OutboxOperation } from "./offline-outbox-types.ts"

const DB_NAME = "quillotana-ops-outbox-v2"
const DB_VERSION = 1
const STORE_OPS = "ops"
const STORE_BLOBS = "blobs"

export type OutboxBlobRecord = {
  id: string
  userId: string
  contentType: string
  sha256: string
  sizeBytes: number
  /** ArrayBuffer o Uint8Array persistido por IDB. */
  data: ArrayBuffer
  createdAt: string
}

type MemoryDb = {
  ops: Map<string, OutboxOperation>
  blobs: Map<string, OutboxBlobRecord>
}

/** Inyectable para tests Node sin IndexedDB. */
let memoryDb: MemoryDb | null = null

export function useOutboxMemoryStoreForTests(enabled: boolean): void {
  memoryDb = enabled ? { ops: new Map(), blobs: new Map() } : null
}

export function resetOutboxMemoryStoreForTests(): void {
  if (memoryDb) {
    memoryDb.ops.clear()
    memoryDb.blobs.clear()
  }
}

function openDb(): Promise<IDBDatabase> {
  return new Promise((resolve, reject) => {
    if (typeof indexedDB === "undefined") {
      reject(new Error("indexedDB unavailable"))
      return
    }
    const req = indexedDB.open(DB_NAME, DB_VERSION)
    req.onupgradeneeded = () => {
      const db = req.result
      if (!db.objectStoreNames.contains(STORE_OPS)) {
        const ops = db.createObjectStore(STORE_OPS, { keyPath: "localUuid" })
        ops.createIndex("byUser", "userId", { unique: false })
        ops.createIndex("byUserPlanCustomer", ["userId", "planId", "customerKey"], {
          unique: false,
        })
        ops.createIndex("byIdem", ["userId", "idempotency_key"], { unique: true })
        ops.createIndex("byStatus", "status", { unique: false })
      }
      if (!db.objectStoreNames.contains(STORE_BLOBS)) {
        const blobs = db.createObjectStore(STORE_BLOBS, { keyPath: "id" })
        blobs.createIndex("byUser", "userId", { unique: false })
      }
    }
    req.onsuccess = () => resolve(req.result)
    req.onerror = () => reject(req.error || new Error("outbox idb open failed"))
  })
}

export async function putOutboxOp(op: OutboxOperation): Promise<void> {
  if (memoryDb) {
    memoryDb.ops.set(op.localUuid, structuredClone(op))
    return
  }
  const db = await openDb()
  await new Promise<void>((resolve, reject) => {
    const tx = db.transaction(STORE_OPS, "readwrite")
    tx.objectStore(STORE_OPS).put(op)
    tx.oncomplete = () => resolve()
    tx.onerror = () => reject(tx.error || new Error("outbox put failed"))
  })
}

export async function getOutboxOp(localUuid: string): Promise<OutboxOperation | null> {
  if (memoryDb) {
    return memoryDb.ops.get(localUuid) ?? null
  }
  try {
    const db = await openDb()
    return await new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_OPS, "readonly")
      const req = tx.objectStore(STORE_OPS).get(localUuid)
      req.onsuccess = () => resolve((req.result as OutboxOperation) ?? null)
      req.onerror = () => reject(req.error)
    })
  } catch {
    return null
  }
}

export async function listOutboxOpsForUser(
  userId: string,
): Promise<OutboxOperation[]> {
  if (memoryDb) {
    return [...memoryDb.ops.values()]
      .filter((o) => o.userId === String(userId))
      .sort((a, b) => b.captured_at.localeCompare(a.captured_at))
  }
  try {
    const db = await openDb()
    return await new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_OPS, "readonly")
      const idx = tx.objectStore(STORE_OPS).index("byUser")
      const req = idx.getAll(String(userId))
      req.onsuccess = () => {
        const rows = (req.result as OutboxOperation[]) || []
        rows.sort((a, b) => b.captured_at.localeCompare(a.captured_at))
        resolve(rows)
      }
      req.onerror = () => reject(req.error)
    })
  } catch {
    return []
  }
}

export async function listOutboxOpsForPlanCustomer(
  userId: string,
  planId: number,
  customerKey: string,
): Promise<OutboxOperation[]> {
  const all = await listOutboxOpsForUser(userId)
  return all.filter(
    (o) => o.planId === Number(planId) && o.customerKey === customerKey,
  )
}

export async function putOutboxBlob(rec: OutboxBlobRecord): Promise<void> {
  if (memoryDb) {
    memoryDb.blobs.set(rec.id, {
      ...rec,
      data: rec.data.slice(0),
    })
    return
  }
  const db = await openDb()
  await new Promise<void>((resolve, reject) => {
    const tx = db.transaction(STORE_BLOBS, "readwrite")
    tx.objectStore(STORE_BLOBS).put(rec)
    tx.oncomplete = () => resolve()
    tx.onerror = () => reject(tx.error || new Error("outbox blob put failed"))
  })
}

export async function getOutboxBlob(
  blobId: string,
): Promise<OutboxBlobRecord | null> {
  if (memoryDb) {
    return memoryDb.blobs.get(blobId) ?? null
  }
  try {
    const db = await openDb()
    return await new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_BLOBS, "readonly")
      const req = tx.objectStore(STORE_BLOBS).get(blobId)
      req.onsuccess = () => resolve((req.result as OutboxBlobRecord) ?? null)
      req.onerror = () => reject(req.error)
    })
  } catch {
    return null
  }
}

/**
 * Verifica que el blob exista y coincida en tamaño/sha.
 * Obligatorio antes de marcar closed_store como pending.
 */
export async function verifyOutboxBlobPersisted(
  blobId: string,
  expectedSha: string,
  expectedSize: number,
): Promise<{ ok: true } | { ok: false; reason: string }> {
  const blob = await getOutboxBlob(blobId)
  if (!blob) return { ok: false, reason: "Fotografía no encontrada en almacenamiento local" }
  if (blob.sha256 !== expectedSha) {
    return { ok: false, reason: "Hash de fotografía no coincide" }
  }
  if (blob.sizeBytes !== expectedSize || blob.data.byteLength !== expectedSize) {
    return { ok: false, reason: "Tamaño de fotografía inconsistente" }
  }
  return { ok: true }
}

/**
 * Intencional: NO hay clearOutboxForUser en logout/401.
 * Solo procedimiento autorizado futuro.
 */
export function outboxClearApiBlocked(): string {
  return "Borrado de outbox no disponible en 2A.2 sin procedimiento autorizado"
}


/** Read/compare/write in one IDB transaction, including across tabs. */
async function mutateOutboxOp(
  localUuid: string,
  mutate: (current: OutboxOperation | null) => OutboxOperation | null,
): Promise<OutboxOperation | null> {
  if (memoryDb) {
    const current = memoryDb.ops.get(localUuid)
    const next = mutate(current ? structuredClone(current) : null)
    if (next) memoryDb.ops.set(localUuid, structuredClone(next))
    return next
  }
  const db = await openDb()
  try {
    return await new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_OPS, "readwrite")
      const store = tx.objectStore(STORE_OPS)
      const req = store.get(localUuid)
      let result: OutboxOperation | null = null
      req.onsuccess = () => {
        result = mutate((req.result as OutboxOperation | undefined) ?? null)
        if (result) store.put(result)
      }
      tx.oncomplete = () => resolve(result)
      tx.onabort = () => reject(tx.error || new Error("outbox mutation aborted"))
      tx.onerror = () => reject(tx.error || new Error("outbox mutation failed"))
    })
  } finally {
    db.close()
  }
}

export function claimOutboxOpForSync(
  localUuid: string,
  userId: string,
  now = Date.now(),
): Promise<OutboxOperation | null> {
  return mutateOutboxOp(localUuid, (current) => {
    if (!current || current.userId !== String(userId) || !canClaimOutboxSync(current, now)) return null
    return {
      ...current,
      status: "syncing",
      attempt_count: (current.attempt_count || 0) + 1,
      updated_at: new Date(now).toISOString(),
      last_error: null,
    }
  })
}

/** A late response from an expired lease cannot overwrite a newer attempt. */
export function settleOutboxSync(
  claim: OutboxOperation,
  changes: Pick<OutboxOperation, "status" | "last_error"> & Partial<Pick<OutboxOperation, "server_confirmed" | "sync_enabled">>,
): Promise<OutboxOperation | null> {
  return mutateOutboxOp(claim.localUuid, (current) => {
    if (!current || current.userId !== claim.userId || current.idempotency_key !== claim.idempotency_key ||
      current.status !== "syncing" || current.attempt_count !== claim.attempt_count ||
      current.updated_at !== claim.updated_at) return null
    return { ...current, ...changes, updated_at: new Date().toISOString() }
  })
}
