/**
 * Almacén offline de solo lectura (IndexedDB).
 * Aislado por userId. No guarda tokens ni contraseñas.
 * No simula escrituras de entrega / GPS / R2 / WhatsApp.
 */

const DB_NAME = "quillotana-ops-offline"
const DB_VERSION = 1
const STORE = "kv"

export type OfflineMeta = {
  userId: string
  email: string
  profiles: string[]
  syncedAt: string
}

export type OfflinePlansBundle = {
  items: unknown[]
  count: number
  syncedAt: string
}

export type OfflineClientsBundle = {
  items: unknown[]
  count: number
  summary?: Record<string, unknown>
  syncedAt: string
}

export type OfflineClientDetail = {
  payload: unknown
  syncedAt: string
}

/** Estado de descarga completa por despacho (evita declarar offline parcial). */
export type OfflinePlanReady = {
  planId: number
  clientsCount: number
  detailsCount: number
  syncedAt: string
  /** true solo si hay lista de clientes y al menos 1 ficha, o 0 clientes legítimos. */
  complete: boolean
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
      if (!db.objectStoreNames.contains(STORE)) {
        db.createObjectStore(STORE)
      }
    }
    req.onsuccess = () => resolve(req.result)
    req.onerror = () => reject(req.error || new Error("idb open failed"))
  })
}

async function idbGet<T>(key: string): Promise<T | null> {
  try {
    const db = await openDb()
    return await new Promise((resolve, reject) => {
      const tx = db.transaction(STORE, "readonly")
      const req = tx.objectStore(STORE).get(key)
      req.onsuccess = () => resolve((req.result as T) ?? null)
      req.onerror = () => reject(req.error)
    })
  } catch {
    return null
  }
}

async function idbSet(key: string, value: unknown): Promise<void> {
  try {
    const db = await openDb()
    await new Promise<void>((resolve, reject) => {
      const tx = db.transaction(STORE, "readwrite")
      tx.objectStore(STORE).put(value, key)
      tx.oncomplete = () => resolve()
      tx.onerror = () => reject(tx.error)
    })
  } catch {
    /* quota / private mode */
  }
}

async function idbDeletePrefix(prefix: string): Promise<void> {
  try {
    const db = await openDb()
    await new Promise<void>((resolve, reject) => {
      const tx = db.transaction(STORE, "readwrite")
      const store = tx.objectStore(STORE)
      const req = store.openCursor()
      req.onsuccess = () => {
        const cursor = req.result
        if (!cursor) return
        if (String(cursor.key).startsWith(prefix)) cursor.delete()
        cursor.continue()
      }
      tx.oncomplete = () => resolve()
      tx.onerror = () => reject(tx.error)
    })
  } catch {
    /* ignore */
  }
}

export function offlineUserPrefix(userId: string | number | null | undefined): string {
  return `u:${String(userId || "anon")}:`
}

export async function saveOfflineMeta(meta: OfflineMeta): Promise<void> {
  await idbSet(`${offlineUserPrefix(meta.userId)}meta`, meta)
}

export async function getOfflineMeta(
  userId: string | number,
): Promise<OfflineMeta | null> {
  return idbGet(`${offlineUserPrefix(userId)}meta`)
}

export async function saveOfflinePlans(
  userId: string | number,
  bundle: OfflinePlansBundle,
): Promise<void> {
  await idbSet(`${offlineUserPrefix(userId)}plans`, bundle)
}

export async function getOfflinePlans(
  userId: string | number,
): Promise<OfflinePlansBundle | null> {
  return idbGet(`${offlineUserPrefix(userId)}plans`)
}

export async function saveOfflineClients(
  userId: string | number,
  planId: number,
  bundle: OfflineClientsBundle,
): Promise<void> {
  await idbSet(`${offlineUserPrefix(userId)}plan:${planId}:clients`, bundle)
}

export async function getOfflineClients(
  userId: string | number,
  planId: number,
): Promise<OfflineClientsBundle | null> {
  return idbGet(`${offlineUserPrefix(userId)}plan:${planId}:clients`)
}

export async function saveOfflineClientDetail(
  userId: string | number,
  planId: number,
  customerKey: string,
  detail: OfflineClientDetail,
): Promise<void> {
  const safeKey = encodeURIComponent(customerKey)
  await idbSet(
    `${offlineUserPrefix(userId)}plan:${planId}:client:${safeKey}`,
    detail,
  )
}

export async function getOfflineClientDetail(
  userId: string | number,
  planId: number,
  customerKey: string,
): Promise<OfflineClientDetail | null> {
  const safeKey = encodeURIComponent(customerKey)
  return idbGet(`${offlineUserPrefix(userId)}plan:${planId}:client:${safeKey}`)
}

export async function saveOfflinePlanReady(
  userId: string | number,
  ready: OfflinePlanReady,
): Promise<void> {
  await idbSet(`${offlineUserPrefix(userId)}plan:${ready.planId}:ready`, ready)
}

export async function getOfflinePlanReady(
  userId: string | number,
  planId: number,
): Promise<OfflinePlanReady | null> {
  return idbGet(`${offlineUserPrefix(userId)}plan:${planId}:ready`)
}

export async function listOfflineReadyPlans(
  userId: string | number,
): Promise<OfflinePlanReady[]> {
  try {
    const db = await openDb()
    const prefix = `${offlineUserPrefix(userId)}plan:`
    const out: OfflinePlanReady[] = []
    await new Promise<void>((resolve, reject) => {
      const tx = db.transaction(STORE, "readonly")
      const req = tx.objectStore(STORE).openCursor()
      req.onsuccess = () => {
        const cursor = req.result
        if (!cursor) return
        const key = String(cursor.key)
        if (key.startsWith(prefix) && key.endsWith(":ready")) {
          const val = cursor.value as OfflinePlanReady
          if (val?.complete) out.push(val)
        }
        cursor.continue()
      }
      tx.oncomplete = () => resolve()
      tx.onerror = () => reject(tx.error)
    })
    return out
  } catch {
    return []
  }
}

/** Borra solo el namespace del usuario (cambio de cuenta / logout). */
export async function clearOfflineForUser(
  userId: string | number | null | undefined,
): Promise<void> {
  if (userId == null || userId === "") return
  await idbDeletePrefix(offlineUserPrefix(userId))
}

export function formatSyncedAt(iso: string | null | undefined): string {
  if (!iso) return "—"
  try {
    return new Intl.DateTimeFormat("es-CL", {
      dateStyle: "short",
      timeStyle: "short",
      timeZone: "America/Santiago",
    }).format(new Date(iso))
  } catch {
    return iso
  }
}

export function isLikelyOfflineError(err: unknown): boolean {
  if (typeof navigator !== "undefined" && navigator.onLine === false) return true
  if (err && typeof err === "object" && "status" in err) {
    const status = Number((err as { status?: number }).status)
    return status === 0 || status === 503 || status === 504
  }
  return false
}

export function isDeviceOffline(): boolean {
  return typeof navigator !== "undefined" && navigator.onLine === false
}

export function shouldUseOfflineSnapshot(
  navigatorOnline: boolean | undefined,
  err?: unknown,
): boolean {
  if (navigatorOnline === false) return true
  return isLikelyOfflineError(err)
}

export function plansBundleUsable(bundle: OfflinePlansBundle | null): boolean {
  return Boolean(bundle?.items && bundle.items.length > 0)
}

export function offlineBannerLabel(syncedAt: string | null | undefined): string {
  if (!syncedAt) return "Sin conexión · sin datos sincronizados"
  return `Sin conexión · Datos del ${formatSyncedAt(syncedAt)}`
}

/** Valida que una ficha sync tenga estructura mínima (client + documents). */
export function detailLooksComplete(payload: unknown): boolean {
  if (!payload || typeof payload !== "object") return false
  const p = payload as {
    client?: unknown
    documents?: unknown[]
  }
  if (!p.client || typeof p.client !== "object") return false
  return Array.isArray(p.documents)
}
