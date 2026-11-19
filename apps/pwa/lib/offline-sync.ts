/**
 * Descarga completa de un despacho para consulta offline (solo lectura).
 * Marca plan:*:ready.complete solo si clientes + fichas (docs/productos) quedaron validados.
 */

import {
  getMyCliente,
  listMyClientes,
  type DriverClientCard,
  type DriverDespacho,
} from "./api"
import {
  detailLooksComplete,
  saveOfflineClientDetail,
  saveOfflineClients,
  saveOfflinePlanReady,
  type OfflinePlanReady,
} from "./offline-store"

export { detailLooksComplete }

const MAX_CLIENTS = 80
const DETAIL_CONCURRENCY = 3

export type SyncPlanProgress = {
  planId: number
  phase: "clients" | "details" | "done" | "error"
  clientsTotal: number
  detailsDone: number
  message: string
}

async function mapPool<T, R>(
  items: T[],
  concurrency: number,
  worker: (item: T, index: number) => Promise<R>,
): Promise<R[]> {
  const out: R[] = new Array(items.length)
  let next = 0
  async function run() {
    while (next < items.length) {
      const i = next
      next += 1
      out[i] = await worker(items[i], i)
    }
  }
  const n = Math.min(concurrency, Math.max(1, items.length))
  await Promise.all(Array.from({ length: n }, () => run()))
  return out
}

/** Precache HTML de rutas del plan para navegación documento offline. */
async function warmPlanRoutes(
  planId: number,
  customerKeys: string[] = [],
): Promise<void> {
  if (typeof window === "undefined") return
  const paths = [
    `/despachos/${planId}`,
    `/despachos/${planId}/clientes`,
    ...customerKeys.slice(0, MAX_CLIENTS).map(
      (key) => `/despachos/${planId}/clientes/${encodeURIComponent(key)}`,
    ),
  ]
  await mapPool(paths, DETAIL_CONCURRENCY, async (path) => {
    try {
      await fetch(path, { credentials: "same-origin" })
    } catch {
      /* ignore */
    }
  })
}

export async function syncPlanForOffline(
  userId: string,
  planId: number,
  onProgress?: (p: SyncPlanProgress) => void,
): Promise<OfflinePlanReady> {
  const syncedAt = new Date().toISOString()
  onProgress?.({
    planId,
    phase: "clients",
    clientsTotal: 0,
    detailsDone: 0,
    message: "Descargando clientes…",
  })

  const data = await listMyClientes(planId)
  const list = (data.items || []) as DriverClientCard[]
  await saveOfflineClients(userId, planId, {
    items: list,
    count: list.length,
    summary: data.summary as Record<string, unknown> | undefined,
    syncedAt,
  })

  const targets = list.slice(0, MAX_CLIENTS)
  const expected = targets.length
  let detailsCount = 0
  onProgress?.({
    planId,
    phase: "details",
    clientsTotal: list.length,
    detailsDone: 0,
    message: `Descargando fichas 0/${expected}…`,
  })

  await mapPool(targets, DETAIL_CONCURRENCY, async (c) => {
    const detail = await getMyCliente(planId, c.customer_key)
    if (!detailLooksComplete(detail)) {
      throw new Error(`Ficha incompleta: ${c.customer_key}`)
    }
    await saveOfflineClientDetail(userId, planId, c.customer_key, {
      payload: detail,
      syncedAt,
    })
    detailsCount += 1
    onProgress?.({
      planId,
      phase: "details",
      clientsTotal: list.length,
      detailsDone: detailsCount,
      message: `Descargando fichas ${detailsCount}/${expected}…`,
    })
  })

  const complete = list.length === 0 || (expected > 0 && detailsCount >= expected)
  const ready: OfflinePlanReady = {
    planId,
    clientsCount: list.length,
    detailsCount,
    syncedAt,
    complete,
  }
  await saveOfflinePlanReady(userId, ready)
  if (complete) {
    void warmPlanRoutes(
      planId,
      targets.map((c) => c.customer_key),
    )
  }
  onProgress?.({
    planId,
    phase: complete ? "done" : "error",
    clientsTotal: list.length,
    detailsDone: detailsCount,
    message: complete
      ? "Listo para usar sin conexión"
      : `Descarga incompleta (${detailsCount}/${expected} fichas)`,
  })
  return ready
}

/** Sincroniza planes prioritarios sin abrir cada ficha a mano. */
export async function syncPriorityPlansForOffline(
  userId: string,
  plans: DriverDespacho[],
  onProgress?: (p: SyncPlanProgress) => void,
): Promise<OfflinePlanReady[]> {
  const out: OfflinePlanReady[] = []
  for (const plan of plans) {
    try {
      const ready = await syncPlanForOffline(userId, plan.id, onProgress)
      out.push(ready)
    } catch {
      const failed: OfflinePlanReady = {
        planId: plan.id,
        clientsCount: 0,
        detailsCount: 0,
        syncedAt: new Date().toISOString(),
        complete: false,
      }
      await saveOfflinePlanReady(userId, failed)
      out.push(failed)
      onProgress?.({
        planId: plan.id,
        phase: "error",
        clientsTotal: 0,
        detailsDone: 0,
        message: `No se pudo sincronizar plan #${plan.id}`,
      })
    }
  }
  return out
}
