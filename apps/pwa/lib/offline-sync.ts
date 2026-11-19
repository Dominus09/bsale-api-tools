/**
 * Descarga completa de un despacho para consulta offline (solo lectura).
 * Marca plan:*:ready.complete solo si clientes + fichas quedaron validados.
 */

import {
  getMyCliente,
  listMyClientes,
  type DriverClientCard,
  type DriverDespacho,
} from "./api"
import {
  saveOfflineClientDetail,
  saveOfflineClients,
  saveOfflinePlanReady,
  type OfflinePlanReady,
} from "./offline-store"

const MAX_CLIENTS = 80

export type SyncPlanProgress = {
  planId: number
  phase: "clients" | "details" | "done" | "error"
  clientsTotal: number
  detailsDone: number
  message: string
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

  const expected = Math.min(list.length, MAX_CLIENTS)
  let detailsCount = 0
  onProgress?.({
    planId,
    phase: "details",
    clientsTotal: list.length,
    detailsDone: 0,
    message: `Descargando fichas 0/${expected}…`,
  })

  for (const c of list.slice(0, MAX_CLIENTS)) {
    const detail = await getMyCliente(planId, c.customer_key)
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
  }

  const complete = list.length === 0 || (expected > 0 && detailsCount >= expected)
  const ready: OfflinePlanReady = {
    planId,
    clientsCount: list.length,
    detailsCount,
    syncedAt,
    complete,
  }
  await saveOfflinePlanReady(userId, ready)
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

/** Sincroniza planes prioritarios (hoy + pendientes) sin abrir cada ficha a mano. */
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
