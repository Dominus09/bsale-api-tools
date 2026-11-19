/**
 * Descarga completa de un despacho para consulta offline (solo lectura).
 * Marca plan:*:ready.complete solo si clientes + fichas (docs/productos/cantidades)
 * y rutas HTML warm quedan validados.
 */

import {
  getMyCliente,
  listMyClientes,
  type DriverClientCard,
  type DriverDespacho,
} from "./api"
import { extractNextStaticAssets } from "./clients-list-safe"
import {
  detailLooksComplete,
  planSnapshotIntegrity,
  saveOfflineClientDetail,
  saveOfflineClients,
  saveOfflinePlanReady,
  type OfflinePlanReady,
} from "./offline-store"

export { detailLooksComplete, planSnapshotIntegrity }

const MAX_CLIENTS = 80
const DETAIL_CONCURRENCY = 3

export type SyncPlanProgress = {
  planId: number
  phase: "clients" | "details" | "warm" | "done" | "error"
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

/**
 * Precache HTML + chunks /_next/static de rutas del plan.
 * Sin los JS de la ruta, hard-nav offline rompe con client-side exception.
 */
export async function warmPlanRoutes(
  planId: number,
  customerKeys: string[] = [],
): Promise<{ ok: boolean; warmed: number; failed: string[]; assets: number }> {
  if (typeof window === "undefined") {
    return { ok: false, warmed: 0, failed: ["ssr"], assets: 0 }
  }
  const paths = [
    "/despachos",
    `/despachos/${planId}`,
    `/despachos/${planId}/clientes`,
    ...customerKeys.slice(0, MAX_CLIENTS).map(
      (key) => `/despachos/${planId}/clientes/${encodeURIComponent(key)}`,
    ),
  ]
  const failed: string[] = []
  let warmed = 0
  const assetSet = new Set<string>()

  await mapPool(paths, DETAIL_CONCURRENCY, async (path) => {
    try {
      const res = await fetch(path, {
        credentials: "same-origin",
        headers: { Accept: "text/html" },
      })
      if (!res.ok) {
        failed.push(path)
        return
      }
      warmed += 1
      const ct = (res.headers.get("content-type") || "").toLowerCase()
      if (ct.includes("text/html")) {
        const html = await res.text()
        for (const asset of extractNextStaticAssets(html)) {
          assetSet.add(asset)
        }
      }
    } catch {
      failed.push(path)
    }
  })

  let assetsOk = 0
  const assetList = [...assetSet]
  await mapPool(assetList, DETAIL_CONCURRENCY, async (assetPath) => {
    try {
      const res = await fetch(assetPath, { credentials: "same-origin" })
      if (!res.ok) {
        failed.push(assetPath)
        return
      }
      assetsOk += 1
    } catch {
      failed.push(assetPath)
    }
  })

  // HTML de shell + clientes es obligatorio; chunks se precachean best-effort
  // (muchas veces ya están en ASSET_CACHE tras abrir Mis despachos).
  const shellOk =
    !failed.includes("/despachos") &&
    !failed.includes(`/despachos/${planId}`) &&
    !failed.includes(`/despachos/${planId}/clientes`)
  return {
    ok: shellOk,
    warmed,
    failed,
    assets: assetsOk,
  }
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
  const summary = data.summary || null
  await saveOfflineClients(userId, planId, {
    items: list,
    count: list.length,
    summary: summary as Record<string, unknown> | undefined,
    syncedAt,
  })

  const truncated = list.length > MAX_CLIENTS
  const targets = list.slice(0, MAX_CLIENTS)
  const expected = targets.length
  let detailsCount = 0
  let documentsFromDetails = 0
  onProgress?.({
    planId,
    phase: "details",
    clientsTotal: list.length,
    detailsDone: 0,
    message: `Descargando fichas 0/${expected}…`,
  })

  await mapPool(targets, DETAIL_CONCURRENCY, async (c) => {
    const detail = await getMyCliente(planId, c.customer_key)
    if (
      !detailLooksComplete(detail, {
        document_count: c.document_count,
        units_total: c.units_total,
      })
    ) {
      throw new Error(`Ficha incompleta: ${c.customer_key}`)
    }
    documentsFromDetails += Array.isArray(detail.documents)
      ? detail.documents.length
      : 0
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

  const integrity = planSnapshotIntegrity({
    clients: list,
    summary,
    detailsOk: detailsCount,
    documentsFromDetails,
    truncated,
  })

  let complete = integrity.ok
  if (complete) {
    onProgress?.({
      planId,
      phase: "warm",
      clientsTotal: list.length,
      detailsDone: detailsCount,
      message: "Preparando navegación offline…",
    })
    const warm = await warmPlanRoutes(
      planId,
      targets.map((c) => c.customer_key),
    )
    if (!warm.ok) {
      complete = false
    }
  }

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
      ? `Listo sin conexión · ${integrity.expectedClients} clientes · ${integrity.expectedDocuments} docs`
      : integrity.ok
        ? "Rutas offline incompletas; reintenta con internet"
        : integrity.reason || "Descarga incompleta",
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
