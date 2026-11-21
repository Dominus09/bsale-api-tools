/**
 * Descarga completa de un despacho para consulta offline (solo lectura).
 * Separa datos de negocio (IDB) de navegación (HTML/chunks del build actual).
 * Skip por fingerprint cuando data + nav + BUILD_ID siguen vigentes.
 */

import {
  getMyCliente,
  listMyClientes,
  type DriverClientCard,
  type DriverDespacho,
} from "./api"
import { extractNextStaticAssets } from "./clients-list-safe"
import { loadOfflineBuildInfo } from "./offline-build"
import {
  buildOfflineFingerprint,
  canReuseOfflineSnapshot,
  type OfflineSnapshotFingerprint,
} from "./offline-fingerprint"
import {
  detailLooksComplete,
  getOfflineClients,
  getOfflinePlanReady,
  listOfflineReadyPlans,
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
  phase: "clients" | "details" | "warm" | "done" | "error" | "skip"
  clientsTotal: number
  detailsDone: number
  documentsDone?: number
  documentsTotal?: number
  message: string
  skipped?: boolean
}

export type WarmPlanResult = {
  ok: boolean
  warmed: number
  failed: string[]
  assets: number
  criticalAssets: string[]
  navBuildId: string
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

function emptyReady(
  planId: number,
  syncedAt: string,
  navBuildId: string | null,
  extra?: Partial<OfflinePlanReady>,
): OfflinePlanReady {
  return {
    planId,
    clientsCount: 0,
    detailsCount: 0,
    documentsCount: 0,
    syncedAt,
    dataComplete: false,
    navReady: false,
    navBuildId,
    fingerprint: null,
    complete: false,
    ...extra,
  }
}

export function fingerprintFromClientesPayload(
  planId: number,
  data: {
    picking_id?: number | null
    picking_version?: number | string | null
    items?: DriverClientCard[]
    summary?: { clients?: number; documents?: number } | null
  },
  truncated: boolean,
): OfflineSnapshotFingerprint {
  const list = data.items || []
  const clients =
    typeof data.summary?.clients === "number" ? data.summary.clients : list.length
  const documents =
    typeof data.summary?.documents === "number"
      ? data.summary.documents
      : list.reduce((acc, c) => acc + (Number(c.document_count) || 0), 0)
  return buildOfflineFingerprint({
    planId,
    pickingId: data.picking_id,
    pickingVersion: data.picking_version,
    clients,
    documents,
    truncated,
  })
}

/**
 * Precache HTML + chunks /_next/static.
 * `ok` exige HTML crítico Y todos los JS/CSS extraídos de esos HTML.
 */
export async function warmPlanRoutes(
  planId: number,
  customerKeys: string[] = [],
): Promise<WarmPlanResult> {
  const build = await loadOfflineBuildInfo()
  if (typeof window === "undefined") {
    return {
      ok: false,
      warmed: 0,
      failed: ["ssr"],
      assets: 0,
      criticalAssets: [],
      navBuildId: build.navBuildId,
    }
  }

  const criticalPaths = [
    "/despachos",
    `/despachos/${planId}`,
    `/despachos/${planId}/clientes`,
  ]
  const optionalPaths = customerKeys.slice(0, MAX_CLIENTS).map(
    (key) => `/despachos/${planId}/clientes/${encodeURIComponent(key)}`,
  )
  const paths = [...criticalPaths, ...optionalPaths]
  const failed: string[] = []
  let warmed = 0
  const assetsByPath = new Map<string, string[]>()

  await mapPool(paths, DETAIL_CONCURRENCY, async (path) => {
    try {
      const res = await fetch(path, {
        credentials: "same-origin",
        redirect: "follow",
        headers: { Accept: "text/html" },
      })
      // No aceptar HTML de login / redirect cruzado como warm de la ruta.
      if (!res.ok || res.redirected) {
        failed.push(path)
        return
      }
      const finalPath = new URL(res.url).pathname
      if (finalPath !== path) {
        failed.push(path)
        return
      }
      const ct = (res.headers.get("content-type") || "").toLowerCase()
      if (!ct.includes("text/html")) {
        failed.push(path)
        return
      }
      warmed += 1
      const html = await res.text()
      if (/\/login/i.test(html) && /password|contrase/i.test(html)) {
        failed.push(path)
        return
      }
      assetsByPath.set(path, extractNextStaticAssets(html))
    } catch {
      failed.push(path)
    }
  })

  const criticalAssets = new Set<string>()
  for (const path of criticalPaths) {
    for (const a of assetsByPath.get(path) || []) criticalAssets.add(a)
  }
  // También precachear assets de fichas (no bloquean ok si fallan).
  const optionalAssets = new Set<string>()
  for (const path of optionalPaths) {
    for (const a of assetsByPath.get(path) || []) {
      if (!criticalAssets.has(a)) optionalAssets.add(a)
    }
  }

  let assetsOk = 0
  await mapPool([...criticalAssets], DETAIL_CONCURRENCY, async (assetPath) => {
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
  await mapPool([...optionalAssets], DETAIL_CONCURRENCY, async (assetPath) => {
    try {
      const res = await fetch(assetPath, { credentials: "same-origin" })
      if (res.ok) assetsOk += 1
    } catch {
      /* optional */
    }
  })

  const shellOk = criticalPaths.every((p) => !failed.includes(p))
  const criticalAssetsOk = [...criticalAssets].every((a) => !failed.includes(a))
  return {
    ok: shellOk && criticalAssetsOk && criticalAssets.size > 0,
    warmed,
    failed,
    assets: assetsOk,
    criticalAssets: [...criticalAssets],
    navBuildId: build.navBuildId,
  }
}

export type SyncPlanOptions = {
  force?: boolean
  onProgress?: (p: SyncPlanProgress) => void
}

export async function syncPlanForOffline(
  userId: string,
  planId: number,
  onProgressOrOpts?: ((p: SyncPlanProgress) => void) | SyncPlanOptions,
): Promise<OfflinePlanReady> {
  const opts: SyncPlanOptions =
    typeof onProgressOrOpts === "function"
      ? { onProgress: onProgressOrOpts }
      : onProgressOrOpts || {}
  const onProgress = opts.onProgress
  const force = Boolean(opts.force)
  const syncedAt = new Date().toISOString()
  const build = await loadOfflineBuildInfo()
  onProgress?.({
    planId,
    phase: "clients",
    clientsTotal: 0,
    detailsDone: 0,
    message: "Consultando despacho…",
  })

  const data = await listMyClientes(planId)
  if (data.picking_ready === false) {
    const failed = emptyReady(planId, syncedAt, build.navBuildId, {
      fingerprint: fingerprintFromClientesPayload(planId, data, false),
    })
    await saveOfflinePlanReady(userId, failed)
    onProgress?.({
      planId,
      phase: "error",
      clientsTotal: 0,
      detailsDone: 0,
      message: data.picking_reason || "Picking no disponible aún",
    })
    return failed
  }

  const list = (data.items || []) as DriverClientCard[]
  const summary = data.summary || null
  const truncated = list.length > MAX_CLIENTS
  const nextFp = fingerprintFromClientesPayload(planId, data, truncated)
  const existing = await getOfflinePlanReady(userId, planId)
  const dataOk = Boolean(existing?.dataComplete ?? existing?.complete)
  const navOk =
    Boolean(existing?.navReady) && existing?.navBuildId === build.navBuildId

  // Solo reutilizar si hay picking_id+version reales y el fingerprint coincide.
  // Sin versión confiable no se declara vigente solo por conteos.
  if (!force && dataOk && canReuseOfflineSnapshot(existing?.fingerprint, nextFp)) {
    if (navOk) {
      const skipped: OfflinePlanReady = {
        ...existing!,
        fingerprint: nextFp,
        pickingId: nextFp.pickingId,
        pickingVersion: nextFp.pickingVersion,
        documentsCount: nextFp.documents,
        clientsCount: existing!.clientsCount || nextFp.clients,
        dataComplete: true,
        navReady: true,
        navBuildId: build.navBuildId,
        complete: true,
      }
      await saveOfflinePlanReady(userId, skipped)
      onProgress?.({
        planId,
        phase: "skip",
        clientsTotal: skipped.clientsCount,
        detailsDone: skipped.detailsCount,
        documentsDone: skipped.documentsCount,
        documentsTotal: nextFp.documents,
        message: "Ya preparado · sin cambios de picking",
        skipped: true,
      })
      return skipped
    }
    // Solo BUILD_ID / nav: re-warm sin fichas.
    onProgress?.({
      planId,
      phase: "warm",
      clientsTotal: existing!.clientsCount,
      detailsDone: existing!.detailsCount,
      documentsDone: existing!.documentsCount,
      documentsTotal: nextFp.documents,
      message: "Actualizando navegación de esta versión…",
    })
    const clients = await getOfflineClients(userId, planId)
    const keys = (clients?.items || [])
      .map((raw) =>
        raw && typeof raw === "object"
          ? String((raw as { customer_key?: string }).customer_key || "")
          : "",
      )
      .filter(Boolean)
      .slice(0, MAX_CLIENTS)
    const warm = await warmPlanRoutes(planId, keys)
    const refreshed: OfflinePlanReady = {
      ...existing!,
      fingerprint: nextFp,
      pickingId: nextFp.pickingId,
      pickingVersion: nextFp.pickingVersion,
      documentsCount: nextFp.documents,
      dataComplete: true,
      navReady: warm.ok,
      navBuildId: warm.navBuildId,
      complete: warm.ok,
      syncedAt: new Date().toISOString(),
    }
    await saveOfflinePlanReady(userId, refreshed)
    onProgress?.({
      planId,
      phase: refreshed.complete ? "done" : "error",
      clientsTotal: refreshed.clientsCount,
      detailsDone: refreshed.detailsCount,
      documentsDone: refreshed.documentsCount,
      documentsTotal: nextFp.documents,
      message: refreshed.complete
        ? "Navegación lista · datos previos vigentes"
        : "Datos OK · falta cache de navegación de este build",
    })
    return refreshed
  }

  await saveOfflineClients(userId, planId, {
    items: list,
    count: list.length,
    summary: summary as Record<string, unknown> | undefined,
    syncedAt,
  })

  const targets = list.slice(0, MAX_CLIENTS)
  const expected = targets.length
  let detailsCount = 0
  let documentsFromDetails = 0
  onProgress?.({
    planId,
    phase: "details",
    clientsTotal: list.length,
    detailsDone: 0,
    documentsDone: 0,
    documentsTotal: nextFp.documents,
    message: truncated
      ? `Demasiados clientes (${list.length}>${MAX_CLIENTS}) · sync V1 truncado`
      : `Descargando fichas 0/${expected}…`,
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
      documentsDone: documentsFromDetails,
      documentsTotal: nextFp.documents,
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

  const dataComplete = integrity.ok
  let navReady = false
  let navBuildId: string | null = null

  if (dataComplete) {
    onProgress?.({
      planId,
      phase: "warm",
      clientsTotal: list.length,
      detailsDone: detailsCount,
      documentsDone: documentsFromDetails,
      documentsTotal: integrity.expectedDocuments,
      message: "Preparando navegación offline…",
    })
    const warm = await warmPlanRoutes(
      planId,
      targets.map((c) => c.customer_key),
    )
    navReady = warm.ok
    navBuildId = warm.navBuildId
  }

  const complete = dataComplete && navReady && navBuildId === build.navBuildId
  const ready: OfflinePlanReady = {
    planId,
    clientsCount: list.length,
    detailsCount,
    documentsCount: documentsFromDetails,
    syncedAt,
    dataComplete,
    navReady,
    navBuildId,
    fingerprint: nextFp,
    pickingId: nextFp.pickingId,
    pickingVersion: nextFp.pickingVersion,
    complete,
  }
  await saveOfflinePlanReady(userId, ready)
  onProgress?.({
    planId,
    phase: complete ? "done" : "error",
    clientsTotal: list.length,
    detailsDone: detailsCount,
    documentsDone: documentsFromDetails,
    documentsTotal: integrity.expectedDocuments,
    message: complete
      ? `Listo sin conexión · ${integrity.expectedClients} clientes · ${integrity.expectedDocuments} docs`
      : !dataComplete
        ? integrity.reason || "Descarga incompleta"
        : "Datos guardados · falta cache de navegación de este build",
  })
  return ready
}

/**
 * Tras deploy (SW v8 + BUILD_ID nuevo): no borra fichas/productos.
 * Marca complete=false si navBuildId no coincide; re-warms online.
 */
export async function reconcileOfflineNavForCurrentBuild(
  userId: string,
): Promise<OfflinePlanReady[]> {
  const build = await loadOfflineBuildInfo()
  const existing = await listOfflineReadyPlans(userId, {
    includeDataOnly: true,
  })
  const out: OfflinePlanReady[] = []

  for (const row of existing) {
    const dataOk = Boolean(row.dataComplete ?? row.complete)
    if (!dataOk) {
      out.push(row)
      continue
    }
    const buildMatch = row.navBuildId === build.navBuildId && Boolean(row.navReady)
    if (buildMatch) {
      const ok: OfflinePlanReady = {
        ...row,
        dataComplete: true,
        navReady: true,
        navBuildId: build.navBuildId,
        complete: true,
      }
      await saveOfflinePlanReady(userId, ok)
      out.push(ok)
      continue
    }

    // Demote UI readiness; keep data.
    const demoted: OfflinePlanReady = {
      ...row,
      dataComplete: true,
      navReady: false,
      navBuildId: row.navBuildId || null,
      complete: false,
    }
    await saveOfflinePlanReady(userId, demoted)

    if (typeof navigator !== "undefined" && navigator.onLine === false) {
      out.push(demoted)
      continue
    }

    const clients = await getOfflineClients(userId, row.planId)
    const keys = (clients?.items || [])
      .map((raw) =>
        raw && typeof raw === "object"
          ? String((raw as { customer_key?: string }).customer_key || "")
          : "",
      )
      .filter(Boolean)
    const warm = await warmPlanRoutes(row.planId, keys)
    const refreshed: OfflinePlanReady = {
      ...demoted,
      navReady: warm.ok,
      navBuildId: warm.navBuildId,
      complete: warm.ok,
      syncedAt: new Date().toISOString(),
    }
    await saveOfflinePlanReady(userId, refreshed)
    out.push(refreshed)
  }

  return out
}

/**
 * @deprecated Prefer syncSinglePriorityPlanForOffline (máx. 1 por ciclo).
 * Conservado: sincroniza solo el primer plan de la lista.
 */
export async function syncPriorityPlansForOffline(
  userId: string,
  plans: DriverDespacho[],
  onProgress?: (p: SyncPlanProgress) => void,
): Promise<OfflinePlanReady[]> {
  if (!plans.length) return []
  const ready = await syncSinglePriorityPlanForOffline(userId, plans[0], {
    onProgress,
  })
  return [ready]
}

/** Descarga automática: un solo despacho prioritario por ciclo. */
export async function syncSinglePriorityPlanForOffline(
  userId: string,
  plan: DriverDespacho,
  opts?: SyncPlanOptions,
): Promise<OfflinePlanReady> {
  const build = await loadOfflineBuildInfo()
  try {
    return await syncPlanForOffline(userId, plan.id, opts)
  } catch {
    const failed = emptyReady(plan.id, new Date().toISOString(), build.navBuildId)
    await saveOfflinePlanReady(userId, failed)
    opts?.onProgress?.({
      planId: plan.id,
      phase: "error",
      clientsTotal: 0,
      detailsDone: 0,
      message: `No se pudo sincronizar plan #${plan.id}`,
    })
    return failed
  }
}

export async function getPlanReadyForUi(
  userId: string,
  planId: number,
): Promise<OfflinePlanReady | null> {
  const ready = await getOfflinePlanReady(userId, planId)
  if (!ready) return null
  const build = await loadOfflineBuildInfo()
  const dataOk = Boolean(ready.dataComplete ?? ready.complete)
  const navOk =
    Boolean(ready.navReady) && ready.navBuildId === build.navBuildId
  return {
    ...ready,
    dataComplete: dataOk,
    navReady: navOk,
    complete: dataOk && navOk,
  }
}
