/**
 * Orquestación de lectura offline (solo lectura).
 * Evita spinners infinitos: IDB primero si no hay red; API con timeout si hay red.
 */

import { getSessionUserId } from "./auth"
import {
  getOfflineClients,
  getOfflineClientDetail,
  getOfflinePlanReady,
  getOfflinePlans,
  listOfflineReadyPlans,
  type OfflinePlanReady,
} from "./offline-store"

export {
  isDeviceOffline,
  offlineBannerLabel,
  plansBundleUsable,
  shouldUseOfflineSnapshot,
} from "./offline-policy"

export type OfflineListOutcome<T> =
  | { status: "success"; items: T[]; syncedAt: string | null; readyCount: number }
  | { status: "empty"; message: string }
  | { status: "error"; message: string }

export async function readOfflineDespachos(): Promise<OfflineListOutcome<unknown>> {
  const uid = getSessionUserId()
  if (!uid) {
    return {
      status: "error",
      message: "Sin sesión local. Conéctate e inicia sesión para sincronizar.",
    }
  }
  try {
    const [cached, ready] = await Promise.all([
      getOfflinePlans(uid),
      listOfflineReadyPlans(uid),
    ])
    if (cached?.items?.length) {
      return {
        status: "success",
        items: cached.items,
        syncedAt: cached.syncedAt || null,
        readyCount: ready.length,
      }
    }
    return {
      status: "empty",
      message:
        "No tienes despachos descargados. Conéctate a internet para sincronizarlos.",
    }
  } catch {
    return {
      status: "error",
      message: "No se pudo leer el almacenamiento local. Reintenta.",
    }
  }
}

export async function readOfflineClients(
  planId: number,
): Promise<
  OfflineListOutcome<unknown> & { incomplete?: boolean; ready?: OfflinePlanReady | null }
> {
  const uid = getSessionUserId()
  if (!uid) {
    return {
      status: "error",
      message: "Sin sesión local. Conéctate e inicia sesión para sincronizar.",
    }
  }
  try {
    const [cached, ready] = await Promise.all([
      getOfflineClients(uid, planId),
      getOfflinePlanReady(uid, planId),
    ])
    if (cached?.items?.length) {
      return {
        status: "success",
        items: cached.items,
        syncedAt: cached.syncedAt || null,
        readyCount: ready?.complete ? 1 : 0,
        incomplete: Boolean(ready && !ready.complete),
        ready,
      }
    }
    return {
      status: "empty",
      message:
        "Este despacho no está disponible offline. Conéctate y sincronízalo desde Mis despachos.",
    }
  } catch {
    return {
      status: "error",
      message: "No se pudo leer clientes offline. Reintenta.",
    }
  }
}

export async function readOfflineClientDetail(
  planId: number,
  customerKey: string,
): Promise<
  | { status: "success"; payload: unknown; syncedAt: string }
  | { status: "empty"; message: string }
  | { status: "error"; message: string }
> {
  const uid = getSessionUserId()
  if (!uid) {
    return {
      status: "error",
      message: "Sin sesión local. Conéctate e inicia sesión para sincronizar.",
    }
  }
  try {
    const cached = await getOfflineClientDetail(uid, planId, customerKey)
    if (cached?.payload) {
      return {
        status: "success",
        payload: cached.payload,
        syncedAt: cached.syncedAt,
      }
    }
    return {
      status: "empty",
      message:
        "Esta ficha no está descargada. Conéctate y sincroniza el despacho completo.",
    }
  } catch {
    return {
      status: "error",
      message: "No se pudo leer la ficha offline. Reintenta.",
    }
  }
}

export async function readOfflinePlanFromList(
  planId: number,
): Promise<
  | { status: "success"; plan: unknown; syncedAt: string | null }
  | { status: "empty"; message: string }
  | { status: "error"; message: string }
> {
  const uid = getSessionUserId()
  if (!uid) {
    return {
      status: "error",
      message: "Sin sesión local. Conéctate e inicia sesión para sincronizar.",
    }
  }
  try {
    const cached = await getOfflinePlans(uid)
    const hit = (cached?.items || []).find(
      (raw) => Number((raw as { id?: number }).id) === planId,
    )
    if (hit) {
      return {
        status: "success",
        plan: hit,
        syncedAt: cached?.syncedAt || null,
      }
    }
    return {
      status: "empty",
      message:
        "Este despacho no está descargado. Conéctate para sincronizarlo desde Mis despachos.",
    }
  } catch {
    return {
      status: "error",
      message: "No se pudo leer el despacho offline. Reintenta.",
    }
  }
}
