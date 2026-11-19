/**
 * Recorrido activo elegido por el chofer (local, por userId).
 * Rompe empates entre varios despachos del mismo nivel de prioridad.
 */

const PREFIX = "lq:ops:activePlan:"

function storageKey(userId: string): string {
  return `${PREFIX}${userId}`
}

export function getActivePlanId(userId: string | null | undefined): number | null {
  if (!userId || typeof window === "undefined") return null
  try {
    const raw = window.localStorage.getItem(storageKey(userId))
    if (!raw) return null
    const n = Number(raw)
    return Number.isFinite(n) && n > 0 ? n : null
  } catch {
    return null
  }
}

export function setActivePlanId(
  userId: string | null | undefined,
  planId: number,
): void {
  if (!userId || typeof window === "undefined") return
  if (!Number.isFinite(planId) || planId <= 0) return
  try {
    window.localStorage.setItem(storageKey(userId), String(planId))
  } catch {
    /* private mode */
  }
}

export function clearActivePlanId(userId: string | null | undefined): void {
  if (!userId || typeof window === "undefined") return
  try {
    window.localStorage.removeItem(storageKey(userId))
  } catch {
    /* ignore */
  }
}
