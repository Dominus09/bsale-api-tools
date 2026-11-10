/** Reglas de recuperación post-cierre / multi-visita (sin mutar servidor). */

import type { DeliveryVisit } from "./api"

function isClosed(status: string | null | undefined): boolean {
  const s = String(status || "")
  return (
    s === "completed" ||
    s === "partial" ||
    s === "not_delivered" ||
    s === "cancelled"
  )
}

/** Cierre válido operativo (bloquea nueva visita / retom de duplicada). */
export function isValidClosedStatus(status: string | null | undefined): boolean {
  const s = String(status || "")
  return s === "completed" || s === "partial" || s === "not_delivered"
}

export type VisitRecoveryState = {
  active: DeliveryVisit | null
  latest: DeliveryVisit | null
  lastClosed: DeliveryVisit | null
  canResumeActive: boolean
  canViewClosedResult: boolean
  /** Activa abierta coexistiendo con cierre válido → revisión admin. */
  adminReviewPending: boolean
  requiresExplicitNewVisit: boolean
  /** visitId preferido para pantalla resultado sin query. */
  preferredResultVisitId: number | null
}

export function resolveVisitRecovery(input: {
  activeVisit?: DeliveryVisit | null
  latestVisit?: DeliveryVisit | null
  lastClosedVisit?: DeliveryVisit | null
}): VisitRecoveryState {
  const active =
    input.activeVisit && input.activeVisit.status === "in_progress"
      ? input.activeVisit
      : null
  const latest = input.latestVisit || active || input.lastClosedVisit || null
  let lastClosed = input.lastClosedVisit || null
  if (!lastClosed && latest && isClosed(latest.status) && latest.id !== active?.id) {
    lastClosed = latest
  }
  if (lastClosed && active && lastClosed.id === active.id) {
    lastClosed = null
  }

  const hasValidClose = Boolean(
    lastClosed && isValidClosedStatus(lastClosed.status),
  )
  const adminReviewPending = Boolean(active && hasValidClose)
  // Solo retomar si no hay cierre válido previo del mismo plan+cliente.
  const canResumeActive = Boolean(active) && !adminReviewPending

  return {
    active,
    latest,
    lastClosed,
    canResumeActive,
    canViewClosedResult: Boolean(lastClosed && isClosed(lastClosed.status)),
    adminReviewPending,
    requiresExplicitNewVisit: !active && !hasValidClose,
    preferredResultVisitId: lastClosed?.id ?? null,
  }
}
