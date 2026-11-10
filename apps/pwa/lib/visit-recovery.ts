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

export type VisitRecoveryState = {
  active: DeliveryVisit | null
  latest: DeliveryVisit | null
  lastClosed: DeliveryVisit | null
  canResumeActive: boolean
  canViewClosedResult: boolean
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

  return {
    active,
    latest,
    lastClosed,
    canResumeActive: Boolean(active),
    canViewClosedResult: Boolean(lastClosed && isClosed(lastClosed.status)),
    requiresExplicitNewVisit: !active,
    preferredResultVisitId: lastClosed?.id ?? null,
  }
}
