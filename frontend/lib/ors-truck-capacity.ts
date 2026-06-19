/** Estimación de carga por camión en planificación ORS (proxy hasta peso real en OC). */

/** Peso medio operativo por parada (kg) — proxy configurable. */
export const ORS_ESTIMATED_KG_PER_STOP = 450

export function estimateAssignedKgFromStops(stopCount: number): number {
  return Math.max(0, stopCount) * ORS_ESTIMATED_KG_PER_STOP
}

export function truckUtilizationPct(
  assignedKg: number,
  maxWeightKg: number | null | undefined,
): number | null {
  const cap = Number(maxWeightKg)
  if (!Number.isFinite(cap) || cap <= 0) return null
  return Math.min(999, Math.round((assignedKg / cap) * 100))
}

export function isTruckOverloaded(
  assignedKg: number,
  maxWeightKg: number | null | undefined,
): boolean {
  const cap = Number(maxWeightKg)
  if (!Number.isFinite(cap) || cap <= 0) return false
  return assignedKg > cap
}
