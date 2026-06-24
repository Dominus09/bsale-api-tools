/** Estimación de carga por camión en planificación ORS (proxy hasta peso real en OC). */

/** Peso medio operativo por parada (kg) — proxy si no hay peso calculado. */
export const ORS_ESTIMATED_KG_PER_STOP = 450

export function estimateAssignedKgFromStops(stopCount: number): number {
  return Math.max(0, stopCount) * ORS_ESTIMATED_KG_PER_STOP
}

export function estimateAssignedKgFromOrders(
  orders: Array<{ weight_kg?: number | null }>,
): number {
  const sum = orders.reduce((acc, o) => {
    const w = Number(o.weight_kg)
    return acc + (Number.isFinite(w) && w > 0 ? w : 0)
  }, 0)
  if (sum > 0) return sum
  return estimateAssignedKgFromStops(orders.length)
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
