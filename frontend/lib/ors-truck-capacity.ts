/** Estimación de carga por camión en planificación ORS (proxy hasta peso real en OC). */

/** Peso medio operativo por parada (kg) — proxy si no hay peso calculado. */
export const ORS_ESTIMATED_KG_PER_STOP = 450

export type OrderWeightLike = {
  weight_kg?: number | null
  peso_total_kg?: number | null
  weight?: {
    value_kg?: number | null
    status?: string | null
  } | null
}

export type AssignedKgEstimate = {
  assignedKg: number
  knownKg: number
  unavailableCount: number
  usedStopProxy: boolean
  incomplete: boolean
}

export function estimateAssignedKgFromStops(stopCount: number): number {
  return Math.max(0, stopCount) * ORS_ESTIMATED_KG_PER_STOP
}

function orderKnownKg(o: OrderWeightLike): number | null {
  const status = o.weight?.status
  if (status === "unavailable" || status === "error") return null
  const raw = o.weight?.value_kg ?? o.peso_total_kg ?? o.weight_kg
  if (raw == null) return null
  const w = typeof raw === "number" ? raw : Number(raw)
  if (!Number.isFinite(w)) return null
  return w
}

export function summarizeAssignedKgFromOrders(
  orders: OrderWeightLike[],
): AssignedKgEstimate {
  let knownKg = 0
  let unavailableCount = 0
  for (const o of orders) {
    const w = orderKnownKg(o)
    if (w == null) {
      unavailableCount += 1
      continue
    }
    if (w > 0) knownKg += w
  }
  if (knownKg > 0) {
    return {
      assignedKg: knownKg,
      knownKg,
      unavailableCount,
      usedStopProxy: false,
      incomplete: unavailableCount > 0,
    }
  }
  // Solo usa proxy por paradas si no hay ningún peso conocido.
  return {
    assignedKg: estimateAssignedKgFromStops(orders.length),
    knownKg: 0,
    unavailableCount,
    usedStopProxy: true,
    incomplete: unavailableCount > 0 || orders.length > 0,
  }
}

export function estimateAssignedKgFromOrders(orders: OrderWeightLike[]): number {
  return summarizeAssignedKgFromOrders(orders).assignedKg
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
