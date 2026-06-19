import type { PlanificacionStoredOrder } from "@/lib/planificacion-despacho-storage"

export type RouteOperationalCosts = {
  ferry_clp: number
  per_diem_clp: number
  other_clp: number
  diesel_clp_per_liter: number
}

export const DEFAULT_DIESEL_CLP_PER_LITER = 1500

export const EMPTY_OPERATIONAL_COSTS: RouteOperationalCosts = {
  ferry_clp: 0,
  per_diem_clp: 0,
  other_clp: 0,
  diesel_clp_per_liter: DEFAULT_DIESEL_CLP_PER_LITER,
}

export function computeRouteSales(orders: PlanificacionStoredOrder[]): number {
  return orders.reduce((sum, o) => sum + (Number(o.total_amount) || 0), 0)
}

export function computeOperationalCostClp(
  fuelCostClp: number,
  costs: RouteOperationalCosts,
): number {
  return (
    Math.round(fuelCostClp) +
    Math.round(costs.ferry_clp) +
    Math.round(costs.per_diem_clp) +
    Math.round(costs.other_clp)
  )
}
