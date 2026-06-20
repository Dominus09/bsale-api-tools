import type { DispatchPlanStatus } from "@/lib/api"

export type DispatchPlanOperationalPhase =
  | "abierto"
  | "pickings_generados"
  | "cerrado"
  | "despachado"
  | "cuadrado"

const OPEN_STATUSES: DispatchPlanStatus[] = [
  "draft",
  "planned",
  "invoicing",
  "ready_for_picking",
]

export function dispatchPlanOperationalPhase(
  status: DispatchPlanStatus | string | null | undefined,
): DispatchPlanOperationalPhase {
  const st = String(status || "").toLowerCase()
  if (st === "picking_generated") return "pickings_generados"
  if (st === "closed") return "cerrado"
  if (st === "dispatched" || st === "delivered") return "despachado"
  if (st === "squared") return "cuadrado"
  if (OPEN_STATUSES.includes(st as DispatchPlanStatus)) return "abierto"
  return "abierto"
}

export function dispatchPlanOperationalLabel(
  status: DispatchPlanStatus | string | null | undefined,
): string {
  const phase = dispatchPlanOperationalPhase(status)
  const labels: Record<DispatchPlanOperationalPhase, string> = {
    abierto: "Abierto",
    pickings_generados: "Pickings generados",
    cerrado: "Cerrado",
    despachado: "Despachado",
    cuadrado: "Cuadrado",
  }
  return labels[phase]
}

export function dispatchPlanVersionLabel(
  planningCode: string | null | undefined,
  planId: number,
  pickingVersion: number | null | undefined,
): string {
  const base = (planningCode || `PLAN-${planId}`).trim()
  if (!pickingVersion) return base
  return `${base} v${pickingVersion}`
}
