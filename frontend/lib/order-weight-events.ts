import type { DistribuidoraDispatchPrepPlanningRow } from "@/lib/api"

export const ORDER_WEIGHT_UPDATED = "ORDER_WEIGHT_UPDATED" as const

export type OrderWeightUpdatedDetail = {
  documentId: number
  patch: Partial<DistribuidoraDispatchPrepPlanningRow>
}

export function emitOrderWeightUpdated(
  documentId: number,
  patch: Partial<DistribuidoraDispatchPrepPlanningRow>,
): void {
  if (typeof window === "undefined") return
  window.dispatchEvent(
    new CustomEvent<OrderWeightUpdatedDetail>(ORDER_WEIGHT_UPDATED, {
      detail: { documentId, patch },
    }),
  )
}

export function subscribeOrderWeightUpdated(
  handler: (detail: OrderWeightUpdatedDetail) => void,
): () => void {
  if (typeof window === "undefined") return () => {}
  const listener = (event: Event) => {
    const custom = event as CustomEvent<OrderWeightUpdatedDetail>
    if (custom.detail) handler(custom.detail)
  }
  window.addEventListener(ORDER_WEIGHT_UPDATED, listener)
  return () => window.removeEventListener(ORDER_WEIGHT_UPDATED, listener)
}
