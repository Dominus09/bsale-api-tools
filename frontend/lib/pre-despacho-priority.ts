import type { DistribuidoraDispatchPrepPlanningRow } from "@/lib/api"
import {
  resolvePurchaseStatusCode,
  type PurchaseInvoiceStatusFields,
} from "@/lib/purchase-invoice-status"

export type PreDespachoPriorityFlag = "recent" | "high_amount" | "stale_pending"

export type PreDespachoPriorityMeta = {
  flags: PreDespachoPriorityFlag[]
  primary: PreDespachoPriorityFlag | null
}

export type PreDespachoAmountThresholds = {
  highAmount: number
  recentOc: number
  stalePendingOc: number
}

function numericOc(row: DistribuidoraDispatchPrepPlanningRow): number {
  const n = Number(row.oc)
  return Number.isFinite(n) ? n : -Infinity
}

/** Umbrales relativos al lote visible (sin cambiar reglas de negocio). */
export function computeAmountThresholds(
  rows: DistribuidoraDispatchPrepPlanningRow[],
): PreDespachoAmountThresholds {
  const amounts = rows
    .map((r) => Number(r.total_amount))
    .filter((n) => Number.isFinite(n) && n > 0)
    .sort((a, b) => a - b)
  const ocs = rows.map(numericOc).filter((n) => Number.isFinite(n) && n > -Infinity)

  const pct = (arr: number[], p: number) => {
    if (arr.length === 0) return 0
    const ix = Math.min(arr.length - 1, Math.floor((arr.length - 1) * p))
    return arr[ix] ?? 0
  }

  return {
    highAmount: pct(amounts, 0.75),
    recentOc: pct(ocs, 0.75),
    stalePendingOc: pct(ocs, 0.25),
  }
}

export function resolveRowPriority(
  row: DistribuidoraDispatchPrepPlanningRow,
  thresholds: PreDespachoAmountThresholds,
): PreDespachoPriorityMeta {
  const flags: PreDespachoPriorityFlag[] = []
  const amount = Number(row.total_amount)
  const oc = numericOc(row)
  const code = resolvePurchaseStatusCode(row as PurchaseInvoiceStatusFields)

  if (Number.isFinite(amount) && amount >= thresholds.highAmount && thresholds.highAmount > 0) {
    flags.push("high_amount")
  }
  if (oc >= thresholds.recentOc && thresholds.recentOc > -Infinity) {
    flags.push("recent")
  }
  if (
    code === "PENDIENTE" &&
    oc <= thresholds.stalePendingOc &&
    thresholds.stalePendingOc > -Infinity &&
    oc > -Infinity
  ) {
    flags.push("stale_pending")
  }

  const primary =
    flags.find((f) => f === "stale_pending") ??
    flags.find((f) => f === "high_amount") ??
    flags.find((f) => f === "recent") ??
    null

  return { flags, primary }
}

export function priorityLabel(flag: PreDespachoPriorityFlag): string {
  switch (flag) {
    case "recent":
      return "Reciente"
    case "high_amount":
      return "Monto alto"
    case "stale_pending":
      return "Pend. antigua"
  }
}

export function priorityBadgeClass(flag: PreDespachoPriorityFlag): string {
  switch (flag) {
    case "recent":
      return "border-sky-200 bg-sky-50 text-sky-800 dark:border-sky-800 dark:bg-sky-950/50 dark:text-sky-200"
    case "high_amount":
      return "border-amber-200 bg-amber-50 text-amber-900 dark:border-amber-800 dark:bg-amber-950/40 dark:text-amber-100"
    case "stale_pending":
      return "border-rose-200 bg-rose-50 text-rose-800 dark:border-rose-800 dark:bg-rose-950/40 dark:text-rose-200"
  }
}
