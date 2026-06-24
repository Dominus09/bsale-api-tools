import type { DistribuidoraDispatchPrepPlanningRow } from "@/lib/api"
import {
  resolvePurchaseStatusCode,
  type PurchaseInvoiceStatusFields,
} from "@/lib/purchase-invoice-status"
import {
  computeAmountThresholds,
  resolveRowPriority,
  type PreDespachoAmountThresholds,
} from "@/lib/pre-despacho-priority"

export type OperationalStatusKind =
  | "bsale_modified"
  | "no_geo"
  | "invoiced"
  | "probable"
  | "pending"
  | "recent"
  | "ok"

export type OperationalStatus = {
  kind: OperationalStatusKind
  emoji: string
  label: string
  detail: string
}

export function resolveOperationalStatus(
  row: DistribuidoraDispatchPrepPlanningRow,
  thresholds: PreDespachoAmountThresholds,
  hasGeo: boolean,
): OperationalStatus {
  const code = resolvePurchaseStatusCode(row as PurchaseInvoiceStatusFields)
  const priority = resolveRowPriority(row, thresholds)
  const parts: string[] = []

  if (row.bsale_updated_pending) {
    parts.push("Cambios en Bsale posteriores al último sync ERP")
  }
  if (!hasGeo) parts.push("Sin georreferencia")
  if (code === "FACTURADA_CONFIRMADA") parts.push("Facturada confirmada")
  else if (code.startsWith("PROBABLE")) parts.push("Coincidencia probable")
  else if (code === "PENDIENTE") parts.push("Pendiente de facturación")
  if (priority.flags.includes("recent")) parts.push("OC reciente en el lote")
  if (priority.flags.includes("high_amount")) parts.push("Monto alto en el lote")
  if (priority.flags.includes("stale_pending")) parts.push("Pendiente antigua")

  if (row.bsale_updated_pending) {
    return {
      kind: "bsale_modified",
      emoji: "🔴",
      label: "Modificada",
      detail: parts.join(" · "),
    }
  }
  if (!hasGeo) {
    return {
      kind: "no_geo",
      emoji: "🟠",
      label: "Sin geo",
      detail: parts.join(" · "),
    }
  }
  if (code === "FACTURADA_CONFIRMADA") {
    return {
      kind: "invoiced",
      emoji: "🟢",
      label: "Actualizada",
      detail: parts.join(" · "),
    }
  }
  if (code.startsWith("PROBABLE")) {
    return {
      kind: "probable",
      emoji: "🟡",
      label: "Probable",
      detail: parts.join(" · "),
    }
  }
  if (priority.flags.includes("recent")) {
    return {
      kind: "recent",
      emoji: "🔵",
      label: "Reciente",
      detail: parts.join(" · "),
    }
  }
  if (code === "PENDIENTE") {
    return {
      kind: "pending",
      emoji: "🟡",
      label: "Pendiente",
      detail: parts.join(" · "),
    }
  }
  return {
    kind: "ok",
    emoji: "🟢",
    label: "Actualizada",
    detail: parts.join(" · ") || "Datos sincronizados",
  }
}

export function operationalStatusBadgeClass(kind: OperationalStatusKind): string {
  switch (kind) {
    case "bsale_modified":
      return "border-red-300 bg-red-50 text-red-800 dark:border-red-900 dark:bg-red-950/40 dark:text-red-200"
    case "no_geo":
      return "border-amber-300 bg-amber-50 text-amber-900 dark:border-amber-800 dark:bg-amber-950/40 dark:text-amber-100"
    case "invoiced":
    case "ok":
      return "border-emerald-200 bg-emerald-50 text-emerald-800 dark:border-emerald-900 dark:bg-emerald-950/40 dark:text-emerald-200"
    case "probable":
    case "pending":
      return "border-amber-200 bg-amber-50 text-amber-900 dark:border-amber-800 dark:bg-amber-950/40 dark:text-amber-100"
    case "recent":
      return "border-sky-200 bg-sky-50 text-sky-800 dark:border-sky-800 dark:bg-sky-950/50 dark:text-sky-200"
  }
}

export { computeAmountThresholds }
