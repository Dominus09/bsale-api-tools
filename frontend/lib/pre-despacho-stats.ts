import type {
  DistribuidoraDispatchPrepMunicipalityRow,
  DistribuidoraDispatchPrepPlanningRow,
} from "@/lib/api"
import {
  matchesPurchaseStatusFilter,
  resolvePurchaseStatusCode,
  type PurchaseInvoiceStatusFilter,
  type PurchaseInvoiceStatusFields,
} from "@/lib/purchase-invoice-status"

export type PreDespachoOperationalStats = {
  totalOrders: number
  totalAmount: number
  pending: number
  invoiced: number
  probable: number
}

export function computePreDespachoStats(
  rows: DistribuidoraDispatchPrepPlanningRow[] | null | undefined,
): PreDespachoOperationalStats {
  const list = Array.isArray(rows) ? rows : []
  let totalAmount = 0
  let pending = 0
  let invoiced = 0
  let probable = 0

  for (const row of list) {
    const amt = Number(row.total_amount)
    if (Number.isFinite(amt)) totalAmount += amt
    const code = resolvePurchaseStatusCode(row as PurchaseInvoiceStatusFields)
    if (code === "FACTURADA_CONFIRMADA") invoiced += 1
    else if (
      code === "PROBABLE_FACTURADA_HIGH" ||
      code === "PROBABLE_FACTURADA_MEDIUM" ||
      code === "PROBABLE_FACTURADA_LOW"
    ) {
      probable += 1
    } else pending += 1
  }

  return {
    totalOrders: list.length,
    totalAmount,
    pending,
    invoiced,
    probable,
  }
}

export function filterPlanningRowsByStatus(
  rows: DistribuidoraDispatchPrepPlanningRow[] | null | undefined,
  filter: PurchaseInvoiceStatusFilter,
): DistribuidoraDispatchPrepPlanningRow[] {
  const list = Array.isArray(rows) ? rows : []
  if (filter === "all") return list
  return list.filter((r) =>
    matchesPurchaseStatusFilter(r as PurchaseInvoiceStatusFields, filter),
  )
}

function municipalityLabel(row: DistribuidoraDispatchPrepPlanningRow): string {
  const m = row.municipality?.trim()
  return m || "(Sin comuna)"
}

/** Resumen por comuna a partir de filas de planificación (respeta filtro de estado). */
export function aggregateDispatchPrepByMunicipality(
  rows: DistribuidoraDispatchPrepPlanningRow[] | null | undefined,
  filter: PurchaseInvoiceStatusFilter,
): DistribuidoraDispatchPrepMunicipalityRow[] {
  const filtered = filterPlanningRowsByStatus(rows ?? [], filter)
  const map = new Map<
    string,
    { clientes: Set<number>; pedidos: number; ventas: number }
  >()

  for (const r of filtered) {
    const mun = municipalityLabel(r)
    if (!map.has(mun)) {
      map.set(mun, { clientes: new Set(), pedidos: 0, ventas: 0 })
    }
    const bucket = map.get(mun)!
    const cid = r.client_id
    if (cid != null && Number.isFinite(Number(cid))) {
      bucket.clientes.add(Number(cid))
    }
    bucket.pedidos += 1
    const amt = Number(r.total_amount)
    if (Number.isFinite(amt)) bucket.ventas += amt
  }

  return [...map.entries()]
    .map(([municipality, b]) => ({
      municipality,
      clientes_unicos: b.clientes.size,
      pedidos: b.pedidos,
      total_ventas: b.ventas,
    }))
    .sort((a, b) => {
      const dv = b.total_ventas - a.total_ventas
      if (dv !== 0) return dv
      return a.municipality.localeCompare(b.municipality, "es")
    })
}

export function computeResumenKpis(
  rows: DistribuidoraDispatchPrepMunicipalityRow[] | null | undefined,
) {
  const list = Array.isArray(rows) ? rows : []
  let pedidos = 0
  let ventas = 0
  for (const r of list) {
    pedidos += Number(r.pedidos) || 0
    ventas += Number(r.total_ventas) || 0
  }
  return { comunas: list.length, pedidos, ventas }
}
