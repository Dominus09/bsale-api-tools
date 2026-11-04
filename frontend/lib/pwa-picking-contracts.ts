/**
 * Contratos de datos PWA (bodega / chofer) reutilizando picking de API.
 * Sin eventos operativos ni escritura.
 */

import type {
  DispatchPlanPickingClientRow,
  DispatchPlanPickingHeader,
  DispatchPlanPickingProductRow,
} from "@/lib/api"
import type { DriverDeliveryCardData } from "@/components/distribuidora/pwa/driver-delivery-card"
import type { WarehousePickingCardData } from "@/components/distribuidora/pwa/warehouse-picking-card"
import {
  parseCommuneSector,
  productLineLabel,
  visibleStopNumber,
} from "@/lib/picking-display"

export function toWarehouseCards(
  header: DispatchPlanPickingHeader,
  items: DispatchPlanPickingProductRow[],
): WarehousePickingCardData[] {
  return items.map((it) => {
    const upb = Number(it.units_per_box_efectivo ?? it.units_per_box) || 0
    const units = Number(it.unidades) || 0
    const pack =
      upb > 0
        ? `${units > 0 ? Math.round((units / upb) * 100) / 100 : "—"} cajas SEC${upb}`
        : null
    return {
      productLabel: productLineLabel(it),
      barcode: it.codigo_barras,
      category: it.tipo_producto,
      requestedUnits: units,
      commercialPackLabel: pack,
      preparedUnits: null,
      loadedUnits: null,
      truckName: header.truck_name,
      planningCode: header.planning_number,
    }
  })
}

export function toDriverDeliveryCards(
  clients: DispatchPlanPickingClientRow[],
): DriverDeliveryCardData[] {
  const minRo = clients.reduce((min, c) => {
    const ro = Number(c.route_order)
    return Number.isFinite(ro) ? Math.min(min, ro) : min
  }, 0)
  return clients.map((c) => {
    const geo = parseCommuneSector(c.city)
    return {
      stopNumber: visibleStopNumber(c.route_order, minRo),
      clientName: c.client_name || "—",
      fantasyName: c.fantasy_name,
      address: c.address,
      commune: geo.displayCommune || undefined,
      sector: geo.displaySector || undefined,
      phone: c.phone,
      documentNumber: c.document_number,
      documentType: c.document_type,
      documentTotalClp: c.document_total != null ? Number(c.document_total) : null,
      declaredPaymentMethod: c.payment_method,
      observations: c.observations || c.delivery_notes,
      products: [],
      deliveryStatus: "pending",
    }
  })
}
