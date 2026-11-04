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
  formatCommercialBoxesCell,
  parseCommuneSector,
  productLineLabel,
  visibleStopNumber,
} from "@/lib/picking-display"

export function toWarehouseCards(
  header: DispatchPlanPickingHeader,
  items: DispatchPlanPickingProductRow[],
): WarehousePickingCardData[] {
  // Misma lista consolidada y orden del picking producto impreso.
  return items.map((it) => {
    const units = Number(it.unidades) || 0
    return {
      productLabel: productLineLabel(it),
      barcode: it.codigo_barras,
      category: it.tipo_producto,
      branchLabel: it.sucursal_bodega || "Centro de despacho",
      requestedUnits: units,
      commercialPackLabel: formatCommercialBoxesCell(it),
      preparedUnits: null, // control físico — PWA futura
      loadedUnits: null,
      truckName: header.truck_name,
      planningCode: header.planning_number,
      sealLabel: header.sello || null,
    }
  })
}

export function toDriverDeliveryCards(
  clients: DispatchPlanPickingClientRow[],
): DriverDeliveryCardData[] {
  // Mismo orden de ruta que el PDF/Excel de picking cliente.
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
      sellerName: c.seller_name || null,
      observations: c.observations || c.delivery_notes,
      products: [], // detalle por tap — futuro; no en PDF principal
      deliveryStatus: "pending",
    }
  })
}
