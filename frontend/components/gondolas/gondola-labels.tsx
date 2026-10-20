import { StatusBadge, type AnalyticTone } from "@/components/analytics-v2/shared/analytics-ui"
import type { ReplenishmentStatus, ScanResult } from "@/lib/gondolas/types"

export const MOVEMENT_LABELS: Record<string, string> = {
  factura: "Factura",
  guia_despacho: "Guía de despacho",
  nota_credito: "Nota de crédito",
  transferencia_interna: "Transferencia interna",
  ajuste: "Ajuste de inventario",
  sin_documento: "Sin documento",
  no_determinado: "Movimiento no determinado",
}

export const SCAN_RESULT_LABELS: Record<ScanResult, string> = {
  encontrado: "Encontrado en góndola",
  duplicado: "Ya estaba registrado",
  ambiguo: "Código con varias variantes: elija cuál es",
  sin_coincidencia: "Código no reconocido en esta empresa",
  fuera_de_tipo: "Es de otro tipo de producto",
  stock_cero: "Sin stock registrado en esta sucursal",
  fuera_de_universo: "No pertenece al universo inicial del recorrido",
}

export const SCAN_RESULT_TONES: Record<ScanResult, AnalyticTone> = {
  encontrado: "success",
  duplicado: "info",
  ambiguo: "warning",
  sin_coincidencia: "danger",
  fuera_de_tipo: "warning",
  stock_cero: "warning",
  fuera_de_universo: "warning",
}

export const REPLENISHMENT_LABELS: Record<ReplenishmentStatus, string> = {
  pendiente_buscar: "Pendiente de buscar",
  encontrado_bodega: "Encontrado en bodega",
  repuesto_gondola: "Repuesto en góndola",
  no_encontrado: "No encontrado físicamente",
}

export const REPLENISHMENT_TONES: Record<ReplenishmentStatus, AnalyticTone> = {
  pendiente_buscar: "warning",
  encontrado_bodega: "info",
  repuesto_gondola: "success",
  no_encontrado: "danger",
}

export function movementLabel(value: string | null | undefined): string {
  if (!value) return "Sin ingreso registrado"
  return MOVEMENT_LABELS[value] ?? "Movimiento no determinado"
}

export function formatQty(value: number | null | undefined): string {
  if (value === null || value === undefined) return "—"
  return new Intl.NumberFormat("es-CL", { maximumFractionDigits: 3 }).format(value)
}

export function formatDate(value: string | null | undefined): string {
  if (!value) return "—"
  const date = new Date(value.length <= 10 ? `${value}T12:00:00` : value)
  if (Number.isNaN(date.getTime())) return value
  return new Intl.DateTimeFormat("es-CL", { day: "2-digit", month: "short", year: "numeric" }).format(date)
}

export function formatDateTime(value: string | null | undefined): string {
  if (!value) return "—"
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return new Intl.DateTimeFormat("es-CL", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date)
}

export function ExhibitionBadge({ status }: { status: "pendiente" | "encontrado" | null }) {
  if (status === "encontrado") return <StatusBadge tone="success">Encontrado</StatusBadge>
  if (status === "pendiente") return <StatusBadge tone="warning">Pendiente</StatusBadge>
  return <StatusBadge tone="neutral">Sin recorrido</StatusBadge>
}
