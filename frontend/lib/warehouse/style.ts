/** Colores de la vista Layout — deben coincidir con SYMBOLOGY_CATALOG.colors_layout_view */

import type { WarehouseElementType } from "@/lib/warehouse/types"

/** Saturación reducida para sectores grandes; estructuras/posiciones más definidas. */
export const LAYOUT_COLORS = {
  facility_boundary: { fill: "rgba(15,23,42,0.02)", stroke: "#334155" },
  sector: { fill: "rgba(37,99,235,0.06)", stroke: "#3b82f6" },
  position: { fill: "rgba(14,165,233,0.22)", stroke: "#0284c7" },
  loose_storage: { fill: "rgba(202,138,4,0.22)", stroke: "#92400e" },
  special_storage: { fill: "rgba(234,88,12,0.20)", stroke: "#c2410c" },
  aisle: { fill: "rgba(99,102,241,0.10)", stroke: "#6366f1" },
  circulation: { fill: "rgba(148,163,184,0.12)", stroke: "#64748b" },
  structure: { fill: "rgba(100,116,139,0.28)", stroke: "#475569" },
  office: { fill: "rgba(217,119,6,0.16)", stroke: "#b45309" },
  stair: { fill: "rgba(109,40,217,0.28)", stroke: "#5b21b6" },
  gate: { fill: "rgba(22,163,74,0.28)", stroke: "#15803d" },
  door: { fill: "rgba(22,163,74,0.20)", stroke: "#166534" },
  selection: { stroke: "#ea580c", strokeWidth: 2.5 },
  pending_validation: { dash: [6, 4] as number[], badge: "#f59e0b" },
  extends_outside: { stroke: "#db2777", dash: [2, 3] as number[] },
  geom_conflict: { stroke: "#dc2626", fill: "rgba(220,38,38,0.14)" },
  grid: "#e2e8f0",
  background: "#f1f5f9",
  label: "#0f172a",
  labelMuted: "#475569",
  marker: "#1e293b",
} as const

const STRUCTURE_TYPES = new Set<WarehouseElementType>([
  "wall",
  "pillar",
  "obstacle",
  "beam",
  "unusable",
  "other",
])

export function colorsForType(
  type: WarehouseElementType,
  meta?: Record<string, unknown>,
): { fill: string; stroke: string; dash?: number[] } {
  if (type === "facility_boundary") return LAYOUT_COLORS.facility_boundary
  if (type === "sector") return LAYOUT_COLORS.sector
  if (type === "loose_storage") return LAYOUT_COLORS.loose_storage
  if (type === "position") {
    const kind = meta?.storage_kind || (meta as { storage_kind?: string } | undefined)
    const sk =
      (typeof meta?.storage_kind === "string" && meta.storage_kind) ||
      (typeof kind === "string" ? kind : null)
    if (sk === "special_restricted" || meta?.under_stair === true) {
      return { ...LAYOUT_COLORS.special_storage, dash: [4, 3] }
    }
    return LAYOUT_COLORS.position
  }
  if (type === "aisle") return LAYOUT_COLORS.aisle
  if (type === "circulation") return LAYOUT_COLORS.circulation
  if (type === "office") return LAYOUT_COLORS.office
  if (type === "stair") return LAYOUT_COLORS.stair
  if (type === "gate") return LAYOUT_COLORS.gate
  if (type === "door") return LAYOUT_COLORS.door
  if (STRUCTURE_TYPES.has(type)) return LAYOUT_COLORS.structure
  return LAYOUT_COLORS.structure
}

/** Prioridad de etiqueta: evita que sectores grandes tapen códigos de pasillos/estructuras. */
export function labelPriority(type: WarehouseElementType): number {
  if (type === "facility_boundary") return 0
  if (type === "sector" || type === "circulation") return 1
  if (type === "aisle") return 2
  if (type === "office" || type === "gate" || type === "door") return 3
  if (type === "loose_storage") return 4
  if (type === "stair") return 5
  if (type === "position") return 6
  return 3
}

export function isUnderStairMeta(meta?: Record<string, unknown>): boolean {
  if (!meta) return false
  return meta.under_stair === true || meta.storage_kind === "special_restricted"
}

export function shouldShowDimensions(
  type: WarehouseElementType,
  screenW: number,
  screenH: number,
): boolean {
  if (type === "facility_boundary") return false
  if (type === "sector" || type === "circulation") {
    return screenW > 56 && screenH > 40
  }
  if (type === "aisle") return screenW > 36 && screenH > 48
  return screenW > 44 && screenH > 32
}

export const ELEMENT_TYPE_LABELS_ES: Record<WarehouseElementType, string> = {
  facility_boundary: "Límite",
  sector: "Sector",
  aisle: "Pasillo",
  position: "Posición (pallet)",
  loose_storage: "Almacenamiento sin pallet",
  wall: "Muro",
  pillar: "Pilar",
  door: "Puerta",
  gate: "Portón",
  stair: "Escalera",
  office: "Oficina",
  circulation: "Circulación",
  obstacle: "Obstáculo",
  unusable: "No utilizable",
  beam: "Viga",
  other: "Otro",
}

export const STANDARD_PALLET_M = { length: 1.26, width: 1.06 }

export const GENERATOR_PRESETS = ["4+4", "4+5", "5+5", "5+6", "6+6", "custom"] as const
