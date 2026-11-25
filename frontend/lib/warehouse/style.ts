/** Colores de la vista Layout — deben coincidir con SYMBOLOGY_CATALOG.colors_layout_view */

import type { WarehouseElementType } from "@/lib/warehouse/types"

export const LAYOUT_COLORS = {
  facility_boundary: { fill: "rgba(15,23,42,0.04)", stroke: "#0f172a" },
  sector: { fill: "rgba(37,99,235,0.12)", stroke: "#2563eb" },
  position: { fill: "rgba(14,165,233,0.35)", stroke: "#0284c7" },
  loose_storage: { fill: "rgba(234,179,8,0.35)", stroke: "#a16207" },
  special_storage: { fill: "rgba(249,115,22,0.28)", stroke: "#c2410c" },
  aisle: { fill: "rgba(99,102,241,0.16)", stroke: "#4f46e5" },
  circulation: { fill: "rgba(148,163,184,0.22)", stroke: "#64748b" },
  structure: { fill: "rgba(100,116,139,0.35)", stroke: "#475569" },
  office: { fill: "rgba(245,158,11,0.25)", stroke: "#d97706" },
  stair: { fill: "rgba(168,85,247,0.22)", stroke: "#7c3aed" },
  gate: { fill: "rgba(34,197,94,0.35)", stroke: "#16a34a" },
  door: { fill: "rgba(34,197,94,0.28)", stroke: "#15803d" },
  selection: { stroke: "#ea580c", strokeWidth: 2.5 },
  pending_validation: { dash: [6, 4] as number[], badge: "#f59e0b" },
  extends_outside: { stroke: "#db2777", dash: [2, 3] as number[] },
  geom_conflict: { stroke: "#dc2626", fill: "rgba(220,38,38,0.18)" },
  grid: "#e2e8f0",
  background: "#f8fafc",
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
