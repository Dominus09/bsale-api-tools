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

// Konva draws literal colors, not CSS variables. Preserve the light palette
// and use legible equivalents in Graphite Dark.
const DARK_LAYOUT_COLORS = {
  ...LAYOUT_COLORS,
  facility_boundary: {fill:"rgba(244,245,247,0.02)",stroke:"#94a3b8"},
  sector: {fill:"rgba(75,108,205,0.08)",stroke:"#93b2ff"},
  position: {fill:"rgba(56,189,248,0.23)",stroke:"#7dd3fc"},
  loose_storage: {fill:"rgba(251,191,36,0.18)",stroke:"#fcd34d"},
  special_storage: {fill:"rgba(251,146,60,0.20)",stroke:"#fdba74"},
  aisle: {fill:"rgba(147,178,255,0.08)",stroke:"#a5b4fc"},
  circulation: {fill:"rgba(148,163,184,0.10)",stroke:"#94a3b8"},
  structure: {fill:"rgba(148,163,184,0.22)",stroke:"#cbd5e1"},
  office: {fill:"rgba(212,160,60,0.15)",stroke:"#fcd34d"},
  stair: {fill:"rgba(196,181,253,0.18)",stroke:"#c4b5fd"},
  gate: {fill:"rgba(52,211,153,0.20)",stroke:"#6ee7b7"},
  door: {fill:"rgba(52,211,153,0.16)",stroke:"#6ee7b7"},
  selection: {stroke:"#fdba74",strokeWidth:2.5},
  pending_validation: {dash:[6,4],badge:"#fbbf24"},
  extends_outside: {stroke:"#f9a8d4",dash:[2,3]},
  geom_conflict: {stroke:"#f87171",fill:"rgba(248,113,113,0.20)"},
  grid:"#34404f",background:"#101318",label:"#f4f5f7",labelMuted:"#aeb7c4",marker:"#f4f5f7",
}
export function layoutColorsForTheme(dark = false) {
  return dark ? DARK_LAYOUT_COLORS : LAYOUT_COLORS
}

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
  dark = false,
): { fill: string; stroke: string; dash?: number[] } {
  const palette = layoutColorsForTheme(dark)
  if (type === "facility_boundary") return palette.facility_boundary
  if (type === "sector") return palette.sector
  if (type === "loose_storage") return palette.loose_storage
  if (type === "position") {
    const kind = meta?.storage_kind || (meta as { storage_kind?: string } | undefined)
    const sk =
      (typeof meta?.storage_kind === "string" && meta.storage_kind) ||
      (typeof kind === "string" ? kind : null)
    if (sk === "special_restricted" || meta?.under_stair === true) {
      return { ...palette.special_storage, dash: [4, 3] }
    }
    return palette.position
  }
  if (type === "aisle") return palette.aisle
  if (type === "circulation") return palette.circulation
  if (type === "office") return palette.office
  if (type === "stair") return palette.stair
  if (type === "gate") return palette.gate
  if (type === "door") return palette.door
  if (STRUCTURE_TYPES.has(type)) return palette.structure
  return palette.structure
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
