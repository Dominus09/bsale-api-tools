/** Tipos Inteligencia de Bodega — layout físico (slice 1.2). */

export type WarehouseElementType =
  | "facility_boundary"
  | "sector"
  | "aisle"
  | "position"
  | "loose_storage"
  | "wall"
  | "pillar"
  | "door"
  | "gate"
  | "stair"
  | "office"
  | "circulation"
  | "obstacle"
  | "unusable"
  | "beam"
  | "other"

export type ValidationStatus = "draft" | "pending_validation" | "validated"
export type RevisionStatus = "draft" | "published" | "archived"

export type WarehouseFacility = {
  id: number
  code: string
  name: string
  company_id: number
  bsale_office_id: number | null
  status: string
  meta_json: Record<string, unknown>
}

export type WarehouseFloor = {
  id: number
  facility_id: number
  code: string
  name: string
  level_index: number
  length_m: number | null
  width_m: number | null
  status: string
  meta_json: Record<string, unknown>
}

export type WarehouseRevision = {
  id: number
  floor_id: number
  status: RevisionStatus
  label: string | null
  row_version: number
  notes: string | null
  published_at: string | null
  published_by: string | null
  archived_at?: string | null
  archived_by?: string | null
  source_revision_id?: number | null
  publish_meta_json?: Record<string, unknown>
}

export type ElementGeometry = {
  layout_element_id?: number | null
  revision_id?: number | null
  x_m: number
  y_m: number
  elevation_m: number
  width_m: number
  depth_m: number
  height_clear_m: number | null
  rotation_deg: number
  geom_type: string
  polygon_m: unknown
  stack_levels_max: number | null
  validation_status: ValidationStatus
  constraints_json: Record<string, unknown>
  observations: string | null
  meta_json: Record<string, unknown>
}

export type ElementPhysical = {
  id: number
  facility_id: number
  floor_id: number
  parent_id: number | null
  element_type: WarehouseElementType
  code: string
  name: string
  status: string
  meta_json: Record<string, unknown>
  identity_source?: string
}

export type WarehouseElement = {
  physical: ElementPhysical
  geometry: ElementGeometry
  /** Cliente: id temporal negativo hasta guardar */
  clientKey: string
  dirty?: boolean
  pendingRetire?: boolean
  pendingRemoveGeometry?: boolean
  isNew?: boolean
}

export type RevisionBundle = {
  revision: WarehouseRevision
  floor: WarehouseFloor | null
  facility: WarehouseFacility | null
  coordinate_convention: Record<string, string>
  elements: Array<{ physical: ElementPhysical; geometry: ElementGeometry }>
  element_count: number
}

export type SymbologyCatalog = {
  nomenclature: Record<string, string>
  nomenclature_examples?: Array<{
    code: string
    parts: Array<{ token: string; meaning: string }>
  }>
  symbols: Array<{ type: string; label_es: string; shape: string }>
  colors_layout_view: Record<
    string,
    {
      fill?: string
      stroke?: string
      strokeWidth?: number
      dash?: number[]
      badge?: string
      label_es: string
    }
  >
  colors_future_views?: string[]
  colors_note?: string
}

export type EditorTool =
  | "select"
  | "create_sector"
  | "create_position"
  | "create_structure"
  | "move"
  | "rotate"
  | "resize"
  | "duplicate"
  | "delete"
  | "measure"
  | "pan"

export type ElementUpsertPayload = {
  physical_element_id?: number | null
  parent_id?: number | null
  element_type: WarehouseElementType
  code: string
  name: string
  meta_json?: Record<string, unknown>
  geometry: {
    x_m: number
    y_m: number
    elevation_m?: number
    width_m: number
    depth_m: number
    height_clear_m?: number | null
    rotation_deg?: number
    geom_type?: string
    validation_status?: ValidationStatus
    constraints_json?: Record<string, unknown>
    observations?: string | null
    meta_json?: Record<string, unknown>
    stack_levels_max?: number | null
  }
  observations?: string | null
}

export type BatchSaveResult = {
  revision: WarehouseRevision
  upserted: Array<{ physical: ElementPhysical; geometry: ElementGeometry }>
  retired_physical_ids: number[]
  removed_geometry_physical_ids: number[]
  note_es?: string
}

export class WarehouseApiError extends Error {
  status: number
  code?: string
  constructor(message: string, status: number, code?: string) {
    super(message)
    this.name = "WarehouseApiError"
    this.status = status
    this.code = code
  }
}
