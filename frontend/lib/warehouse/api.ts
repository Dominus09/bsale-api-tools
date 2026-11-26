import { getApiBaseUrl } from "@/lib/api-base"
import { getAuthHeaders } from "@/lib/api"
import type {
  BatchSaveResult,
  ElementUpsertPayload,
  RevisionBundle,
  SymbologyCatalog,
  WarehouseFacility,
  WarehouseFloor,
  WarehouseRevision,
} from "@/lib/warehouse/types"
import { WarehouseApiError } from "@/lib/warehouse/types"

const API_URL = getApiBaseUrl()

async function warehouseFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_URL}${path}`, {
    ...init,
    headers: {
      ...getAuthHeaders(),
      ...(init?.headers || {}),
    },
  })
  if (!res.ok) {
    let message = `Error warehouse ${res.status}`
    let code: string | undefined
    try {
      const body = await res.json()
      const detail = body?.detail
      if (typeof detail === "string") message = detail
      else if (detail?.message) {
        message = detail.message
        code = detail.code
      } else if (body?.message) message = body.message
    } catch {
      /* ignore */
    }
    throw new WarehouseApiError(message, res.status, code)
  }
  if (res.status === 204) return undefined as T
  return (await res.json()) as T
}

export async function getWarehouseCoordinateMeta(): Promise<{
  convention: Record<string, string>
  symbology_catalog: SymbologyCatalog
  note_es?: string
}> {
  return warehouseFetch("/warehouse/meta/coordinate-convention")
}

export async function listWarehouseFacilities(): Promise<WarehouseFacility[]> {
  const data = await warehouseFetch<{ items: WarehouseFacility[] }>("/warehouse/facilities")
  return data.items || []
}

export async function listWarehouseFloors(facilityId: number): Promise<WarehouseFloor[]> {
  const data = await warehouseFetch<{ items: WarehouseFloor[] }>(
    `/warehouse/facilities/${facilityId}/floors`,
  )
  return data.items || []
}

export async function listWarehouseRevisions(floorId: number): Promise<WarehouseRevision[]> {
  const data = await warehouseFetch<{ items: WarehouseRevision[] }>(
    `/warehouse/floors/${floorId}/revisions`,
  )
  return data.items || []
}

export async function getOrCreateDraft(
  floorId: number,
  label?: string,
): Promise<{ created: boolean; revision: WarehouseRevision }> {
  return warehouseFetch(`/warehouse/floors/${floorId}/draft`, {
    method: "POST",
    body: JSON.stringify({ label: label ?? null }),
  })
}

export async function getWarehouseRevision(revisionId: number): Promise<RevisionBundle> {
  return warehouseFetch(`/warehouse/revisions/${revisionId}`)
}

export async function saveWarehouseBatch(
  revisionId: number,
  body: {
    expected_row_version: number
    upserts: ElementUpsertPayload[]
    retire_physical_ids?: number[]
    remove_geometry_physical_ids?: number[]
  },
): Promise<BatchSaveResult> {
  return warehouseFetch(`/warehouse/revisions/${revisionId}/elements/batch`, {
    method: "POST",
    body: JSON.stringify(body),
  })
}

export type GeneratePositionIssue = {
  severity: string
  code: string
  message: string
  generated_codes?: string[]
  element_code?: string
  other_code?: string
}

export type GeneratePreviewResult = {
  preview: boolean
  persists: boolean
  preset: string
  row_axis?: "x" | "y"
  can_incorporate?: boolean
  requires_extension_ack?: boolean
  position_issues?: GeneratePositionIssue[]
  blocking_issues?: GeneratePositionIssue[]
  position_count: number
  occupied_area_m2: number
  interior_gap_m: number | null
  remaining_transverse_m?: number
  extension_outside_parent_m: number
  positions: Array<{
    code: string
    name: string
    element_type: string
    parent_code?: string
    row_index: number
    slot_index: number
    geometry: Record<string, unknown>
  }>
  warnings: Array<{ severity: string; code: string; message: string }>
  obstacle_conflicts?: Array<Record<string, unknown>>
  circulation_warnings?: Array<Record<string, unknown>>
  validation?: { counts: Record<string, number>; issues: Array<Record<string, unknown>> }
  note_es?: string
}

export async function previewGeneratePositions(
  body: Record<string, unknown>,
): Promise<GeneratePreviewResult> {
  return warehouseFetch("/warehouse/meta/generate-positions/preview", {
    method: "POST",
    body: JSON.stringify(body),
  })
}

export async function validateWarehouseRevision(
  revisionId: number,
  elements?: unknown[],
): Promise<{
  issues: Array<Record<string, unknown>>
  counts: Record<string, number>
  note_es?: string
}> {
  return warehouseFetch(`/warehouse/revisions/${revisionId}/validate`, {
    method: "POST",
    body: JSON.stringify({ elements: elements ?? null }),
  })
}

export async function listWarehouseAudit(
  revisionId: number,
  limit = 50,
): Promise<Array<Record<string, unknown>>> {
  const data = await warehouseFetch<{ items: Array<Record<string, unknown>> }>(
    `/warehouse/revisions/${revisionId}/audit?limit=${limit}`,
  )
  return data.items || []
}

export type PublishEvaluation = {
  revision_id: number
  can_publish: boolean
  blockers: Array<Record<string, unknown>>
  warnings: Array<Record<string, unknown>>
  pending_validation: Array<Record<string, unknown>>
  counts: Record<string, number>
  note_es?: string
  safety_certification?: boolean
}

export async function evaluateWarehousePublish(
  revisionId: number,
  elements?: unknown[],
): Promise<PublishEvaluation> {
  return warehouseFetch(`/warehouse/revisions/${revisionId}/evaluate-publish`, {
    method: "POST",
    body: JSON.stringify({ elements: elements ?? null }),
  })
}

export async function publishWarehouseRevision(
  revisionId: number,
  body: { expected_row_version: number; label?: string | null },
): Promise<{
  revision: WarehouseRevision
  archived_previous: WarehouseRevision | null
  evaluation: PublishEvaluation
  note_es?: string
}> {
  return warehouseFetch(`/warehouse/revisions/${revisionId}/publish`, {
    method: "POST",
    body: JSON.stringify(body),
  })
}

export async function createDraftFromRevision(
  sourceRevisionId: number,
  label?: string,
): Promise<{
  created: boolean
  copied_elements: number
  source_revision_id: number
  revision: WarehouseRevision
}> {
  return warehouseFetch(`/warehouse/revisions/${sourceRevisionId}/create-draft`, {
    method: "POST",
    body: JSON.stringify({ label: label ?? null }),
  })
}


// Fase 2A — sólo levantamiento físico (no sincroniza stock Bsale).
export type PalletProduct = {
  variant_id: number; product_id: number; product_name: string; variant_name: string | null
  product_type: string | null; barcode: string | null; sku: string | null; units_per_box: number | null
}
export type PalletLocation = {
  id: number; code: string; name: string; validation_status: string
  stack_levels_max: number | null; constraints_json: Record<string, unknown>
  physical_meta_snap: Record<string, unknown>; meta_json: Record<string, unknown>
}
export type PalletLocations = {
  floor: { floor_id: number; floor_code: string; facility_id: number; facility_code: string; company_id: number }
  revision: { id: number; status: string; row_version: number } | null
  items: PalletLocation[]
  observed_slots: { position_id: number; observed_level: number; pallet_id?: number; pallet_code?: string }[]
}
export type PalletLineInput = {
  variant_id: number; quantity_status: "verified" | "unknown"; boxes: number | null
  loose_units: number | null; expected_units_per_box: number | null
  expiry_status: "dated" | "unknown" | "not_applicable"; expires_on: string | null; lot: string | null
}
export type ObservedLocationInput = {
  position_id: number | null; observed_level: number | null
  layout_revision_id: number | null; layout_row_version: number | null; observed_ack: boolean
}
export type PalletRegisterInput = ObservedLocationInput & {
  registration_key: string; floor_id: number; pallet_kind: "complete" | "partial" | "mixed"
  received_on: string | null; notes: string | null; lines: PalletLineInput[]
}
export type PalletLine = {
  id: number; variant_id: number; product_id: number; product_name_snap: string
  variant_name_snap: string | null; product_type_snap: string | null
  barcode_snap: string | null; sku_snap: string | null; units_per_box_snap: number | null
  quantity_status: "verified" | "unknown"; boxes: number | null; loose_units: number | null
  total_units: number | null; expiry_status: "dated" | "unknown" | "not_applicable"
  expires_on: string | null; lot: string | null
}
export type PalletEvent = { id: number; action: string; actor_label: string; created_at: string; detail_json: Record<string, unknown> }
export type PalletRecord = {
  id: number; code: string; floor_id: number; pallet_kind: "complete" | "partial" | "mixed"
  received_on: string | null; notes: string | null; position_id: number | null; position_code_snap: string | null
  observed_level: number | null; layout_revision_id: number | null; layout_row_version: number | null
  status: "active" | "voided"; row_version: number; created_at: string; created_by: string
  lines: PalletLine[]; events?: PalletEvent[]; location_in_current_layout?: boolean | null
  current_position_code?: string | null
}

export function listPalletLocations(floorId: number): Promise<PalletLocations> {
  return warehouseFetch(`/warehouse/pallets/locations?floor_id=${floorId}`)
}
export async function searchPalletProducts(floorId: number, q: string): Promise<PalletProduct[]> {
  const data = await warehouseFetch<{items: PalletProduct[]}>(`/warehouse/pallets/products?floor_id=${floorId}&q=${encodeURIComponent(q)}`)
  return data.items
}
export function listWarehousePallets(floorId: number, q = "", offset = 0): Promise<{ items: PalletRecord[]; has_more: boolean; offset: number }> {
  return warehouseFetch(`/warehouse/pallets?floor_id=${floorId}&q=${encodeURIComponent(q)}&offset=${offset}&limit=50`)
}
export function registerWarehousePallet(body: PalletRegisterInput): Promise<PalletRecord> {
  return warehouseFetch("/warehouse/pallets", {method: "POST", body: JSON.stringify(body)})
}
export function getWarehousePallet(id: number): Promise<PalletRecord> {
  return warehouseFetch(`/warehouse/pallets/${id}`)
}
export function observeWarehousePalletLocation(id: number, body: ObservedLocationInput & {expected_row_version: number; reason: string}): Promise<PalletRecord> {
  return warehouseFetch(`/warehouse/pallets/${id}/observe-location`, {method: "POST", body: JSON.stringify(body)})
}
export function voidWarehousePallet(id: number, body: {expected_row_version: number; reason: string}): Promise<PalletRecord> {
  return warehouseFetch(`/warehouse/pallets/${id}/void`, {method: "POST", body: JSON.stringify(body)})
}


export type PalletDetailsInput = {
  expected_row_version: number; reason: string; pallet_kind: PalletRegisterInput["pallet_kind"]
  received_on: string | null; notes: string | null
  lines: (PalletLineInput & {line_id: number | null})[]; remove_line_ids: number[]
}
export function updateWarehousePalletDetails(id: number, body: PalletDetailsInput): Promise<PalletRecord> {
  return warehouseFetch(`/warehouse/pallets/${id}/details`, {method:"PATCH",body:JSON.stringify(body)})
}
