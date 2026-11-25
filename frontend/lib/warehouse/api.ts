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

export type GeneratePreviewResult = {
  preview: boolean
  persists: boolean
  preset: string
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
