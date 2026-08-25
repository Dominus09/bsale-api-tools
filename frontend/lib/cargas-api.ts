import { getApiBaseUrl } from "@/lib/api-base"
import { getAuthHeaders } from "@/lib/api"

const API_URL = getApiBaseUrl()

export type LoadStatus =
  | "draft"
  | "pending"
  | "in_progress"
  | "completed"
  | "certified"
  | "cancelled"

export type LoadItemStatus = "pending" | "partial" | "complete" | "excess" | "issue"

export type LoadListRow = {
  id: number
  picking_number: string
  picking_date?: string | null
  destination?: string | null
  truck?: string | null
  seal?: string | null
  status: LoadStatus
  total_items: number
  total_requested_units: number
  items_complete?: number
  items_pending?: number
  open_issues?: number
  progress_pct?: number
  loading_started_at?: string | null
  created_at?: string
  source_type?: string
}

export type LoadItem = {
  id: number
  load_id: number
  product_name: string
  product_type?: string | null
  barcode?: string | null
  sec?: number | null
  requested_units: number
  certified_units: number
  remaining_units?: number
  requested_boxes?: number | null
  remaining_boxes?: number | null
  remaining_loose?: number | null
  source_boxes_value?: number | null
  status: LoadItemStatus
  branch?: string | null
  total_value?: number | null
}

export type LoadDetail = LoadListRow & {
  items: LoadItem[]
  summary: {
    total_items: number
    items_complete: number
    items_partial: number
    items_pending: number
    items_excess: number
    requested_units: number
    certified_units: number
    progress_pct: number
    open_issues?: number
  }
  recent_certified?: {
    product_name?: string
    barcode?: string | null
    created_at?: string
    units_after?: number
  }[]
  product_types?: string[]
  certified_by?: string | null
  certified_at?: string | null
  loading_finished_at?: string | null
}

export type ImportPreview = {
  source_type: "excel" | "pdf"
  original_filename: string
  format_name?: string | null
  picking_number?: string | null
  picking_date?: string | null
  destination?: string | null
  truck?: string | null
  seal?: string | null
  document_units_total?: number | null
  document_value_total?: number | null
  summed_units: number
  summed_value?: number | null
  total_items: number
  valid_count: number
  warning_count: number
  error_count: number
  can_import: boolean
  errors: string[]
  warnings: string[]
  lines: {
    product_name: string
    barcode?: string | null
    requested_units: number
    sec?: number | null
    product_type?: string | null
    severity: string
    messages: string[]
    total_value?: number | null
  }[]
}

async function parseError(res: Response): Promise<string> {
  const body = await res.json().catch(() => null)
  if (body && typeof body === "object" && typeof (body as { detail?: unknown }).detail === "string") {
    return (body as { detail: string }).detail
  }
  return `Error HTTP ${res.status}`
}

export async function listCargas(params?: {
  status?: string
  limit?: number
}): Promise<LoadListRow[]> {
  const qs = new URLSearchParams()
  if (params?.status) qs.set("status", params.status)
  if (params?.limit != null) qs.set("limit", String(params.limit))
  const res = await fetch(`${API_URL}/cargas?${qs}`, { headers: getAuthHeaders() })
  if (!res.ok) throw new Error(await parseError(res))
  return res.json()
}

export async function getCarga(loadId: number): Promise<LoadDetail> {
  const res = await fetch(`${API_URL}/cargas/${loadId}`, { headers: getAuthHeaders() })
  if (!res.ok) throw new Error(await parseError(res))
  return res.json()
}

export async function previewCargaImport(file: File): Promise<ImportPreview> {
  const form = new FormData()
  form.append("file", file)
  const token =
    typeof window !== "undefined" ? localStorage.getItem("token") : null
  const res = await fetch(`${API_URL}/cargas/import-preview`, {
    method: "POST",
    headers: token ? { Authorization: `Bearer ${token}` } : undefined,
    body: form,
  })
  if (!res.ok) throw new Error(await parseError(res))
  return res.json()
}

export async function confirmCargaImport(
  file: File,
  pickingNumber?: string,
): Promise<LoadDetail> {
  const form = new FormData()
  form.append("file", file)
  if (pickingNumber) form.append("picking_number", pickingNumber)
  const token =
    typeof window !== "undefined" ? localStorage.getItem("token") : null
  const res = await fetch(`${API_URL}/cargas/import`, {
    method: "POST",
    headers: token ? { Authorization: `Bearer ${token}` } : undefined,
    body: form,
  })
  if (!res.ok) throw new Error(await parseError(res))
  return res.json()
}

export async function searchCargaItems(
  loadId: number,
  params?: { q?: string; status?: string; product_type?: string },
): Promise<LoadItem[]> {
  const qs = new URLSearchParams()
  if (params?.q) qs.set("q", params.q)
  if (params?.status) qs.set("status", params.status)
  if (params?.product_type) qs.set("product_type", params.product_type)
  const res = await fetch(`${API_URL}/cargas/${loadId}/items/search?${qs}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) throw new Error(await parseError(res))
  return res.json()
}

export async function startCarga(loadId: number): Promise<LoadDetail> {
  const res = await fetch(`${API_URL}/cargas/${loadId}/start`, {
    method: "POST",
    headers: getAuthHeaders(),
  })
  if (!res.ok) throw new Error(await parseError(res))
  return res.json()
}

export async function addCargaUnits(
  loadId: number,
  itemId: number,
  body: {
    boxes?: number
    loose_units?: number
    allow_excess?: boolean
    notes?: string
    complete_remaining?: boolean
  },
): Promise<LoadDetail> {
  const res = await fetch(`${API_URL}/cargas/${loadId}/items/${itemId}/add`, {
    method: "POST",
    headers: { ...getAuthHeaders(), "Content-Type": "application/json" },
    body: JSON.stringify(body),
  })
  if (!res.ok) throw new Error(await parseError(res))
  return res.json()
}

export async function reportCargaIssue(
  loadId: number,
  itemId: number,
  body: { issue_type: string; description?: string },
): Promise<LoadDetail> {
  const res = await fetch(`${API_URL}/cargas/${loadId}/items/${itemId}/issue`, {
    method: "POST",
    headers: { ...getAuthHeaders(), "Content-Type": "application/json" },
    body: JSON.stringify(body),
  })
  if (!res.ok) throw new Error(await parseError(res))
  return res.json()
}

export async function certifyCarga(loadId: number): Promise<LoadDetail> {
  const res = await fetch(`${API_URL}/cargas/${loadId}/certify`, {
    method: "POST",
    headers: getAuthHeaders(),
  })
  if (!res.ok) throw new Error(await parseError(res))
  return res.json()
}
