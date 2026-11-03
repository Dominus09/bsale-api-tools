import { getApiBaseUrl } from "@/lib/api-base"
import { getAuthHeaders } from "@/lib/api"

const API_URL = getApiBaseUrl()

export type OcAuditStatus =
  | "ok"
  | "gap_pending"
  | "missing_in_pg"
  | "version_diff"
  | "cancelled"
  | "not_found_in_bsale"
  | "verification_error"

export type OcAuditFinding = {
  folio: number
  status: OcAuditStatus | string
  local_document_id?: number | null
  postgresql_source_document_id?: number | null
  bsale_source_document_id?: number | null
  emission_date?: string | null
  detail?: string | null
  error?: string | null
  beyond_local_max?: boolean
  recoverable?: boolean
  wrote?: boolean
  [key: string]: unknown
}

export type OcAuditReport = {
  status: string
  mode: string
  wrote: boolean
  investigate?: boolean
  company_id: number
  office_id: number
  from_folio?: number
  to_folio?: number
  local_max_folio?: number | null
  to_folio_beyond_local_max?: boolean
  gaps_beyond_local_max?: number
  gaps_detected?: number
  recoverable_count?: number
  counts?: Record<string, number>
  findings: OcAuditFinding[]
  sequence_probe?: {
    has_overlap?: boolean | null
    other_offices_with_shared_numbers?: Array<{ office_id: number; shared_folios: number }>
    note?: string
  }
  notes?: Record<string, string>
  [key: string]: unknown
}

export type OcAuditMeta = {
  company_id: number
  office_id: number
  document_type_id: number
  bounds: {
    min_folio: number | null
    max_folio: number | null
    positive_count?: number
  }
  suggested_range: {
    from_folio: number | null
    to_folio: number | null
    window?: number
    note?: string
    tail_guidance?: string
  }
  limits: {
    max_range_span: number
    max_gaps: number
    max_investigate_batch: number
  }
  writes_enabled: boolean
  recovery_enabled: boolean
}

async function parseError(res: Response): Promise<string> {
  try {
    const data = await res.json()
    if (typeof data?.detail === "string") return data.detail
    return JSON.stringify(data?.detail ?? data)
  } catch {
    return res.statusText || `HTTP ${res.status}`
  }
}

export async function fetchOcAuditMeta(params: {
  companyId: number
  officeId: number
  window?: number
}): Promise<OcAuditMeta> {
  const qs = new URLSearchParams({
    company_id: String(params.companyId),
    office_id: String(params.officeId),
  })
  if (params.window) qs.set("window", String(params.window))
  const res = await fetch(`${API_URL}/distribuidora/oc-audit/meta?${qs}`, {
    headers: getAuthHeaders(),
  })
  if (!res.ok) throw new Error(await parseError(res))
  return res.json()
}

export async function postOcAuditLocal(body: {
  company_id: number
  office_id: number
  from_folio: number
  to_folio: number
}): Promise<OcAuditReport> {
  const res = await fetch(`${API_URL}/distribuidora/oc-audit/local`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(body),
  })
  if (!res.ok) throw new Error(await parseError(res))
  return res.json()
}

export async function postOcAuditInvestigate(body: {
  company_id: number
  office_id: number
  folios: number[]
}): Promise<OcAuditReport> {
  const res = await fetch(`${API_URL}/distribuidora/oc-audit/investigate`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(body),
  })
  if (!res.ok) throw new Error(await parseError(res))
  return res.json()
}
