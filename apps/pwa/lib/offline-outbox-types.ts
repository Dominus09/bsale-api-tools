/** Tipos Offline V2 — outbox local (2A.2). Sin sync de escrituras todavía. */

export const OUTBOX_OP_TYPES = ["complete_delivery", "closed_store"] as const
export type OutboxOpType = (typeof OUTBOX_OP_TYPES)[number]

export const OUTBOX_LOCAL_STATUSES = [
  "draft",
  "pending",
  "syncing",
  "synced",
  "conflict",
  "failed_needs_action",
] as const
export type OutboxLocalStatus = (typeof OUTBOX_LOCAL_STATUSES)[number]

/** Estados visibles en 2A.2 (sin motor de envío). */
export const OUTBOX_ACTIVE_LOCAL_STATUSES: ReadonlyArray<OutboxLocalStatus> = [
  "draft",
  "pending",
  "failed_needs_action",
  "conflict",
]

export type OutboxGpsCapture = {
  lat: number | null
  lng: number | null
  accuracy_m: number | null
  captured_at: string | null
  source: "device" | "exception" | "deferred_offline"
  exception_code?: string | null
  exception_note?: string | null
}

export type OutboxLine = {
  source_detail_id: number
  variant_id?: number | null
  sku?: string | null
  description?: string | null
  planned_units: number
  delivered_units: number
}

export type OutboxDocument = {
  related_document_id: number | null
  oc_document_id?: number | null
  document_number?: number | null
  document_type?: string | null
  document_total?: number | null
  lines: OutboxLine[]
}

export type OutboxPayment = {
  method: string
  note?: string | null
}

export type OutboxEvidenceRef = {
  localBlobId: string
  localId: string
  content_sha256: string
  content_type: string
  size_bytes: number
  captured_at: string
  /** Nunca true en 2A.2 — no se sube a R2 aún. */
  uploaded_to_r2: false
}

export type OutboxOperation = {
  localUuid: string
  schemaVersion: 2
  idempotency_key: string
  userId: string
  planId: number
  customerKey: string
  clientId: number | null
  clientName: string | null
  op_type: OutboxOpType
  pickingId: number | null
  pickingVersion: string | null
  documents: OutboxDocument[]
  payment: OutboxPayment | null
  note: string | null
  captured_at: string
  gps: OutboxGpsCapture | null
  evidence: OutboxEvidenceRef | null
  status: OutboxLocalStatus
  attempt_count: number
  last_error: string | null
  created_at: string
  updated_at: string
  device_id: string | null
  /** true solo tras respuesta verificable del servidor. */
  server_confirmed: boolean
  /** true si esta op fue/está habilitada para sync writes. */
  sync_enabled: boolean
}

export function outboxStatusLabelEs(status: OutboxLocalStatus): string {
  switch (status) {
    case "draft":
      return "Borrador en este dispositivo"
    case "pending":
      return "Pendiente de sincronización"
    case "syncing":
      return "Sincronizando…"
    case "synced":
      return "Confirmado por servidor"
    case "conflict":
      return "Conflicto — requiere revisión"
    case "failed_needs_action":
      return "Error recuperable"
    default:
      return status
  }
}

export function outboxResultLabelEs(op: Pick<OutboxOperation, "op_type">): string {
  return op.op_type === "complete_delivery"
    ? "Entrega completa (local)"
    : "Local cerrado (local)"
}
