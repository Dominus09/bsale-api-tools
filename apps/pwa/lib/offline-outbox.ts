/**
 * Offline V2 2A.2 — API de outbox local.
 * Guarda operaciones; NO envía escrituras al servidor.
 */

import { isOutboxSyncWritesEnabled } from "./offline-outbox-policy.ts"
import {
  getOutboxOp,
  listOutboxOpsForPlanCustomer,
  listOutboxOpsForUser,
  putOutboxBlob,
  putOutboxOp,
  verifyOutboxBlobPersisted,
} from "./offline-outbox-store.ts"
import {
  OUTBOX_ACTIVE_LOCAL_STATUSES,
  type OutboxDocument,
  type OutboxGpsCapture,
  type OutboxLocalStatus,
  type OutboxOperation,
  type OutboxOpType,
  type OutboxPayment,
} from "./offline-outbox-types.ts"
import { detailLooksComplete } from "./offline-store.ts"

export {
  OFFLINE_OUTBOX_RETENTION,
  OFFLINE_V2_PUBLICATION_BLOCKERS,
  isOutboxSyncWritesEnabled,
} from "./offline-outbox-policy.ts"
export {
  outboxResultLabelEs,
  outboxStatusLabelEs,
  type OutboxLocalStatus,
  type OutboxOperation,
  type OutboxOpType,
} from "./offline-outbox-types.ts"
export {
  resetOutboxMemoryStoreForTests,
  useOutboxMemoryStoreForTests,
} from "./offline-outbox-store.ts"

function nowIso(): string {
  return new Date().toISOString()
}

export function newLocalUuid(): string {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID()
  }
  return `local-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`
}

/**
 * Clave estable por usuario/plan/cliente/tipo.
 * NO regenerar en reintentos del mismo registro.
 */
export function stableIdempotencyKey(input: {
  userId: string
  planId: number
  customerKey: string
  opType: OutboxOpType
  localUuid: string
}): string {
  const safeCust = encodeURIComponent(input.customerKey).slice(0, 80)
  return `v2:${input.userId}:${input.planId}:${safeCust}:${input.opType}:${input.localUuid}`
}

export async function sha256Hex(data: ArrayBuffer): Promise<string> {
  if (typeof crypto !== "undefined" && crypto.subtle) {
    const dig = await crypto.subtle.digest("SHA-256", data)
    return [...new Uint8Array(dig)].map((b) => b.toString(16).padStart(2, "0")).join("")
  }
  // Fallback no criptográfico solo para entornos de test sin subtle.
  const bytes = new Uint8Array(data)
  let h = 0
  for (let i = 0; i < bytes.length; i++) h = (h * 31 + bytes[i]!) >>> 0
  return `fallback-${h.toString(16)}-${bytes.length}`
}

export function isActiveOutboxStatus(status: OutboxLocalStatus): boolean {
  return (OUTBOX_ACTIVE_LOCAL_STATUSES as readonly string[]).includes(status)
}

export async function countPendingOutboxOps(userId: string): Promise<number> {
  const ops = await listOutboxOpsForUser(String(userId))
  return ops.filter((o) => isActiveOutboxStatus(o.status)).length
}

export async function listPendingOutboxOps(
  userId: string,
): Promise<OutboxOperation[]> {
  const ops = await listOutboxOpsForUser(String(userId))
  return ops.filter((o) => isActiveOutboxStatus(o.status))
}

export async function findActiveOutboxForCustomer(
  userId: string,
  planId: number,
  customerKey: string,
): Promise<OutboxOperation | null> {
  const rows = await listOutboxOpsForPlanCustomer(
    String(userId),
    Number(planId),
    customerKey,
  )
  return rows.find((o) => isActiveOutboxStatus(o.status)) ?? null
}

export type SnapshotPickingMeta = {
  pickingId: number | null
  pickingVersion: string | null
}

export function extractPickingFromReady(ready: {
  pickingId?: number | null
  pickingVersion?: string | null
  fingerprint?: { pickingId?: number | null; pickingVersion?: string | null } | null
} | null): SnapshotPickingMeta {
  if (!ready) return { pickingId: null, pickingVersion: null }
  const pid =
    ready.pickingId ?? ready.fingerprint?.pickingId ?? null
  const pver =
    ready.pickingVersion ??
    (ready.fingerprint?.pickingVersion != null
      ? String(ready.fingerprint.pickingVersion)
      : null)
  return {
    pickingId: pid != null && Number.isFinite(Number(pid)) ? Number(pid) : null,
    pickingVersion: pver != null && String(pver).trim() ? String(pver) : null,
  }
}

export function buildDocumentsFromFichaPayload(
  payload: unknown,
): { ok: true; documents: OutboxDocument[] } | { ok: false; reason: string } {
  if (!detailLooksComplete(payload)) {
    return {
      ok: false,
      reason:
        "Snapshot incompleto o sin productos/cantidades válidas. No se puede confirmar localmente.",
    }
  }
  const p = payload as {
    client?: { client_id?: number | null }
    documents?: Array<{
      related_document_id?: number | null
      oc_document_id?: number | null
      document_number?: number | null
      document_type?: string | null
      document_total?: number | null
      products?: Array<{
        source_detail_id?: number | null
        variant_id?: number | null
        sku?: string | null
        variant_code?: string | null
        description?: string | null
        product_name?: string | null
        unidades?: number | null
      }>
    }>
  }
  const documents: OutboxDocument[] = []
  for (const doc of p.documents || []) {
    const lines = []
    for (const prod of doc.products || []) {
      const sid = prod.source_detail_id
      if (sid == null || !Number.isFinite(Number(sid))) {
        return { ok: false, reason: "Producto sin source_detail_id confiable" }
      }
      const planned = Number(prod.unidades)
      if (!Number.isFinite(planned) || planned < 0) {
        return { ok: false, reason: "Cantidad planificada inválida" }
      }
      lines.push({
        source_detail_id: Number(sid),
        variant_id: prod.variant_id != null ? Number(prod.variant_id) : null,
        sku: prod.sku || prod.variant_code || null,
        description: prod.description || prod.product_name || null,
        planned_units: planned,
        delivered_units: planned,
      })
    }
    if (lines.length === 0) {
      return { ok: false, reason: "Documento sin líneas planificadas" }
    }
    documents.push({
      related_document_id:
        doc.related_document_id != null ? Number(doc.related_document_id) : null,
      oc_document_id:
        doc.oc_document_id != null ? Number(doc.oc_document_id) : null,
      document_number:
        doc.document_number != null ? Number(doc.document_number) : null,
      document_type: doc.document_type || null,
      document_total:
        typeof doc.document_total === "number" ? doc.document_total : null,
      lines,
    })
  }
  if (documents.length === 0) {
    return { ok: false, reason: "Sin documentos en el snapshot" }
  }
  return { ok: true, documents }
}

export type SaveCompleteLocalInput = {
  userId: string
  planId: number
  customerKey: string
  clientId: number | null
  clientName: string | null
  fichaPayload: unknown
  picking: SnapshotPickingMeta
  payment: OutboxPayment
  note?: string | null
  gps: OutboxGpsCapture | null
  deviceId?: string | null
  /** Si hay borrador existente compatible, reutilizar su localUuid/idem. */
  resumeLocalUuid?: string | null
}

export type SaveLocalResult =
  | { ok: true; op: OutboxOperation; created: boolean }
  | { ok: false; code: string; message: string; existing?: OutboxOperation }

export async function saveCompleteDeliveryLocal(
  input: SaveCompleteLocalInput,
): Promise<SaveLocalResult> {
  if (isOutboxSyncWritesEnabled()) {
    /* 2A.2 no envía; flag documentado para futuro */
  }
  const docsBuilt = buildDocumentsFromFichaPayload(input.fichaPayload)
  if (!docsBuilt.ok) {
    return { ok: false, code: "SNAPSHOT_INCOMPLETE", message: docsBuilt.reason }
  }
  if (input.picking.pickingId == null || !input.picking.pickingVersion) {
    return {
      ok: false,
      code: "PICKING_INVALID",
      message:
        "Sin picking_id/version del snapshot. No se permite confirmar la operación local.",
    }
  }
  if (!input.payment?.method) {
    return {
      ok: false,
      code: "PAYMENT_REQUIRED",
      message: "Declare la forma de pago (sin montos).",
    }
  }

  const existing = await findActiveOutboxForCustomer(
    input.userId,
    input.planId,
    input.customerKey,
  )
  if (existing && existing.localUuid !== input.resumeLocalUuid) {
    if (existing.op_type !== "complete_delivery" || existing.status !== "draft") {
      return {
        ok: false,
        code: "DUPLICATE_PENDING",
        message:
          "Ya existe una operación pendiente para este cliente. Ábrala desde el panel de pendientes.",
        existing,
      }
    }
  }

  let localUuid = input.resumeLocalUuid || newLocalUuid()
  let prior = input.resumeLocalUuid
    ? await getOutboxOp(input.resumeLocalUuid)
    : null
  if (prior && prior.userId !== String(input.userId)) {
    return {
      ok: false,
      code: "USER_ISOLATION",
      message: "No se puede modificar una operación de otro usuario.",
    }
  }
  if (prior && (prior.planId !== Number(input.planId) || prior.customerKey !== input.customerKey ||
    prior.status !== "draft" || prior.attempt_count > 0 || prior.server_confirmed)) {
    return { ok: false, code: "DUPLICATE_PENDING", message: "La entrega ya está registrada. Revisa su estado antes de crear otra.", existing: prior }
  }
  const idem =
    prior?.idempotency_key ||
    stableIdempotencyKey({
      userId: String(input.userId),
      planId: input.planId,
      customerKey: input.customerKey,
      opType: "complete_delivery",
      localUuid,
    })
  if (!prior) {
    localUuid = localUuid
  } else {
    localUuid = prior.localUuid
  }

  const captured = nowIso()
  const op: OutboxOperation = {
    localUuid,
    schemaVersion: 2,
    idempotency_key: idem,
    userId: String(input.userId),
    planId: Number(input.planId),
    customerKey: input.customerKey,
    clientId: input.clientId,
    clientName: input.clientName,
    op_type: "complete_delivery",
    pickingId: input.picking.pickingId,
    pickingVersion: String(input.picking.pickingVersion),
    documents: docsBuilt.documents,
    payment: {
      method: input.payment.method,
      note: input.payment.note || null,
    },
    note: (input.note || "").trim() || null,
    captured_at: prior?.captured_at || captured,
    gps: input.gps,
    evidence: null,
    status: "pending",
    attempt_count: prior?.attempt_count ?? 0,
    last_error: null,
    created_at: prior?.created_at || captured,
    updated_at: captured,
    device_id: input.deviceId || null,
    server_confirmed: false,
    sync_enabled: false,
  }

  try {
    await putOutboxOp(op)
  } catch (e) {
    return {
      ok: false,
      code: "STORAGE_ERROR",
      message:
        e instanceof Error
          ? e.message
          : "No hay espacio o el almacenamiento local falló",
    }
  }
  return { ok: true, op, created: !prior }
}

export type SaveClosedStoreLocalInput = {
  userId: string
  planId: number
  customerKey: string
  clientId: number | null
  clientName: string | null
  picking: SnapshotPickingMeta
  note: string
  gps: OutboxGpsCapture | null
  photo: {
    bytes: ArrayBuffer
    contentType: string
    capturedAt: string
  }
  deviceId?: string | null
  resumeLocalUuid?: string | null
}

export async function saveClosedStoreLocal(
  input: SaveClosedStoreLocalInput,
): Promise<SaveLocalResult> {
  const note = (input.note || "").trim()
  if (note.length < 3) {
    return {
      ok: false,
      code: "NOTE_REQUIRED",
      message: "Observación obligatoria para local cerrado.",
    }
  }
  if (!input.photo?.bytes || input.photo.bytes.byteLength < 32) {
    return {
      ok: false,
      code: "PHOTO_REQUIRED",
      message: "La fotografía es obligatoria y debe quedar guardada en el dispositivo.",
    }
  }
  if (input.picking.pickingId == null || !input.picking.pickingVersion) {
    return {
      ok: false,
      code: "PICKING_INVALID",
      message:
        "Sin picking_id/version del snapshot. No se permite confirmar la operación local.",
    }
  }

  const existing = await findActiveOutboxForCustomer(
    input.userId,
    input.planId,
    input.customerKey,
  )
  if (existing && existing.localUuid !== input.resumeLocalUuid) {
    return {
      ok: false,
      code: "DUPLICATE_PENDING",
      message:
        "Ya existe una operación pendiente para este cliente. Ábrala desde el panel de pendientes.",
      existing,
    }
  }

  let prior = input.resumeLocalUuid
    ? await getOutboxOp(input.resumeLocalUuid)
    : null
  if (prior && prior.userId !== String(input.userId)) {
    return {
      ok: false,
      code: "USER_ISOLATION",
      message: "No se puede modificar una operación de otro usuario.",
    }
  }
  if (prior && (prior.planId !== Number(input.planId) || prior.customerKey !== input.customerKey ||
    prior.status !== "draft" || prior.attempt_count > 0 || prior.server_confirmed)) {
    return { ok: false, code: "DUPLICATE_PENDING", message: "La entrega ya está registrada. Revisa su estado antes de crear otra.", existing: prior }
  }
  const localUuid = prior?.localUuid || newLocalUuid()
  const idem =
    prior?.idempotency_key ||
    stableIdempotencyKey({
      userId: String(input.userId),
      planId: input.planId,
      customerKey: input.customerKey,
      opType: "closed_store",
      localUuid,
    })

  const sha = await sha256Hex(input.photo.bytes)
  const blobId = prior?.evidence?.localBlobId || `blob-${localUuid}`
  try {
    await putOutboxBlob({
      id: blobId,
      userId: String(input.userId),
      contentType: input.photo.contentType || "image/jpeg",
      sha256: sha,
      sizeBytes: input.photo.bytes.byteLength,
      data: input.photo.bytes,
      createdAt: nowIso(),
    })
  } catch {
    return {
      ok: false,
      code: "STORAGE_ERROR",
      message:
        "No se pudo guardar la fotografía (espacio insuficiente u otro error). No se marcó como guardado.",
    }
  }

  const verified = await verifyOutboxBlobPersisted(
    blobId,
    sha,
    input.photo.bytes.byteLength,
  )
  if (!verified.ok) {
    return { ok: false, code: "PHOTO_NOT_PERSISTED", message: verified.reason }
  }

  const captured = nowIso()
  const op: OutboxOperation = {
    localUuid,
    schemaVersion: 2,
    idempotency_key: idem,
    userId: String(input.userId),
    planId: Number(input.planId),
    customerKey: input.customerKey,
    clientId: input.clientId,
    clientName: input.clientName,
    op_type: "closed_store",
    pickingId: input.picking.pickingId,
    pickingVersion: String(input.picking.pickingVersion),
    documents: [],
    payment: null,
    note,
    captured_at: prior?.captured_at || input.photo.capturedAt || captured,
    gps: input.gps,
    evidence: {
      localBlobId: blobId,
      localId: "closed_store",
      content_sha256: sha,
      content_type: input.photo.contentType || "image/jpeg",
      size_bytes: input.photo.bytes.byteLength,
      captured_at: input.photo.capturedAt || captured,
      uploaded_to_r2: false,
    },
    status: "pending",
    attempt_count: prior?.attempt_count ?? 0,
    last_error: null,
    created_at: prior?.created_at || captured,
    updated_at: captured,
    device_id: input.deviceId || null,
    server_confirmed: false,
    sync_enabled: false,
  }

  try {
    await putOutboxOp(op)
  } catch {
    return {
      ok: false,
      code: "STORAGE_ERROR",
      message: "Fotografía guardada, pero falló el registro de la operación. Reintente.",
    }
  }
  return { ok: true, op, created: !prior }
}

/** Aislamiento: otro usuario nunca recibe ops ajenas. */
export async function listOutboxOpsVisibleToUser(
  userId: string,
): Promise<OutboxOperation[]> {
  return listOutboxOpsForUser(String(userId))
}

export function provisionalLocalProgressNote(): string {
  return "Avance provisional local · no cuenta como confirmado por servidor"
}
