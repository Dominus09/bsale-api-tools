/**
 * Offline V2 2A.3 — motor de sincronización de outbox.
 * DESHABILITADO por defecto. No usa Background Sync (Safari iOS).
 */

import { getSessionUserId, getToken } from "./auth.ts"
import { ApiError } from "./api.ts"
import { getApiBase } from "./api-base.ts"
import { multipartAuthHeaders } from "./multipart-headers.ts"
import { isOutboxSyncWritesEnabled } from "./offline-outbox-policy.ts"
import {
  getOutboxBlob,
  getOutboxOp,
  listOutboxOpsForUser,
  putOutboxOp,
} from "./offline-outbox-store.ts"
import type { OutboxOperation } from "./offline-outbox-types.ts"
import { isActiveOutboxStatus } from "./offline-outbox.ts"

const MAX_BACKOFF_MS = 60_000
const BASE_BACKOFF_MS = 2_000
const UPLOAD_TIMEOUT_MS = 60_000

type SyncListener = (state: OutboxSyncState) => void

export type OutboxSyncState = {
  phase: "idle" | "syncing" | "paused_auth" | "disabled"
  message: string | null
  lastResult: string | null
  inFlightCount: number
  pendingCount: number
}

function isProdHost(): boolean {
  if (typeof window === "undefined") return false
  const h = window.location.hostname.toLowerCase()
  return (
    h === "api.quillotana.cl" ||
    h.endsWith(".quillotana.cl") ||
    h.includes("quillotana.cl")
  )
}

async function postOfflineOp(
  planId: number,
  payload: Record<string, unknown>,
  evidence?: Blob | null,
): Promise<Record<string, unknown>> {
  const fd = new FormData()
  fd.append("payload", JSON.stringify(payload))
  if (evidence) {
    fd.append("evidence", evidence, "closed_store.jpg")
  }
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), UPLOAD_TIMEOUT_MS)
  try {
    const res = await fetch(
      `${getApiBase()}/distribuidora/pwa/driver/despachos/${planId}/offline-ops/sync`,
      {
        method: "POST",
        headers: multipartAuthHeaders(),
        body: fd,
        signal: controller.signal,
      },
    )
    if (res.status === 401) {
      throw new ApiError(401, "Sesión expirada", "UNAUTHORIZED")
    }
    const raw = await res.text()
    let json: unknown = null
    try {
      json = raw ? JSON.parse(raw) : null
    } catch {
      json = null
    }
    if (!res.ok) {
      const detail =
        json && typeof json === "object" && "detail" in json
          ? (json as { detail: unknown }).detail
          : raw
      let message = raw.slice(0, 240) || `Error ${res.status}`
      let code: string | null = null
      if (typeof detail === "string") message = detail
      else if (detail && typeof detail === "object") {
        const d = detail as { message_es?: string; message?: string; code?: string }
        message = String(d.message_es || d.message || message)
        code = typeof d.code === "string" ? d.code : null
      }
      throw new ApiError(res.status, message, code)
    }
    return (json || {}) as Record<string, unknown>
  } finally {
    clearTimeout(timer)
  }
}

function buildSyncPayload(op: OutboxOperation): Record<string, unknown> {
  return {
    idempotency_key: op.idempotency_key,
    client_mutation_id: op.localUuid,
    op_type: op.op_type,
    customer_key: op.customerKey,
    picking_id: op.pickingId,
    picking_version: op.pickingVersion,
    captured_at: op.captured_at,
    device_id: op.device_id,
    result:
      op.op_type === "complete_delivery"
        ? { status: "completed", note: op.note }
        : {
            status: "not_delivered",
            reason: "closed_store",
            note: op.note,
          },
    documents: op.documents,
    payment: op.payment,
    note: op.note,
    gps: op.gps
      ? {
          lat: op.gps.lat,
          lng: op.gps.lng,
          accuracy_m: op.gps.accuracy_m,
          captured_at: op.gps.captured_at,
        }
      : null,
    gps_exception:
      op.gps && op.gps.lat == null
        ? {
            code: op.gps.exception_code || "gps_unavailable",
            note: op.gps.exception_note || "GPS offline ausente",
          }
        : null,
    evidence_meta: op.evidence
      ? {
          local_id: op.evidence.localId,
          content_sha256: op.evidence.content_sha256,
          captured_at: op.evidence.captured_at,
        }
      : null,
  }
}

function classifyError(
  err: unknown,
): "auth" | "forbidden" | "conflict" | "validation" | "retry" | "fatal" {
  if (!(err instanceof ApiError)) return "retry"
  if (err.status === 401) return "auth"
  if (err.status === 403) return "forbidden"
  if (err.status === 409) return "conflict"
  if (err.status === 422) return "validation"
  if (err.status === 429 || err.status >= 500 || err.status === 0) return "retry"
  if (err.code === "OFFLINE_SYNC_WRITES_DISABLED") return "fatal"
  return "retry"
}

class OutboxSyncRuntime {
  private listeners = new Set<SyncListener>()
  private running = false
  private inFlightKeys = new Set<string>()
  private planCustomerLocks = new Set<string>()
  private backoffUntil = 0
  private state: OutboxSyncState = {
    phase: "disabled",
    message: null,
    lastResult: null,
    inFlightCount: 0,
    pendingCount: 0,
  }

  subscribe(fn: SyncListener): () => void {
    this.listeners.add(fn)
    fn(this.state)
    return () => this.listeners.delete(fn)
  }

  getState(): OutboxSyncState {
    return this.state
  }

  private emit(partial: Partial<OutboxSyncState>) {
    this.state = { ...this.state, ...partial }
    for (const fn of this.listeners) fn(this.state)
  }

  async refreshPendingCount(): Promise<number> {
    const uid = getSessionUserId()
    if (!uid) {
      this.emit({ pendingCount: 0 })
      return 0
    }
    const ops = await listOutboxOpsForUser(uid)
    const n = ops.filter((o) => isActiveOutboxStatus(o.status)).length
    this.emit({ pendingCount: n })
    return n
  }

  /** Kick seguro: no-op si flag off o host productivo. */
  async kick(reason: string): Promise<void> {
    if (isProdHost() || !isOutboxSyncWritesEnabled()) {
      this.emit({
        phase: "disabled",
        message: "Sync de escrituras deshabilitado (laboratorio / flag)",
      })
      await this.refreshPendingCount()
      return
    }
    if (!getToken() || !getSessionUserId()) {
      this.emit({ phase: "paused_auth", message: "Sesión requerida para sincronizar" })
      return
    }
    if (Date.now() < this.backoffUntil) return
    if (this.running) return
    this.running = true
    this.emit({
      phase: "syncing",
      message: `Sincronizando entregas pendientes… (${reason})`,
    })
    try {
      await this.drainQueue()
    } finally {
      this.running = false
      const pending = await this.refreshPendingCount()
      this.emit({
        phase: pending > 0 ? "idle" : "idle",
        message: pending > 0 ? `${pending} pendientes` : "Sin pendientes",
        inFlightCount: 0,
      })
    }
  }

  async syncNow(): Promise<void> {
    this.backoffUntil = 0
    await this.kick("manual")
  }

  private async drainQueue(): Promise<void> {
    const uid = getSessionUserId()
    if (!uid) return
    const ops = (await listOutboxOpsForUser(uid)).filter(
      (o) => o.status === "pending" || o.status === "failed_needs_action",
    )
    // Orden FIFO por captura
    ops.sort((a, b) => a.captured_at.localeCompare(b.captured_at))

    let transientFails = 0
    for (const op of ops) {
      if (!getToken()) {
        this.emit({ phase: "paused_auth", message: "Sesión expirada — outbox conservada" })
        return
      }
      const pcKey = `${op.planId}:${op.customerKey}`
      if (this.planCustomerLocks.has(pcKey)) continue
      if (this.inFlightKeys.has(op.idempotency_key)) continue

      this.planCustomerLocks.add(pcKey)
      this.inFlightKeys.add(op.idempotency_key)
      this.emit({ inFlightCount: this.inFlightKeys.size })
      try {
        const result = await this.processOne(op)
        if (result === "auth") return
        if (result === "retry") transientFails += 1
      } finally {
        this.planCustomerLocks.delete(pcKey)
        this.inFlightKeys.delete(op.idempotency_key)
        this.emit({ inFlightCount: this.inFlightKeys.size })
      }
    }
    if (transientFails > 0) {
      const delay = Math.min(
        MAX_BACKOFF_MS,
        BASE_BACKOFF_MS * Math.pow(2, Math.min(5, transientFails - 1)),
      )
      this.backoffUntil = Date.now() + delay
    }
  }

  private async processOne(
    op: OutboxOperation,
  ): Promise<"ok" | "auth" | "retry" | "stop"> {
    const fresh = await getOutboxOp(op.localUuid)
    if (!fresh || fresh.userId !== String(getSessionUserId())) return "stop"
    if (fresh.status === "synced") return "ok"
    if (fresh.status === "conflict") return "stop"

    const syncing: OutboxOperation = {
      ...fresh,
      status: "syncing",
      attempt_count: (fresh.attempt_count || 0) + 1,
      updated_at: new Date().toISOString(),
      last_error: null,
    }
    await putOutboxOp(syncing)
    if (typeof window !== "undefined") {
      window.dispatchEvent(new Event("lq-outbox-changed"))
    }

    try {
      let evidenceBlob: Blob | null = null
      if (fresh.op_type === "closed_store") {
        if (!fresh.evidence?.localBlobId) {
          throw new ApiError(422, "Fotografía local ausente", "PHOTO_REQUIRED")
        }
        const blob = await getOutboxBlob(fresh.evidence.localBlobId)
        if (!blob || blob.sha256 !== fresh.evidence.content_sha256) {
          throw new ApiError(422, "Fotografía corrupta o ausente", "PHOTO_CORRUPT")
        }
        evidenceBlob = new Blob([blob.data], {
          type: blob.contentType || "image/jpeg",
        })
      }

      const payload = buildSyncPayload(fresh)
      const res = await postOfflineOp(fresh.planId, payload, evidenceBlob)
      const visit = (res.visit || null) as { id?: number; status?: string } | null
      const synced: OutboxOperation = {
        ...fresh,
        status: "synced",
        attempt_count: syncing.attempt_count,
        updated_at: new Date().toISOString(),
        last_error: null,
        server_confirmed: true,
        sync_enabled: true,
      }
      await putOutboxOp(synced)
      this.emit({
        lastResult: visit?.id
          ? `Entrega confirmada por el servidor (visita #${visit.id})`
          : "Entrega confirmada por el servidor",
      })
      if (typeof window !== "undefined") {
        window.dispatchEvent(new Event("lq-outbox-changed"))
      }
      return "ok"
    } catch (err) {
      const kind = classifyError(err)
      const message = err instanceof Error ? err.message : "Error de sincronización"
      if (kind === "auth") {
        await putOutboxOp({
          ...fresh,
          status: "pending",
          attempt_count: syncing.attempt_count,
          last_error: message,
          updated_at: new Date().toISOString(),
        })
        this.emit({
          phase: "paused_auth",
          message: "401 — outbox conservada. Inicie sesión de nuevo.",
        })
        return "auth"
      }
      if (kind === "conflict" || kind === "forbidden" || kind === "validation") {
        await putOutboxOp({
          ...fresh,
          status: kind === "conflict" ? "conflict" : "failed_needs_action",
          attempt_count: syncing.attempt_count,
          last_error: message,
          updated_at: new Date().toISOString(),
        })
        this.emit({
          lastResult:
            kind === "conflict" ? "Requiere revisión" : `Error: ${message}`,
        })
        if (typeof window !== "undefined") {
          window.dispatchEvent(new Event("lq-outbox-changed"))
        }
        return "stop"
      }
      if (kind === "fatal") {
        await putOutboxOp({
          ...fresh,
          status: "pending",
          attempt_count: syncing.attempt_count,
          last_error: message,
          updated_at: new Date().toISOString(),
        })
        this.emit({
          phase: "disabled",
          message: message,
        })
        return "stop"
      }
      // retry: no asumir fallo de servidor (timeout / 5xx)
      await putOutboxOp({
        ...fresh,
        status: "pending",
        attempt_count: syncing.attempt_count,
        last_error: message,
        updated_at: new Date().toISOString(),
      })
      if (typeof window !== "undefined") {
        window.dispatchEvent(new Event("lq-outbox-changed"))
      }
      return "retry"
    }
  }
}

export const outboxSyncRuntime = new OutboxSyncRuntime()

/** Arranque de listeners de conectividad / foreground (sin Background Sync). */
export function bindOutboxSyncTriggers(): () => void {
  if (typeof window === "undefined") return () => {}

  const onOnline = () => {
    void outboxSyncRuntime.kick("online")
  }
  const onVis = () => {
    if (document.visibilityState === "visible") {
      void outboxSyncRuntime.kick("foreground")
    }
  }
  const onFocus = () => {
    void outboxSyncRuntime.kick("focus")
  }

  window.addEventListener("online", onOnline)
  document.addEventListener("visibilitychange", onVis)
  window.addEventListener("focus", onFocus)
  void outboxSyncRuntime.refreshPendingCount()
  void outboxSyncRuntime.kick("startup")

  return () => {
    window.removeEventListener("online", onOnline)
    document.removeEventListener("visibilitychange", onVis)
    window.removeEventListener("focus", onFocus)
  }
}
