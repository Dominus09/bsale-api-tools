import assert from "node:assert/strict"
import { describe, it } from "node:test"
import { readFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"
import {
  isOutboxSyncWritesEnabled,
  isProductionQuillotanaHost,
  OFFLINE_OUTBOX_RETENTION,
} from "./offline-outbox-policy.ts"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")

describe("offline-outbox-sync 2A.3 policy", () => {
  it("sync writes off por defecto", () => {
    assert.equal(OFFLINE_OUTBOX_RETENTION.syncWritesEnabled, false)
    assert.equal(isOutboxSyncWritesEnabled(), false)
  })

  it("identifica hosts Quillotana (lab route / hints; sync vía servidor)", () => {
    assert.equal(isProductionQuillotanaHost("api.quillotana.cl"), true)
    assert.equal(isProductionQuillotanaHost("app.quillotana.cl"), true)
    assert.equal(isProductionQuillotanaHost("localhost"), false)
    assert.equal(isProductionQuillotanaHost("127.0.0.1"), false)
  })

  it("runtime documenta no Background Sync y flag", () => {
    const src = readFileSync(join(root, "lib/offline-outbox-sync.ts"), "utf8")
    assert.match(src, /No usa Background Sync/)
    assert.match(src, /isOutboxSyncWritesEnabled/)
    assert.match(src, /refreshOfflineV2PilotAuthorization/)
    assert.match(src, /idempotency_key/)
    assert.match(src, /backoff/)
    assert.match(src, /offline-ops\/sync/)
    assert.doesNotMatch(src, /isProdHost\(\)/)
  })

  it("backend flag OFF por defecto en servicio", () => {
    const src = readFileSync(
      join(root, "../../backend/services/distribuidora/delivery_offline_sync_service.py"),
      "utf8",
    )
    assert.match(src, /OFFLINE_SYNC_WRITES_ENABLED/)
    assert.match(src, /require_writes_flag/)
    assert.match(src, /delivery_offline_validate/)
  })

  it("validador real existe y no confía solo en IndexedDB", () => {
    const src = readFileSync(
      join(root, "../../backend/services/distribuidora/delivery_offline_validate.py"),
      "utf8",
    )
    assert.match(src, /get_picking_by_client/)
    assert.match(src, /get_client_ficha_for_driver/)
    assert.match(src, /PICKING_CHANGED/)
    assert.match(src, /DOCUMENT_MISMATCH/)
  })
})


describe("Choferes 2.1: leases and visible state", () => {
  it("reclaims an expired syncing operation, never a review or confirmed operation", async () => {
    const { canClaimOutboxSync, outboxUiGroup, OUTBOX_SYNC_LEASE_MS } = await import("./offline-outbox-types.ts")
    const now = Date.now()
    const row = { status: "syncing", updated_at: new Date(now).toISOString(), server_confirmed: false }
    assert.equal(canClaimOutboxSync(row, now), false)
    assert.equal(outboxUiGroup(row, now), "sending")
    assert.equal(canClaimOutboxSync(row, now + OUTBOX_SYNC_LEASE_MS), true)
    assert.equal(outboxUiGroup(row, now + OUTBOX_SYNC_LEASE_MS), "local")
    for (const status of ["conflict", "failed_needs_action", "draft", "synced"]) {
      assert.equal(canClaimOutboxSync({ ...row, status }, now + OUTBOX_SYNC_LEASE_MS), false)
    }
    assert.equal(outboxUiGroup({ ...row, status: "synced", server_confirmed: true }, now), "confirmed")
    assert.equal(outboxUiGroup({ ...row, status: "synced", server_confirmed: false }, now), "review")
  })

  it("claims once, keeps idempotency, rejects a late settlement and isolates users", async () => {
    const { claimOutboxOpForSync, settleOutboxSync, putOutboxOp, getOutboxOp, useOutboxMemoryStoreForTests } = await import("./offline-outbox-store.ts")
    useOutboxMemoryStoreForTests(true)
    try {
      const now = Date.now()
      const row = { localUuid: "op1", userId: "42", idempotency_key: "stable-key", status: "pending", updated_at: new Date(now).toISOString(), attempt_count: 0, server_confirmed: false }
      await putOutboxOp(row)
      assert.equal(await claimOutboxOpForSync("op1", "99", now), null)
      const first = await claimOutboxOpForSync("op1", "42", now)
      assert.ok(first)
      assert.equal(await claimOutboxOpForSync("op1", "42", now + 1), null)
      const second = await claimOutboxOpForSync("op1", "42", now + 120_001)
      assert.ok(second)
      assert.equal(second.idempotency_key, "stable-key")
      assert.equal(second.attempt_count, 2)
      assert.equal(await settleOutboxSync(first, { status: "pending", last_error: "late" }), null)
      await settleOutboxSync(second, { status: "synced", server_confirmed: true, last_error: null })
      assert.equal((await getOutboxOp("op1")).status, "synced")
      assert.equal(await claimOutboxOpForSync("op1", "42", now + 240_002), null)
    } finally {
      useOutboxMemoryStoreForTests(false)
    }
  })
})
