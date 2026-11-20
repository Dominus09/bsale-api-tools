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

  it("runtime documenta no Background Sync, piloto y flag", () => {
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
