import assert from "node:assert/strict"
import { describe, it } from "node:test"
import {
  formatSyncedAt,
  isLikelyOfflineError,
  offlineUserPrefix,
} from "./offline-store.ts"

describe("offline-store helpers", () => {
  it("aísla claves por userId", () => {
    assert.equal(offlineUserPrefix(42), "u:42:")
    assert.equal(offlineUserPrefix("abc"), "u:abc:")
    assert.notEqual(offlineUserPrefix(1), offlineUserPrefix(2))
    assert.ok(!offlineUserPrefix(1).startsWith(offlineUserPrefix(2)))
  })

  it("formatea syncedAt en es-CL o fallback", () => {
    assert.equal(formatSyncedAt(null), "—")
    assert.equal(formatSyncedAt(""), "—")
    const label = formatSyncedAt("2026-11-18T15:30:00.000Z")
    assert.ok(typeof label === "string" && label.length > 0)
    assert.notEqual(label, "—")
  })

  it("detecta errores de red / offline", () => {
    assert.equal(isLikelyOfflineError({ status: 503 }), true)
    assert.equal(isLikelyOfflineError({ status: 504 }), true)
    assert.equal(isLikelyOfflineError({ status: 0 }), true)
    assert.equal(isLikelyOfflineError({ status: 401 }), false)
    assert.equal(isLikelyOfflineError({ status: 404 }), false)
  })
})
