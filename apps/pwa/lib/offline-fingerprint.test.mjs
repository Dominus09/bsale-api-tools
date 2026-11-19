import assert from "node:assert/strict"
import { describe, it } from "node:test"
import {
  buildOfflineFingerprint,
  fingerprintKey,
  fingerprintsEqual,
  pickingIdentityChanged,
} from "./offline-fingerprint.ts"

describe("offline-fingerprint", () => {
  it("igualdad por picking_id/version y volúmenes", () => {
    const a = buildOfflineFingerprint({
      planId: 10,
      pickingId: 55,
      pickingVersion: 3,
      clients: 12,
      documents: 20,
    })
    const b = buildOfflineFingerprint({
      planId: 10,
      pickingId: 55,
      pickingVersion: "3",
      clients: 12,
      documents: 20,
    })
    assert.equal(fingerprintsEqual(a, b), true)
    assert.ok(fingerprintKey(a).includes("pk55"))
  })

  it("cambio de picking exige re-descarga", () => {
    const prev = buildOfflineFingerprint({
      planId: 10,
      pickingId: 55,
      pickingVersion: 3,
      clients: 12,
      documents: 20,
    })
    const next = buildOfflineFingerprint({
      planId: 10,
      pickingId: 55,
      pickingVersion: 4,
      clients: 12,
      documents: 20,
    })
    assert.equal(pickingIdentityChanged(prev, next), true)
    assert.equal(fingerprintsEqual(prev, next), false)
  })

  it("truncado >80 clientes queda marcado", () => {
    const fp = buildOfflineFingerprint({
      planId: 1,
      pickingId: 1,
      pickingVersion: 1,
      clients: 90,
      documents: 100,
      truncated: true,
    })
    assert.equal(fp.truncated, true)
  })
})
