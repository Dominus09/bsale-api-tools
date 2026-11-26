import assert from "node:assert/strict"
import { describe, it, beforeEach } from "node:test"
import { claimOutboxOpForSync, getOutboxBlob, getOutboxOp, putOutboxOp, saveUnsentOutboxOp, settleOutboxSync } from "./offline-outbox-store.ts"

import {
  OFFLINE_OUTBOX_RETENTION,
  OFFLINE_V2_PUBLICATION_BLOCKERS,
  buildDocumentsFromFichaPayload,
  countPendingOutboxOps,
  findActiveOutboxForCustomer,
  isOutboxSyncWritesEnabled,
  listOutboxOpsVisibleToUser,
  listPendingOutboxOps,
  resetOutboxMemoryStoreForTests,
  saveClosedStoreLocal,
  saveCompleteDeliveryLocal,
  stableIdempotencyKey,
  useOutboxMemoryStoreForTests,
} from "./offline-outbox.ts"

function sampleFicha() {
  return {
    client: { client_id: 9201, client_name: "Cliente 2A2" },
    document_count: 1,
    documents: [
      {
        related_document_id: 91055,
        document_number: 1001,
        document_type: "GD",
        document_total: 1000,
        product_count: 2,
        products: [
          {
            source_detail_id: 91001,
            unidades: 4,
            sku: "A",
            description: "Prod A",
          },
          {
            source_detail_id: 91002,
            unidades: 2,
            sku: "A",
            description: "Prod A",
          },
        ],
      },
    ],
  }
}

describe("offline-outbox 2A.2", () => {
  beforeEach(() => {
    useOutboxMemoryStoreForTests(true)
    resetOutboxMemoryStoreForTests()
  })

  it("política: no sync writes + no auto-wipe", () => {
    assert.equal(isOutboxSyncWritesEnabled(), false)
    assert.equal(OFFLINE_OUTBOX_RETENTION.autoDeleteUnconfirmed, false)
    assert.equal(OFFLINE_OUTBOX_RETENTION.onLogout, "warn_and_retain")
    assert.equal(OFFLINE_OUTBOX_RETENTION.on401, "retain_outbox")
    assert.equal(
      OFFLINE_OUTBOX_RETENTION.onOtherUserLogin,
      "isolate_only_never_wipe_other_outbox",
    )
    assert.ok(
      OFFLINE_V2_PUBLICATION_BLOCKERS.includes(
        "sync_writes_disabled_by_default_until_server_flag",
      ),
    )
    assert.ok(
      OFFLINE_V2_PUBLICATION_BLOCKERS.includes("no_client_flags_grant_backend_writes"),
    )
  })

  it("idempotency_key estable (no regenera por reintento)", () => {
    const a = stableIdempotencyKey({
      userId: "42",
      planId: 910,
      customerKey: "id:9201",
      opType: "complete_delivery",
      localUuid: "same-uuid",
    })
    const b = stableIdempotencyKey({
      userId: "42",
      planId: 910,
      customerKey: "id:9201",
      opType: "complete_delivery",
      localUuid: "same-uuid",
    })
    assert.equal(a, b)
    assert.match(a, /^v2:42:910:/)
  })

  it("entrega completa offline + replay misma clave", async () => {
    const first = await saveCompleteDeliveryLocal({
      userId: "42",
      planId: 910,
      customerKey: "id:9201",
      clientId: 9201,
      clientName: "Cliente 2A2",
      fichaPayload: sampleFicha(),
      picking: { pickingId: 501, pickingVersion: "3" },
      payment: { method: "transfer_declared" },
      note: "OK",
      gps: {
        lat: -33.45,
        lng: -70.66,
        accuracy_m: 12,
        captured_at: "2026-11-20T12:00:00-03:00",
        source: "deferred_offline",
      },
    })
    assert.equal(first.ok, true)
    if (!first.ok) return
    assert.equal(first.op.status, "pending")
    assert.equal(first.op.server_confirmed, false)
    assert.equal(first.op.sync_enabled, false)

    const again = await saveCompleteDeliveryLocal({
      userId: "42",
      planId: 910,
      customerKey: "id:9201",
      clientId: 9201,
      clientName: "Cliente 2A2",
      fichaPayload: sampleFicha(),
      picking: { pickingId: 501, pickingVersion: "3" },
      payment: { method: "cash" },
      note: "upd",
      gps: null,
      resumeLocalUuid: first.op.localUuid,
    })
    assert.equal(again.ok, true)
    if (!again.ok) return
    assert.equal(again.op.localUuid, first.op.localUuid)
    assert.equal(again.op.idempotency_key, first.op.idempotency_key)
    assert.equal(again.op.payment?.method, "cash")
  })

  it("solo permite reanudar pending antes del primer intento", async () => {
    const input = {
      userId: "42", planId: 910, customerKey: "id:9201", clientId: 9201, clientName: "X",
      fichaPayload: sampleFicha(), picking: { pickingId: 501, pickingVersion: "3" },
      payment: { method: "cash" }, gps: null,
    }
    const initial = await saveCompleteDeliveryLocal(input)
    assert.equal(initial.ok, true)
    const claim = await claimOutboxOpForSync(initial.op.localUuid, "42")
    assert.ok(claim)
    const stale = { ...initial.op, note: "stale edit" }
    assert.equal(await saveUnsentOutboxOp(stale, initial.op), false)
    await settleOutboxSync(claim, { status: "pending", last_error: "timeout" })
    const retryPending = await saveCompleteDeliveryLocal({ ...input, note: "edit", resumeLocalUuid: initial.op.localUuid })
    assert.equal(retryPending.ok, false)
    assert.equal(retryPending.code, "DUPLICATE_PENDING")
    for (const status of ["syncing", "synced", "conflict", "failed_needs_action"]) {
      await putOutboxOp({ ...initial.op, status, attempt_count: 1 })
      const res = await saveCompleteDeliveryLocal({ ...input, resumeLocalUuid: initial.op.localUuid })
      assert.equal(res.ok, false)
    }
    await putOutboxOp({ ...initial.op, status: "pending", server_confirmed: true })
    assert.equal((await saveCompleteDeliveryLocal({ ...input, resumeLocalUuid: initial.op.localUuid })).ok, false)
  })

  it("dos guardados simultáneos de un cliente conservan una sola operación", async () => {
    const input = {
      userId: "42", planId: 910, customerKey: "id:9201", clientId: 9201, clientName: "X",
      fichaPayload: sampleFicha(), picking: { pickingId: 501, pickingVersion: "3" },
      payment: { method: "cash" }, gps: null,
    }
    const results = await Promise.all([saveCompleteDeliveryLocal(input), saveCompleteDeliveryLocal(input)])
    assert.equal(results.filter(r => r.ok).length, 1)
    assert.equal(await countPendingOutboxOps("42"), 1)
  })

  it("local cerrado pending puede reanudarse sin sobrescribir evidencia anterior", async () => {
    const input = {
      userId: "42", planId: 910, customerKey: "id:9201", clientId: 9201, clientName: "X",
      picking: { pickingId: 501, pickingVersion: "3" }, note: "Local cerrado", gps: null,
      photo: { bytes: new Uint8Array(64).fill(7).buffer, contentType: "image/jpeg", capturedAt: "2026-11-20T11:00:00-03:00" },
    }
    const first = await saveClosedStoreLocal(input)
    assert.equal(first.ok, true)
    const second = await saveClosedStoreLocal({ ...input, resumeLocalUuid: first.op.localUuid,
      photo: { ...input.photo, bytes: new Uint8Array(64).fill(8).buffer } })
    assert.equal(second.ok, true)
    assert.equal(second.op.idempotency_key, first.op.idempotency_key)
    assert.notEqual(second.op.evidence.localBlobId, first.op.evidence.localBlobId)
    assert.equal((await getOutboxBlob(first.op.evidence.localBlobId)).sha256, first.op.evidence.content_sha256)
    assert.equal((await getOutboxOp(first.op.localUuid)).evidence.content_sha256, second.op.evidence.content_sha256)
    const claim = await claimOutboxOpForSync(first.op.localUuid, "42")
    assert.ok(claim)
    const blocked = await saveClosedStoreLocal({ ...input, resumeLocalUuid: first.op.localUuid })
    assert.equal(blocked.ok, false)
  })

  it("bloquea duplicado pendiente mismo plan/cliente", async () => {
    const first = await saveCompleteDeliveryLocal({
      userId: "42",
      planId: 910,
      customerKey: "id:9201",
      clientId: 9201,
      clientName: "X",
      fichaPayload: sampleFicha(),
      picking: { pickingId: 501, pickingVersion: "3" },
      payment: { method: "cash" },
      gps: null,
    })
    assert.equal(first.ok, true)
    const dup = await saveClosedStoreLocal({
      userId: "42",
      planId: 910,
      customerKey: "id:9201",
      clientId: 9201,
      clientName: "X",
      picking: { pickingId: 501, pickingVersion: "3" },
      note: "Local cerrado sin atención",
      gps: null,
      photo: {
        bytes: new Uint8Array(64).fill(7).buffer,
        contentType: "image/jpeg",
        capturedAt: "2026-11-20T11:00:00-03:00",
      },
    })
    assert.equal(dup.ok, false)
    if (dup.ok) return
    assert.equal(dup.code, "DUPLICATE_PENDING")
  })

  it("picking incompleto / snapshot inválido", async () => {
    const noPick = await saveCompleteDeliveryLocal({
      userId: "42",
      planId: 910,
      customerKey: "id:9201",
      clientId: 9201,
      clientName: "X",
      fichaPayload: sampleFicha(),
      picking: { pickingId: null, pickingVersion: null },
      payment: { method: "cash" },
      gps: null,
    })
    assert.equal(noPick.ok, false)
    if (!noPick.ok) assert.equal(noPick.code, "PICKING_INVALID")

    const bad = buildDocumentsFromFichaPayload({ client: {}, documents: [] })
    assert.equal(bad.ok, false)
  })

  it("local cerrado exige foto persistida; sin foto no OK", async () => {
    const noPhoto = await saveClosedStoreLocal({
      userId: "42",
      planId: 910,
      customerKey: "id:9201",
      clientId: 9201,
      clientName: "X",
      picking: { pickingId: 501, pickingVersion: "3" },
      note: "Cerrado",
      gps: {
        lat: -33.45,
        lng: -70.66,
        accuracy_m: 10,
        captured_at: "2026-11-20T11:00:00-03:00",
        source: "deferred_offline",
      },
      photo: {
        bytes: new ArrayBuffer(8),
        contentType: "image/jpeg",
        capturedAt: "2026-11-20T11:00:05-03:00",
      },
    })
    assert.equal(noPhoto.ok, false)

    const ok = await saveClosedStoreLocal({
      userId: "42",
      planId: 910,
      customerKey: "id:9201",
      clientId: 9201,
      clientName: "X",
      picking: { pickingId: 501, pickingVersion: "3" },
      note: "Local cerrado sin atención",
      gps: null,
      photo: {
        bytes: new Uint8Array(128).fill(1).buffer,
        contentType: "image/jpeg",
        capturedAt: "2026-11-20T11:00:05-03:00",
      },
    })
    assert.equal(ok.ok, true)
    if (!ok.ok) return
    assert.equal(ok.op.evidence?.uploaded_to_r2, false)
    assert.equal(ok.op.payment, null)
    assert.equal(ok.op.documents.length, 0)
  })

  it("cinco operaciones consecutivas + aislamiento por usuario", async () => {
    for (let i = 0; i < 5; i++) {
      const res = await saveCompleteDeliveryLocal({
        userId: "42",
        planId: 910,
        customerKey: `id:92${i}`,
        clientId: 9200 + i,
        clientName: `C${i}`,
        fichaPayload: sampleFicha(),
        picking: { pickingId: 501, pickingVersion: "3" },
        payment: { method: "transfer_declared" },
        gps: null,
      })
      assert.equal(res.ok, true)
    }
    assert.equal(await countPendingOutboxOps("42"), 5)
    assert.equal(await countPendingOutboxOps("99"), 0)
    const other = await listOutboxOpsVisibleToUser("99")
    assert.equal(other.length, 0)
    const mine = await listPendingOutboxOps("42")
    assert.equal(mine.length, 5)
  })

  it("doble pulsación / mismo resume no crea segunda visita", async () => {
    const a = await saveCompleteDeliveryLocal({
      userId: "42",
      planId: 910,
      customerKey: "id:9201",
      clientId: 9201,
      clientName: "X",
      fichaPayload: sampleFicha(),
      picking: { pickingId: 501, pickingVersion: "3" },
      payment: { method: "cash" },
      gps: null,
    })
    assert.equal(a.ok, true)
    if (!a.ok) return
    const b = await saveCompleteDeliveryLocal({
      userId: "42",
      planId: 910,
      customerKey: "id:9201",
      clientId: 9201,
      clientName: "X",
      fichaPayload: sampleFicha(),
      picking: { pickingId: 501, pickingVersion: "3" },
      payment: { method: "cash" },
      gps: null,
    })
    assert.equal(b.ok, false)
    if (!b.ok) assert.equal(b.code, "DUPLICATE_PENDING")
    const found = await findActiveOutboxForCustomer("42", 910, "id:9201")
    assert.ok(found)
    assert.equal(found.localUuid, a.op.localUuid)
  })

  it("otro usuario no ve ops ajenas; original recupera", async () => {
    const saved = await saveCompleteDeliveryLocal({
      userId: "42",
      planId: 910,
      customerKey: "id:9201",
      clientId: 9201,
      clientName: "X",
      fichaPayload: sampleFicha(),
      picking: { pickingId: 501, pickingVersion: "3" },
      payment: { method: "check_declared" },
      gps: null,
    })
    assert.equal(saved.ok, true)
    assert.equal((await listPendingOutboxOps("77")).length, 0)
    assert.equal((await listPendingOutboxOps("42")).length, 1)
  })

  it("outbox DB distinta de snapshots V1 (contrato de retención)", async () => {
    const { readFileSync } = await import("node:fs")
    const { dirname, join } = await import("node:path")
    const { fileURLToPath } = await import("node:url")
    const root = join(dirname(fileURLToPath(import.meta.url)), "..")
    const storeSrc = readFileSync(join(root, "lib/offline-store.ts"), "utf8")
    const outboxSrc = readFileSync(join(root, "lib/offline-outbox-store.ts"), "utf8")
    const authSrc = readFileSync(join(root, "lib/auth.ts"), "utf8")
    assert.match(storeSrc, /quillotana-ops-offline/)
    assert.match(outboxSrc, /quillotana-ops-outbox-v2/)
    assert.match(storeSrc, /NO toca la outbox Offline V2/)
    assert.match(authSrc, /Outbox V2 no se toca/)
    assert.match(authSrc, /countPendingOutboxOps/)
    await saveCompleteDeliveryLocal({
      userId: "42",
      planId: 910,
      customerKey: "id:9201",
      clientId: 9201,
      clientName: "X",
      fichaPayload: sampleFicha(),
      picking: { pickingId: 501, pickingVersion: "3" },
      payment: { method: "cash" },
      gps: null,
    })
    assert.equal(await countPendingOutboxOps("42"), 1)
  })
})
