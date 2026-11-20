/**
 * Offline V2 2A.4 — capa PWA laboratorio (outbox persistente simulada).
 * No habla con api.quillotana.cl. Valida 5 ops, aislamiento, retención SW/BUILD.
 */
import assert from "node:assert/strict"
import { beforeEach, describe, it } from "node:test"
import { readFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"
import {
  countPendingOutboxOps,
  listPendingOutboxOps,
  listOutboxOpsVisibleToUser,
  resetOutboxMemoryStoreForTests,
  saveClosedStoreLocal,
  saveCompleteDeliveryLocal,
  useOutboxMemoryStoreForTests,
} from "./offline-outbox.ts"
import {
  isOutboxSyncWritesEnabled,
  isProductionQuillotanaHost,
  OFFLINE_OUTBOX_RETENTION,
} from "./offline-outbox-policy.ts"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")

function sampleFicha(docId = 91055, d1 = 91001, d2 = 91002) {
  return {
    client: { client_id: 9201, client_name: "Cliente Lab" },
    document_count: 1,
    documents: [
      {
        related_document_id: docId,
        document_number: 1001,
        document_type: "GD",
        document_total: 1000,
        product_count: 2,
        products: [
          { source_detail_id: d1, unidades: 4, sku: "A", description: "A" },
          { source_detail_id: d2, unidades: 2, sku: "B", description: "B" },
        ],
      },
    ],
  }
}

describe("offline-outbox lab E2E 2A.4", () => {
  beforeEach(() => {
    useOutboxMemoryStoreForTests(true)
    resetOutboxMemoryStoreForTests()
  })

  it("cinco operaciones pendientes sobreviven 'reopen' (misma memoria/IDB)", async () => {
    for (let i = 0; i < 4; i++) {
      const res = await saveCompleteDeliveryLocal({
        userId: "42",
        planId: 920,
        customerKey: `id:92${i}`,
        clientId: 9200 + i,
        clientName: `C${i}`,
        fichaPayload: sampleFicha(91055 + i, 91001 + i * 2, 91002 + i * 2),
        picking: { pickingId: 501, pickingVersion: "3" },
        payment: { method: "transfer_declared" },
        gps: {
          lat: -33.45,
          lng: -70.66,
          accuracy_m: 10,
          captured_at: "2026-11-20T12:00:00-03:00",
          source: "deferred_offline",
        },
      })
      assert.equal(res.ok, true)
      if (res.ok) {
        assert.equal(res.op.status, "pending")
        assert.equal(res.op.server_confirmed, false)
      }
    }
    const closed = await saveClosedStoreLocal({
      userId: "42",
      planId: 920,
      customerKey: "id:9204",
      clientId: 9204,
      clientName: "Cerrado",
      picking: { pickingId: 501, pickingVersion: "3" },
      note: "Local cerrado lab",
      gps: {
        lat: -33.45,
        lng: -70.66,
        accuracy_m: 15,
        captured_at: "2026-11-20T11:00:00-03:00",
        source: "deferred_offline",
      },
      photo: {
        bytes: new Uint8Array(128).fill(9).buffer,
        contentType: "image/jpeg",
        capturedAt: "2026-11-20T11:00:05-03:00",
      },
    })
    assert.equal(closed.ok, true)
    if (closed.ok) {
      assert.equal(closed.op.evidence?.uploaded_to_r2, false)
    }
    assert.equal(await countPendingOutboxOps("42"), 5)
    // Simula cierre/reapertura: misma store, mismos pendientes
    const again = await listPendingOutboxOps("42")
    assert.equal(again.length, 5)
    assert.ok(again.every((o) => o.status === "pending"))
  })

  it("aislamiento logout/cambio de usuario", async () => {
    await saveCompleteDeliveryLocal({
      userId: "42",
      planId: 920,
      customerKey: "id:9201",
      clientId: 9201,
      clientName: "A",
      fichaPayload: sampleFicha(),
      picking: { pickingId: 501, pickingVersion: "3" },
      payment: { method: "cash" },
      gps: null,
    })
    assert.equal(await countPendingOutboxOps("42"), 1)
    assert.equal(await countPendingOutboxOps("99"), 0)
    assert.equal((await listOutboxOpsVisibleToUser("99")).length, 0)
    // clearOfflineForUser V1 no toca outbox (DB distinta) — contrato fuente
    const storeSrc = readFileSync(join(root, "lib/offline-store.ts"), "utf8")
    const authSrc = readFileSync(join(root, "lib/auth.ts"), "utf8")
    assert.match(storeSrc, /quillotana-ops-offline/)
    assert.match(storeSrc, /NO toca la outbox Offline V2/)
    assert.match(authSrc, /countPendingOutboxOps/)
    assert.match(authSrc, /Outbox V2 no se toca/)
  })

  it("flags: default off; Quillotana host identificado (permisos vía servidor)", () => {
    assert.equal(OFFLINE_OUTBOX_RETENTION.syncWritesEnabled, false)
    assert.equal(isOutboxSyncWritesEnabled(), false)
    assert.equal(isProductionQuillotanaHost("api.quillotana.cl"), true)
    assert.equal(isProductionQuillotanaHost("localhost"), false)
  })

  it("SW/BUILD_ID no borran outbox (contratos en fuente)", () => {
    const sw = readFileSync(join(root, "public/sw.js"), "utf8")
    const outboxStore = readFileSync(join(root, "lib/offline-outbox-store.ts"), "utf8")
    const sync = readFileSync(join(root, "lib/offline-sync.ts"), "utf8")
    assert.match(outboxStore, /quillotana-ops-outbox-v2/)
    assert.doesNotMatch(sw, /indexedDB\.deleteDatabase/)
    assert.doesNotMatch(sw, /quillotana-ops-outbox/)
    assert.match(sync, /reconcileOfflineNavForCurrentBuild/)
    // reconcile no menciona outbox wipe
    assert.doesNotMatch(sync, /outbox.*wipe|clearOutbox/i)
  })

  it("UX labels: pendiente vs confirmado servidor", async () => {
    const { outboxStatusLabelEs, outboxResultLabelEs } = await import(
      "./offline-outbox-types.ts"
    )
    assert.equal(
      outboxStatusLabelEs("pending"),
      "Guardado en este dispositivo · Pendiente de sincronización",
    )
    assert.equal(
      outboxStatusLabelEs("syncing"),
      "Sincronizando entregas pendientes",
    )
    assert.equal(
      outboxStatusLabelEs("synced"),
      "Entrega confirmada por el servidor",
    )
    assert.equal(outboxStatusLabelEs("conflict"), "Requiere revisión")
    assert.match(
      outboxResultLabelEs({ op_type: "complete_delivery" }),
      /completa \(local\)/,
    )
  })
})
