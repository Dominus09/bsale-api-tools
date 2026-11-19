/**
 * Contratos Offline V1 smart-sync (sin red / sin IDB real).
 */
import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { describe, it } from "node:test"
import { fileURLToPath } from "node:url"
import { planSnapshotIntegrity } from "./offline-store.ts"
import {
  buildOfflineFingerprint,
  fingerprintKey,
} from "./offline-fingerprint.ts"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")

describe("offline smart-sync contratos", () => {
  it(">80 clientes no marca integridad completa", () => {
    const clients = Array.from({ length: 81 }, () => ({
      document_count: 1,
      units_total: 1,
    }))
    const integrity = planSnapshotIntegrity({
      clients,
      summary: { clients: 81, documents: 81 },
      detailsOk: 80,
      documentsFromDetails: 80,
      truncated: true,
    })
    assert.equal(integrity.ok, false)
    assert.match(integrity.reason || "", /truncad/i)
  })

  it("fingerprint incluye picking_id/version", () => {
    const fp = buildOfflineFingerprint({
      planId: 42,
      pickingId: 9,
      pickingVersion: 2,
      clients: 2,
      documents: 4,
      truncated: false,
    })
    assert.equal(fp.pickingId, 9)
    assert.equal(fp.pickingVersion, "2")
    assert.ok(fingerprintKey(fp).includes("pk9"))
  })

  it("despachos ya no auto-sync today+history.slice(0,2)", () => {
    const page = readFileSync(
      join(root, "app/(app)/despachos/page.tsx"),
      "utf8",
    )
    assert.doesNotMatch(page, /history\.slice\(0,\s*2\)/)
    assert.match(page, /runPriorityCycle/)
    assert.match(page, /selectProgressProbeCandidates/)
  })

  it("clientes no dispara syncPlanForOffline directo", () => {
    const page = readFileSync(
      join(root, "app/(app)/despachos/[planId]/clientes/page.tsx"),
      "utf8",
    )
    assert.doesNotMatch(page, /syncPlanForOffline\(/)
    assert.match(page, /offlineSyncRuntime/)
  })

  it("runtime limita un ciclo y dedupe por plan", () => {
    const runtime = readFileSync(
      join(root, "lib/offline-sync-runtime.ts"),
      "utf8",
    )
    assert.match(runtime, /inflightByPlan/)
    assert.match(runtime, /cycleInflight/)
    assert.match(runtime, /selectProgressProbeCandidates/)
    const sync = readFileSync(join(root, "lib/offline-sync.ts"), "utf8")
    assert.match(sync, /fingerprintsEqual/)
    assert.match(sync, /phase: \"skip\"/)
  })

  it("UI indicador + modal presentes", () => {
    const menu = readFileSync(join(root, "components/app-menu.tsx"), "utf8")
    assert.match(menu, /OfflineStatusIndicator/)
    const layout = readFileSync(join(root, "app/(app)/layout.tsx"), "utf8")
    assert.match(layout, /OfflineSyncProvider/)
    const modal = readFileSync(
      join(root, "components/offline-prep-modal.tsx"),
      "utf8",
    )
    assert.match(modal, /Preparando tu recorrido/)
    assert.match(modal, /Entendido/)
  })

  it("backend list clientes expone picking_id/version", () => {
    const py = readFileSync(
      join(
        root,
        "../../backend/services/distribuidora/dispatch_plan_pwa_driver_read_service.py",
      ),
      "utf8",
    )
    assert.match(py, /"picking_id": bundle\.get\("picking_id"\)/)
    assert.match(py, /"picking_version": bundle\.get\("picking_version"\)/)
  })
})
