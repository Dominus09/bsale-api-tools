import assert from "node:assert/strict"
import { describe, it } from "node:test"
import { resolveVisitRecovery } from "./visit-recovery.ts"

describe("resolveVisitRecovery Romeo Alfa dual-visit", () => {
  const closed = {
    id: 1,
    status: "partial",
    closure_review_status: "verified",
    version: 3,
  }
  const active = {
    id: 3,
    status: "in_progress",
    version: 2,
  }

  it("permite retomar activa y ver resultado histórico a la vez", () => {
    const r = resolveVisitRecovery({
      activeVisit: active,
      latestVisit: active,
      lastClosedVisit: closed,
    })
    assert.equal(r.canResumeActive, true)
    assert.equal(r.canViewClosedResult, true)
    assert.equal(r.active.id, 3)
    assert.equal(r.lastClosed.id, 1)
    assert.equal(r.preferredResultVisitId, 1)
    assert.equal(r.requiresExplicitNewVisit, false)
  })

  it("no oculta cerrada aunque latest sea la activa", () => {
    const r = resolveVisitRecovery({
      activeVisit: active,
      latestVisit: active,
      lastClosedVisit: closed,
    })
    assert.notEqual(r.lastClosed?.id, r.active?.id)
  })

  it("sin activa: muestra resultado y exige nueva visita explícita", () => {
    const r = resolveVisitRecovery({
      activeVisit: null,
      latestVisit: closed,
      lastClosedVisit: closed,
    })
    assert.equal(r.canResumeActive, false)
    assert.equal(r.canViewClosedResult, true)
    assert.equal(r.requiresExplicitNewVisit, true)
  })
})
