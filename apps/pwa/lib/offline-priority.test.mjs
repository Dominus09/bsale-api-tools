/**
 * Tests de política: reimplementación espejo mínima + asserts de fuente.
 * Evita importar .ts con deps extensionless (limitación node strip-types).
 */
import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { describe, it } from "node:test"
import { fileURLToPath } from "node:url"
import {
  bucketPlanDate,
  partitionPlansByDate,
} from "./plan-buckets.ts"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")
const src = readFileSync(join(root, "lib/offline-priority.ts"), "utf8")

const MAX_PROGRESS_PROBES = 6
const MAX_TODAY_IN_PROBES = 3
const MAX_HISTORY_IN_PROBES = 3

function planLikelyHasPicking(plan) {
  return typeof plan.document_count === "number" && plan.document_count > 0
}

function selectProgressProbeCandidates(plans, opts = {}) {
  const todayYmd = opts.todayYmd
  const maxProbes = Math.max(1, opts.maxProbes ?? MAX_PROGRESS_PROBES)
  const buckets = partitionPlansByDate(plans, todayYmd)
  const byId = new Map(plans.map((p) => [p.id, p]))
  const out = []
  const seen = new Set()
  function push(p) {
    if (!p || seen.has(p.id) || out.length >= maxProbes) return
    seen.add(p.id)
    out.push(p)
  }
  if (opts.activePlanId != null) push(byId.get(opts.activePlanId))
  for (const p of buckets.today.slice(0, MAX_TODAY_IN_PROBES)) push(p)
  const historyLikely = buckets.history.filter(planLikelyHasPicking)
  const historyFallback = historyLikely.length ? historyLikely : buckets.history
  for (const p of historyFallback.slice(0, MAX_HISTORY_IN_PROBES)) push(p)
  return out
}

function classifyPriorityTier(plan, signals, todayYmd) {
  if (!signals?.pickingReady) return "D"
  const bucket = bucketPlanDate(plan.planning_date, todayYmd)
  const inProgress = Math.max(0, signals.inProgress || 0)
  const remaining = Math.max(0, signals.remainingClients || 0)
  const pendingOps = remaining > 0 || inProgress > 0
  if (inProgress > 0) return "A"
  if (bucket === "today" && pendingOps) return "B"
  if ((bucket === "history" || bucket === "unknown") && pendingOps) return "C"
  return "D"
}

function resolvePriorityPlan(plans, signalsById, opts = {}) {
  const todayYmd = opts.todayYmd
  const probePlanIds = opts.probePlanIds ?? plans.map((p) => p.id)
  const activePlanId = opts.activePlanId ?? null
  const ranked = []
  for (const plan of plans) {
    if (!probePlanIds.includes(plan.id) && plan.id !== activePlanId) continue
    const signals = signalsById[plan.id]
    if (!signals) continue
    const tier = classifyPriorityTier(plan, signals, todayYmd)
    if (tier === "A" || tier === "B" || tier === "C") ranked.push({ plan, tier })
  }
  for (const tier of ["A", "B", "C"]) {
    const group = ranked.filter((r) => r.tier === tier).map((r) => r.plan)
    if (!group.length) continue
    if (group.length === 1) {
      return { tier, planId: group[0].id, needsExplicitSelection: false, sameTierPlans: group }
    }
    const preferred = activePlanId ? group.find((p) => p.id === activePlanId) : null
    if (preferred) {
      return { tier, planId: preferred.id, needsExplicitSelection: false, sameTierPlans: group }
    }
    return { tier, planId: null, needsExplicitSelection: true, sameTierPlans: group }
  }
  return { tier: "none", planId: null, needsExplicitSelection: false, sameTierPlans: [] }
}

function plan(id, date, docs = 0) {
  return { id, planning_date: date, document_count: docs }
}

describe("offline-priority selección inteligente", () => {
  const today = "2026-11-19"

  it("fuente exporta API de política y retención sin auto-delete", () => {
    assert.match(src, /export function resolvePriorityPlan/)
    assert.match(src, /export function selectProgressProbeCandidates/)
    assert.match(src, /MAX_PROGRESS_PROBES = 6/)
    assert.match(src, /autoDelete: false/)
  })

  it("A: visita en curso gana sobre hoy pendiente", () => {
    const d = resolvePriorityPlan(
      [plan(1, today, 5), plan(2, "2026-11-18", 4)],
      {
        1: { pickingReady: true, remainingClients: 3, inProgress: 0 },
        2: { pickingReady: true, remainingClients: 2, inProgress: 1 },
      },
      { todayYmd: today, probePlanIds: [1, 2] },
    )
    assert.equal(d.tier, "A")
    assert.equal(d.planId, 2)
  })

  it("B: despacho de hoy con picking y pendientes", () => {
    const d = resolvePriorityPlan(
      [plan(10, today, 8), plan(11, "2026-11-10", 3)],
      {
        10: { pickingReady: true, remainingClients: 4, inProgress: 0 },
        11: { pickingReady: true, remainingClients: 2, inProgress: 0 },
      },
      { todayYmd: today, probePlanIds: [10, 11] },
    )
    assert.equal(d.tier, "B")
    assert.equal(d.planId, 10)
  })

  it("C: anterior pendiente solo si no hay B", () => {
    const d = resolvePriorityPlan(
      [plan(20, "2026-11-17", 2)],
      { 20: { pickingReady: true, remainingClients: 1, inProgress: 0 } },
      { todayYmd: today, probePlanIds: [20] },
    )
    assert.equal(d.tier, "C")
    assert.equal(d.planId, 20)
  })

  it("sin picking → no auto", () => {
    assert.equal(
      classifyPriorityTier(
        plan(1, today, 0),
        { pickingReady: false, remainingClients: 5, inProgress: 0 },
        today,
      ),
      "D",
    )
  })

  it("dos activos mismo día → needsExplicitSelection", () => {
    const d = resolvePriorityPlan(
      [plan(1, today, 3), plan(2, today, 4)],
      {
        1: { pickingReady: true, remainingClients: 2, inProgress: 0 },
        2: { pickingReady: true, remainingClients: 1, inProgress: 0 },
      },
      { todayYmd: today, probePlanIds: [1, 2] },
    )
    assert.equal(d.needsExplicitSelection, true)
    assert.equal(d.planId, null)
  })

  it("empate resuelto por activePlanId", () => {
    const d = resolvePriorityPlan(
      [plan(1, today, 3), plan(2, today, 4)],
      {
        1: { pickingReady: true, remainingClients: 2, inProgress: 0 },
        2: { pickingReady: true, remainingClients: 1, inProgress: 0 },
      },
      { todayYmd: today, activePlanId: 2, probePlanIds: [1, 2] },
    )
    assert.equal(d.planId, 2)
  })

  it("100 históricos → máx. 6 probes", () => {
    const plans = []
    for (let i = 0; i < 100; i += 1) {
      const day = String(10 - (i % 9)).padStart(2, "0")
      plans.push(plan(1000 + i, `2026-11-${day}`, i % 3 === 0 ? 2 : 0))
    }
    plans.push(plan(1, today, 5))
    const probes = selectProgressProbeCandidates(plans, { todayYmd: today })
    assert.ok(probes.length <= MAX_PROGRESS_PROBES)
    assert.ok(probes.some((p) => p.id === 1))
  })
})
