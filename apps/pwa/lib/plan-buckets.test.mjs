import assert from "node:assert/strict"
import { describe, it } from "node:test"
import {
  bucketPlanDate,
  partitionPlansByDate,
  planDateYmd,
} from "./plan-buckets.ts"

describe("plan-buckets America/Santiago", () => {
  it("normaliza planning_date", () => {
    assert.equal(planDateYmd("2026-11-10"), "2026-11-10")
    assert.equal(planDateYmd("2026-11-10T15:00:00Z"), "2026-11-10")
    assert.equal(planDateYmd(null), null)
  })

  it("clasifica hoy / próximos / historial", () => {
    const today = "2026-11-10"
    assert.equal(bucketPlanDate("2026-11-10", today), "today")
    assert.equal(bucketPlanDate("2026-11-11", today), "upcoming")
    assert.equal(bucketPlanDate("2026-11-09", today), "history")
  })

  it("particiona sin promover historial a hoy", () => {
    const items = [
      { id: 1, planning_date: "2026-11-09" },
      { id: 2, planning_date: "2026-11-10" },
      { id: 3, planning_date: "2026-11-12" },
      { id: 4, planning_date: "2026-11-10" },
    ]
    const p = partitionPlansByDate(items, "2026-11-10")
    assert.deepEqual(
      p.today.map((x) => x.id).sort(),
      [2, 4],
    )
    assert.deepEqual(
      p.upcoming.map((x) => x.id),
      [3],
    )
    assert.deepEqual(
      p.history.map((x) => x.id),
      [1],
    )
  })
})
