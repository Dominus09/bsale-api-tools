import { describe, expect, it } from "vitest"

import {
  dispatchPlanOperationalLabel,
  dispatchPlanOperationalPhase,
  dispatchPlanVersionLabel,
} from "@/lib/dispatch-plan-operational-status"

describe("dispatchPlanOperationalPhase", () => {
  it("maps open statuses", () => {
    expect(dispatchPlanOperationalPhase("planned")).toBe("abierto")
    expect(dispatchPlanOperationalPhase("invoicing")).toBe("abierto")
  })

  it("maps operational milestones", () => {
    expect(dispatchPlanOperationalPhase("picking_generated")).toBe("pickings_generados")
    expect(dispatchPlanOperationalPhase("closed")).toBe("cerrado")
    expect(dispatchPlanOperationalPhase("dispatched")).toBe("despachado")
    expect(dispatchPlanOperationalPhase("squared")).toBe("cuadrado")
  })
})

describe("dispatchPlanOperationalLabel", () => {
  it("returns Spanish labels", () => {
    expect(dispatchPlanOperationalLabel("picking_generated")).toBe("Pickings generados")
    expect(dispatchPlanOperationalLabel("squared")).toBe("Cuadrado")
  })
})

describe("dispatchPlanVersionLabel", () => {
  it("appends picking version", () => {
    expect(dispatchPlanVersionLabel("PLAN-00008", 8, 2)).toBe("PLAN-00008 v2")
    expect(dispatchPlanVersionLabel(null, 8, null)).toBe("PLAN-8")
  })
})
