import { describe, expect, it } from "vitest"

import {
  orderWeightToPlanningPatch,
  summarizeGroupWeights,
} from "@/lib/pre-despacho-weight"
import { summarizeAssignedKgFromOrders } from "@/lib/ors-truck-capacity"

describe("pre-despacho weight contract", () => {
  it("does not treat unavailable as 0 kg in group sum", () => {
    const summary = summarizeGroupWeights([
      { peso_total_kg: 100, weight: { value_kg: 100, status: "calculated" } },
      { peso_total_kg: null, weight: { value_kg: null, status: "unavailable" } },
      { peso_total_kg: 22.5, weight: { value_kg: 22.5, status: "partial" } },
    ])
    expect(summary.knownKg).toBe(122.5)
    expect(summary.unavailableCount).toBe(1)
    expect(summary.partialCount).toBe(1)
    expect(summary.incomplete).toBe(true)
  })

  it("planning patch keeps null when detail weight unavailable", () => {
    const patch = orderWeightToPlanningPatch({
      document_id: 1,
      oc: 68666,
      productos_totales: 42,
      productos_con_peso: 0,
      productos_sin_peso: 42,
      productos_manuales: 0,
      productos_estimados: 0,
      peso_total_kg: null,
      porcentaje_cobertura: 0,
      estado: "sin_peso",
      semaforo: "rojo",
      lines: [],
      weight: {
        value_kg: null,
        status: "unavailable",
        reason: "products_load_failed",
      },
    })
    expect(patch.peso_total_kg).toBeNull()
    expect(patch.weight_kg).toBeNull()
    expect(patch.weight?.status).toBe("unavailable")
  })

  it("truck capacity does not count unavailable as real zero", () => {
    const est = summarizeAssignedKgFromOrders([
      { weight_kg: 200, weight: { value_kg: 200, status: "calculated" } },
      { weight_kg: null, weight: { value_kg: null, status: "unavailable" } },
    ])
    expect(est.assignedKg).toBe(200)
    expect(est.knownKg).toBe(200)
    expect(est.unavailableCount).toBe(1)
    expect(est.incomplete).toBe(true)
    expect(est.usedStopProxy).toBe(false)
  })
})
