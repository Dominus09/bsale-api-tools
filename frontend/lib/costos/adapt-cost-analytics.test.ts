import { describe, expect, it } from "vitest"

import type { CostHistoryRow } from "../api"
import {
  buildVariantAuditRows,
  inferGrossCostQuality,
  inferTaxBreakdownQuality,
  previousGrossFromVariation,
} from "./adapt-cost-analytics"
import { GROSS_COST_QUALITY_LABEL } from "./quality-labels"

describe("costos adapter quality", () => {
  it("prefers cost_bruto_erp as actual purchase gross", () => {
    expect(
      inferGrossCostQuality({
        cost_bruto_erp: 1395,
        cost_net: 1000,
        iva_amount: 190,
        other_taxes: 205,
      }),
    ).toBe("actual_purchase_gross")
  })

  it("marks aggregated other taxes without claiming ILA", () => {
    expect(
      inferTaxBreakdownQuality({
        iva_amount: 190,
        other_taxes: 205,
        cost_bruto_erp: 1395,
      }),
    ).toBe("aggregated_other_taxes")
  })

  it("does not invent previous gross without variation", () => {
    expect(previousGrossFromVariation(1395, null)).toEqual({
      previous: null,
      delta: null,
    })
  })

  it("exposes human labels not technical codes", () => {
    expect(GROSS_COST_QUALITY_LABEL.actual_purchase_gross).toBe(
      "Bruto real de compra",
    )
  })

  it("audits min/max/previous; excludes NC from max valid", () => {
    const rows: CostHistoryRow[] = [
      {
        company_id: 3,
        variant_id: 1,
        reception_id: 1,
        reception_detail_id: 1,
        admission_date: "2026-06-01T00:00:00Z",
        quantity: 1,
        cost_net: 1000,
        cost_bruto_erp: 1190,
        reception_type: "recepcion_normal",
      },
      {
        company_id: 3,
        variant_id: 1,
        reception_id: 2,
        reception_detail_id: 2,
        admission_date: "2026-07-01T00:00:00Z",
        quantity: 1,
        cost_net: 1000,
        cost_bruto_erp: 1395,
        reception_type: "recepcion_normal",
      },
      {
        company_id: 3,
        variant_id: 1,
        reception_id: 3,
        reception_detail_id: 3,
        admission_date: "2026-07-10T00:00:00Z",
        quantity: 1,
        cost_net: 1000,
        cost_bruto_erp: 9999,
        reception_type: "recepcion_nc",
      },
    ]
    const audit = buildVariantAuditRows(rows, new Date("2026-07-20"))
    expect(audit).toHaveLength(1)
    expect(audit[0]!.lastGrossCost).toBe(9999)
    expect(audit[0]!.maxValidGrossCost).toBe(1395)
    expect(audit[0]!.minValidGrossCost).toBe(1190)
    expect(audit[0]!.previousCostGross).toBe(1395)
  })
})
