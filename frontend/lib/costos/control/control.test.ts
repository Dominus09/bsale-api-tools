import { describe, expect, it } from "vitest"

import { formatChangeCell, formatMoneyCLPTable } from "./format"
import {
  BUSINESS_SITUATION_LABEL,
  statusLabel,
  SYMBOLOGY_SCOPE_NOTE,
} from "./labels"

describe("control labels and format", () => {
  it("spanish statuses and business situations", () => {
    expect(statusLabel("valid_gross")).toBe("Costo correcto")
    expect(BUSINESS_SITUATION_LABEL.partial_coverage).toBe("Cobertura parcial")
    expect(SYMBOLOGY_SCOPE_NOTE).toContain("Supermercado La Quillotana")
    expect(SYMBOLOGY_SCOPE_NOTE).not.toContain("company_id")
  })

  it("money and change cell formatting", () => {
    expect(formatMoneyCLPTable("9798")).toBe("$9.798")
    expect(
      formatChangeCell({
        amount: null,
        percent: null,
        hasComparable: false,
      }),
    ).toBe("Sin comparación")
    expect(
      formatChangeCell({
        amount: "0",
        percent: "0",
        hasComparable: true,
        visualNoChange: true,
      }),
    ).toBe("Sin cambio")
  })

  it("company API helpers exist without office_id requirement in source", async () => {
    const fs = await import("node:fs")
    const path = await import("node:path")
    const api = fs.readFileSync(path.join(__dirname, "api.ts"), "utf8")
    expect(api).toContain("buildCostV2CompanyQuery")
    expect(api).toContain("/cost-analytics/v2/company-products")
    expect(api).toContain("getCostV2CompanySummary")
    // company query builder must not force office_id
    const fn = api.slice(api.indexOf("export function buildCostV2CompanyQuery"))
    const body = fn.slice(0, fn.indexOf("export async function"))
    expect(body).not.toContain("office_id")
  })
})
