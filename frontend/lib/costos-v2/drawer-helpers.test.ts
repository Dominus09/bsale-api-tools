import { describe, expect, it } from "vitest"

import {
  displayAdditionalTaxTitle,
  formatMoneyCLPTable,
  formatPercentCL,
  formatTaxRate,
} from "./format"
import {
  COST_V2_SCOPE_NOTE_DRAWER,
  statusDrawerDescription,
  statusSuggestedAction,
} from "./labels"

describe("costos-v2 drawer helpers", () => {
  it("operational money has no decimals", () => {
    expect(formatMoneyCLPTable("9797.55")).toBe("$9.798")
    expect(formatMoneyCLPTable("2050.65")).toBe("$2.051")
    expect(formatMoneyCLPTable("0")).toBe("$0")
  })

  it("tax rates show one decimal max", () => {
    expect(formatTaxRate("0.315")).toBe("31,5 %")
    expect(formatPercentCL("25.12")).toBe("25,1 %")
  })

  it("additional tax titles never say IVA adicional", () => {
    expect(
      displayAdditionalTaxTitle({
        name: "ILA destilados",
        rate: "0.315",
        category: "ila",
      }),
    ).toBe("ILA destilados 31,5 %")
    expect(
      displayAdditionalTaxTitle({
        name: "IVA adicional harina",
        rate: "0.12",
        category: "iva_advance",
      }),
    ).toMatch(/^Anticipo harina/)
    expect(
      displayAdditionalTaxTitle({
        name: null,
        rate: "0.05",
        category: "meat_advance",
      }),
    ).toMatch(/Anticipo carne/)
    expect(
      displayAdditionalTaxTitle({
        name: "IVA adicional harina",
        rate: "0.12",
        category: "iva_advance",
      }),
    ).not.toMatch(/IVA adicional/i)
  })

  it("status drawer copy and suggested action", () => {
    expect(statusDrawerDescription("missing_taxes_in_gross")).toContain(
      "bruto corregido",
    )
    expect(statusSuggestedAction("missing_taxes_in_gross")).toBe(
      "Usar el costo corregido V2 como referencia.",
    )
    expect(statusSuggestedAction("incomplete_tax_context")).toBe(
      "Revisar la configuración tributaria del producto.",
    )
  })

  it("scope note has no technical ids", () => {
    expect(COST_V2_SCOPE_NOTE_DRAWER).toContain("La Quillotana SpA")
    expect(COST_V2_SCOPE_NOTE_DRAWER).not.toContain("company_id")
    expect(COST_V2_SCOPE_NOTE_DRAWER).not.toContain("office_id")
    expect(COST_V2_SCOPE_NOTE_DRAWER).not.toMatch(/\b3\b/)
  })
})
