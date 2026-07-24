import { describe, expect, it } from "vitest"

import {
  actualMarkupPct,
  grossMarginPct,
  markupToMarginOnPricePct,
  recommendedGrossPrice,
  resolvePricePolicyStatus,
} from "./price-policy"

describe("price-policy markup semantics", () => {
  it("1-4: costo 10000 precio 12500 → recargo 25% margen 20%", () => {
    expect(actualMarkupPct(12500, 10000)).toBe(25)
    expect(grossMarginPct(12500, 10000)).toBe(20)
  })

  it("5: regla 22-30 → within_policy", () => {
    expect(
      resolvePricePolicyStatus({
        grossPrice: 12500,
        grossCost: 10000,
        minMarkupPct: 22,
        maxMarkupPct: 30,
        hasRule: true,
      }),
    ).toBe("within_policy")
  })

  it("6: below_minimum", () => {
    expect(
      resolvePricePolicyStatus({
        grossPrice: 11000,
        grossCost: 10000,
        minMarkupPct: 22,
        maxMarkupPct: 30,
        hasRule: true,
      }),
    ).toBe("below_minimum")
  })

  it("7: above_maximum", () => {
    expect(
      resolvePricePolicyStatus({
        grossPrice: 14000,
        grossCost: 10000,
        minMarkupPct: 22,
        maxMarkupPct: 30,
        hasRule: true,
      }),
    ).toBe("above_maximum")
  })

  it("8: missing_cost", () => {
    expect(
      resolvePricePolicyStatus({
        grossPrice: 12500,
        grossCost: null,
        minMarkupPct: 22,
        maxMarkupPct: 30,
        hasRule: true,
      }),
    ).toBe("missing_cost")
  })

  it("9: missing_price", () => {
    expect(
      resolvePricePolicyStatus({
        grossPrice: null,
        grossCost: 10000,
        minMarkupPct: 22,
        maxMarkupPct: 30,
        hasRule: true,
      }),
    ).toBe("missing_price")
  })

  it("10: missing_rule", () => {
    expect(
      resolvePricePolicyStatus({
        grossPrice: 12500,
        grossCost: 10000,
        minMarkupPct: null,
        maxMarkupPct: null,
        hasRule: false,
      }),
    ).toBe("missing_rule")
  })

  it("11-14: recommended prices from markup", () => {
    expect(recommendedGrossPrice(10000, 22)).toBe(12200)
    expect(recommendedGrossPrice(10000, 30)).toBe(13000)
  })

  it("12: cost_outlier priority over within_policy", () => {
    expect(
      resolvePricePolicyStatus({
        grossPrice: 12500,
        grossCost: 10000,
        minMarkupPct: 22,
        maxMarkupPct: 30,
        hasRule: true,
        isOutlier: true,
      }),
    ).toBe("cost_outlier")
  })

  it("16: never uses sold units (pure price/cost)", () => {
    expect(markupToMarginOnPricePct(25)?.toFixed(2)).toBe("20.00")
    expect(actualMarkupPct.length).toBe(2)
  })
})
