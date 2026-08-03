import { describe, expect, it, vi, beforeEach, afterEach } from "vitest"
import fs from "node:fs"
import path from "node:path"

import {
  buildCostV2Query,
  getCostV2Products,
  getCostV2ProductsSummary,
  mergeProductsByVariantId,
} from "./api"
import {
  formatMoneyCLPTable,
  formatPercentCL,
  displayCorrectedGross,
} from "./format"
import { FORBIDDEN_AGGREGATE_PHRASES } from "./labels"

vi.mock("@/lib/api-base", () => ({
  getApiBaseUrl: () => "https://api.test.local",
}))

describe("E.7.1 format", () => {
  it("table money has no decimals", () => {
    expect(formatMoneyCLPTable("3477.7400")).toBe("$3.478")
    expect(formatMoneyCLPTable("151")).toBe("$151")
    expect(displayCorrectedGross(null)).toBe("No calculable")
  })

  it("percent max one decimal", () => {
    expect(formatPercentCL("39.5000")).toBe("39,5 %")
    expect(formatPercentCL("19")).toBe("19 %")
    expect(formatPercentCL("-8.21")).toBe("-8,2 %")
  })
})

describe("E.7.1 products API client", () => {
  const originalFetch = globalThis.fetch
  beforeEach(() => {
    globalThis.fetch = vi.fn()
    vi.stubGlobal("window", {
      localStorage: { getItem: () => "jwt" },
    })
  })
  afterEach(() => {
    globalThis.fetch = originalFetch
  })

  it("calls products and products-summary without OFFSET", async () => {
    ;(globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue({
      ok: true,
      json: async () => ({
        items: [],
        page: { limit: 50, has_more: false, next_cursor: null },
        meta: { data_source: "x", calculation_version: "cost-v2.0.0" },
      }),
    })
    await getCostV2Products({
      company_id: 3,
      office_id: 3,
      date_from: "2026-06-01",
      date_to: "2026-07-31",
      sort: "pct_increase",
    })
    const url = String((globalThis.fetch as ReturnType<typeof vi.fn>).mock.calls[0][0])
    expect(url).toContain("/cost-analytics/v2/products?")
    expect(url).toContain("sort=pct_increase")
    expect(url).not.toContain("offset=")

    ;(globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue({
      ok: true,
      json: async () => ({
        summary: { total_products: 1 },
        meta: { data_source: "x", calculation_version: "cost-v2.0.0" },
      }),
    })
    await getCostV2ProductsSummary({
      company_id: 3,
      office_id: 3,
      date_from: "2026-06-01",
      date_to: "2026-07-31",
      change_threshold_percent: 10,
    })
    const url2 = String((globalThis.fetch as ReturnType<typeof vi.fn>).mock.calls[1][0])
    expect(url2).toContain("/cost-analytics/v2/products-summary?")
    expect(url2).toContain("change_threshold_percent=10")
  })

  it("merges products by variant_id", () => {
    expect(
      mergeProductsByVariantId([{ variant_id: 1 }, { variant_id: 2 }], [
        { variant_id: 2 },
        { variant_id: 3 },
      ]).map((x) => x.variant_id),
    ).toEqual([1, 2, 3])
  })
})

describe("E.7.1 UI scaffolding", () => {
  const root = path.resolve(__dirname, "../..")

  it("default tab resumen, four KPIs, no donut", () => {
    const page = fs.readFileSync(
      path.join(root, "app/(dashboard)/costos-v2/page.tsx"),
      "utf8",
    )
    expect(page).toContain('useState("resumen")')
    expect(page).toContain("CostV2ControlKpis")
    expect(page).toContain("Control de costos")
    expect(page).not.toContain("PieChart")
    expect(page).not.toContain("CostV2StatusChart")
    expect(page).toContain("Recepciones")
    expect(page).toContain("Productos")
    for (const p of FORBIDDEN_AGGREGATE_PHRASES) {
      expect(page.toLowerCase()).not.toContain(p)
    }
  })

  it("products table has at most 9 columns", () => {
    const table = fs.readFileSync(
      path.join(root, "components/costos-v2/cost-v2-products-table.tsx"),
      "utf8",
    )
    const heads = (table.match(/<TableHead[\s>]/g) || []).length
    expect(heads).toBeGreaterThan(0)
    expect(heads).toBeLessThanOrEqual(9)
    expect(table).toContain("COST_V2_PRODUCT_TABLE_COLUMNS")
  })

  it("legacy /costos intact", () => {
    const legacy = fs.readFileSync(
      path.join(root, "app/(dashboard)/costos/page.tsx"),
      "utf8",
    )
    expect(legacy).not.toContain("getCostV2Products")
    const sidebar = fs.readFileSync(
      path.join(root, "components/layout/sidebar.tsx"),
      "utf8",
    )
    expect(sidebar).toContain('href: "/costos"')
    expect(sidebar).not.toContain('href: "/costos-v2"')
  })

  it("filters use stacked rows not seven-in-one-line", () => {
    const filters = fs.readFileSync(
      path.join(root, "components/costos-v2/cost-v2-filters.tsx"),
      "utf8",
    )
    expect(filters).toContain("Más filtros")
    expect(filters).toContain("Collapsible")
    expect(filters).toContain("lg:grid-cols-5")
  })
})

describe("query helpers", () => {
  it("requires company and office", () => {
    const qs = buildCostV2Query({
      company_id: 3,
      office_id: 3,
      date_from: "2026-06-01",
      date_to: "2026-07-01",
    })
    expect(qs.get("company_id")).toBe("3")
    expect(qs.get("office_id")).toBe("3")
  })
})
