import { describe, expect, it, vi, beforeEach, afterEach } from "vitest"

import {
  buildCostV2Query,
  CostV2ApiError,
  getCostV2ReceptionDetail,
  getCostV2Receptions,
  getCostV2Summary,
  mergeReceptionItemsByHistoryId,
} from "./api"
import {
  additionalTaxCategoryLabel,
  displayCorrectedGross,
  displayUnitDifference,
  explanationForStatus,
  formatDateCL,
  formatDecimalMoneyCLP,
} from "./format"
import {
  FORBIDDEN_AGGREGATE_PHRASES,
  statusLabel,
  warningLabel,
  buildStatusChartData,
} from "./labels"
import { COST_V2_MAX_LIMIT } from "./types"

vi.mock("@/lib/api-base", () => ({
  getApiBaseUrl: () => "https://api.test.local",
}))

describe("costos-v2 format", () => {
  it("formats decimal money without inventing zero for null", () => {
    expect(formatDecimalMoneyCLP(null)).toBe("—")
    expect(formatDecimalMoneyCLP("1190.5")).toBe("$1.190,5")
    expect(formatDecimalMoneyCLP("1000000")).toBe("$1.000.000")
  })

  it("null corrected is No calculable, never $0", () => {
    expect(displayCorrectedGross(null)).toBe("No calculable")
    expect(displayCorrectedGross("0")).toBe("$0")
  })

  it("unit difference is dash when stored gross missing", () => {
    expect(
      displayUnitDifference({
        stored_cost_gross: null,
        unit_difference: "10",
      }),
    ).toBe("—")
    expect(
      displayUnitDifference({
        stored_cost_gross: "100",
        unit_difference: "12.5",
      }),
    ).toBe("$12,5")
  })

  it("formats chilean dates", () => {
    expect(formatDateCL("2026-06-22")).toBe("22-06-2026")
  })

  it("explains missing_cost and incomplete", () => {
    expect(explanationForStatus("missing_cost")).toMatch(/costo neto/)
    expect(explanationForStatus("incomplete_tax_context")).toMatch(/contexto tributario/)
  })

  it("labels flour/meat advance as anticipo, not IVA adicional", () => {
    expect(additionalTaxCategoryLabel("iva_advance")).toBe("Anticipo tributario")
    expect(additionalTaxCategoryLabel("iva_advance").toLowerCase()).not.toContain(
      "iva adicional",
    )
  })
})

describe("costos-v2 labels", () => {
  it("translates status and warning", () => {
    expect(statusLabel("missing_taxes_in_gross")).toBe("Impuestos no incluidos")
    expect(statusLabel("incomplete_tax_context")).toBe(
      "Contexto tributario incompleto",
    )
    expect(statusLabel("missing_cost")).toBe("Costo faltante")
    expect(statusLabel("valid_gross")).toBe("Costo válido")
    expect(statusLabel("gross_component_mismatch")).toBe("Componentes no coinciden")
    expect(statusLabel("duplicated_taxes_in_gross")).toBe("Posible impuesto duplicado")
    expect(warningLabel("suspicious_outlier")).toBe("Costo atípico")
  })

  it("outlier warning does not replace status label", () => {
    expect(statusLabel("valid_gross")).toBe("Costo válido")
    expect(warningLabel("suspicious_outlier")).not.toBe(statusLabel("valid_gross"))
  })

  it("does not use forbidden aggregate phrases in labels", () => {
    const blob = JSON.stringify({
      status: statusLabel("valid_gross"),
      warn: warningLabel("suspicious_outlier"),
    }).toLowerCase()
    for (const phrase of FORBIDDEN_AGGREGATE_PHRASES) {
      expect(blob).not.toContain(phrase)
    }
  })

  it("builds status chart including known keys", () => {
    const data = buildStatusChartData({
      valid_gross: 10,
      missing_cost: 2,
      weird: 1,
    })
    expect(data.find((d) => d.status === "valid_gross")?.count).toBe(10)
    expect(data.find((d) => d.status === "weird")?.count).toBe(1)
  })
})

describe("costos-v2 query + merge", () => {
  it("sends company_id, office_id and dates; omits empty filters", () => {
    const qs = buildCostV2Query({
      company_id: 3,
      office_id: 3,
      date_from: "2026-06-01",
      date_to: "2026-07-31",
      status: "",
      warning: null,
      barcode: "7803473005960",
      search: "  ",
      limit: 50,
    })
    expect(qs.get("company_id")).toBe("3")
    expect(qs.get("office_id")).toBe("3")
    expect(qs.get("date_from")).toBe("2026-06-01")
    expect(qs.get("date_to")).toBe("2026-07-31")
    expect(qs.get("barcode")).toBe("7803473005960")
    expect(qs.get("status")).toBeNull()
    expect(qs.get("search")).toBeNull()
    expect(qs.has("offset")).toBe(false)
  })

  it("caps limit at 200", () => {
    const qs = buildCostV2Query({
      company_id: 3,
      office_id: 3,
      limit: 999,
    })
    expect(qs.get("limit")).toBe(String(COST_V2_MAX_LIMIT))
  })

  it("treats barcode as text", () => {
    const qs = buildCostV2Query({
      company_id: 3,
      office_id: 3,
      barcode: "7803473005960",
    })
    expect(qs.get("barcode")).toBe("7803473005960")
  })

  it("merges without duplicate history_id", () => {
    const a = [{ history_id: 1 }, { history_id: 2 }]
    const b = [{ history_id: 2 }, { history_id: 3 }]
    expect(mergeReceptionItemsByHistoryId(a, b).map((x) => x.history_id)).toEqual([
      1, 2, 3,
    ])
  })
})

describe("costos-v2 API client", () => {
  const originalFetch = globalThis.fetch

  beforeEach(() => {
    globalThis.fetch = vi.fn()
  })

  afterEach(() => {
    globalThis.fetch = originalFetch
  })

  it("calls summary and receptions V2 with auth header", async () => {
    vi.stubGlobal("window", {
      localStorage: {
        getItem: (k: string) => (k === "token" ? "test-jwt" : null),
      },
    })
    ;(globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue({
      ok: true,
      json: async () => ({
        summary: {
          total_rows: 2,
          unique_variants: 1,
          unique_documents: 1,
          by_status: { valid_gross: 2 },
          by_warning: {},
          with_corrected_gross: 2,
          without_corrected_gross: 0,
          min_admission_date: null,
          max_admission_date: null,
          status_sum_matches_total: true,
        },
        meta: { data_source: "x", calculation_version: "cost-v2.0.0" },
      }),
    })

    await getCostV2Summary({
      company_id: 3,
      office_id: 3,
      date_from: "2026-06-01",
      date_to: "2026-07-01",
    })

    const [url, init] = (globalThis.fetch as ReturnType<typeof vi.fn>).mock.calls[0]
    expect(String(url)).toContain("/cost-analytics/v2/summary?")
    expect(String(url)).toContain("company_id=3")
    expect(String(url)).toContain("office_id=3")
    expect(String(url)).not.toContain("offset=")
    expect((init as RequestInit).headers).toMatchObject({
      Authorization: "Bearer test-jwt",
    })
  })

  it("calls receptions with next_cursor for load more (no OFFSET)", async () => {
    ;(globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue({
      ok: true,
      json: async () => ({
        items: [],
        page: { limit: 50, has_more: false, next_cursor: null },
        meta: { data_source: "x", calculation_version: "cost-v2.0.0" },
      }),
    })

    await getCostV2Receptions({
      company_id: 3,
      office_id: 3,
      date_from: "2026-06-01",
      date_to: "2026-07-01",
      cursor: "abc123",
    })

    const url = String((globalThis.fetch as ReturnType<typeof vi.fn>).mock.calls[0][0])
    expect(url).toContain("/cost-analytics/v2/receptions?")
    expect(url).toContain("cursor=abc123")
    expect(url).not.toContain("offset=")
  })

  it("detail hits /v2/receptions/{history_id}", async () => {
    ;(globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue({
      ok: true,
      json: async () => ({
        item: { history_id: 23190 },
        meta: { data_source: "x", calculation_version: "cost-v2.0.0" },
      }),
    })

    await getCostV2ReceptionDetail({
      company_id: 3,
      office_id: 3,
      history_id: 23190,
    })

    const url = String((globalThis.fetch as ReturnType<typeof vi.fn>).mock.calls[0][0])
    expect(url).toContain("/cost-analytics/v2/receptions/23190?")
  })

  it("maps 401/403/422 to friendly errors without SQL", async () => {
    ;(globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue({
      ok: false,
      status: 401,
      text: async () => "traceback SQL SELECT * FROM",
    })
    await expect(
      getCostV2Summary({
        company_id: 3,
        office_id: 3,
        date_from: "2026-06-01",
        date_to: "2026-07-01",
      }),
    ).rejects.toMatchObject({
      status: 401,
      message: expect.stringMatching(/sesión/i),
    })
  })

  it("passes AbortSignal and does not embed tokens in URL", async () => {
    const ac = new AbortController()
    ;(globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue({
      ok: true,
      json: async () => ({
        items: [],
        page: { limit: 50, has_more: false, next_cursor: null },
        meta: { data_source: "x", calculation_version: "cost-v2.0.0" },
      }),
    })

    await getCostV2Receptions({
      company_id: 3,
      office_id: 3,
      date_from: "2026-06-01",
      date_to: "2026-07-01",
      signal: ac.signal,
    })

    const [url, init] = (globalThis.fetch as ReturnType<typeof vi.fn>).mock.calls[0]
    expect(String(url)).not.toMatch(/Bearer|BSALE|token=/i)
    expect((init as RequestInit).signal).toBe(ac.signal)
  })

  it("does not call Bsale endpoints", async () => {
    ;(globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue({
      ok: true,
      json: async () => ({
        items: [],
        page: { limit: 50, has_more: false, next_cursor: null },
        meta: { data_source: "x", calculation_version: "cost-v2.0.0" },
      }),
    })
    await getCostV2Receptions({
      company_id: 3,
      office_id: 3,
      date_from: "2026-06-01",
      date_to: "2026-07-01",
    })
    const url = String((globalThis.fetch as ReturnType<typeof vi.fn>).mock.calls[0][0])
    expect(url).not.toContain("bsale")
    expect(url).not.toContain("/stocks/receptions")
  })

  it("CostV2ApiError exposes status", () => {
    const err = new CostV2ApiError(404, "nope")
    expect(err.status).toBe(404)
  })
})

describe("legacy intactness markers", () => {
  it("V2 module does not import legacy adapt-cost-analytics", async () => {
    const fs = await import("node:fs")
    const path = await import("node:path")
    const root = path.resolve(__dirname)
    for (const file of ["api.ts", "format.ts", "labels.ts", "types.ts"]) {
      const src = fs.readFileSync(path.join(root, file), "utf8")
      expect(src).not.toContain("adapt-cost-analytics")
      expect(src).not.toContain("syncCostAnalytics")
      expect(src).not.toContain("variant_cost")
    }
  })
})
