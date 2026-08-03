import { describe, expect, it } from "vitest"
import fs from "node:fs"
import path from "node:path"

const root = path.resolve(__dirname, "../..")

describe("costos-v2 route scaffolding", () => {
  it("renders route module at /costos-v2", () => {
    const page = path.join(root, "app/(dashboard)/costos-v2/page.tsx")
    expect(fs.existsSync(page)).toBe(true)
    const src = fs.readFileSync(page, "utf8")
    expect(src).toContain("Costos V2 — Vista de validación")
    expect(src).toContain("getCostV2Summary")
    expect(src).toContain("getCostV2Receptions")
    expect(src).toContain("Cargar más")
    expect(src).toContain("AbortController")
    expect(src).not.toContain("impacto total")
    expect(src).not.toContain("OFFSET")
    expect(src).not.toContain("syncCostAnalytics")
  })

  it("does not change legacy /costos link in sidebar", () => {
    const sidebar = fs.readFileSync(
      path.join(root, "components/layout/sidebar.tsx"),
      "utf8",
    )
    expect(sidebar).toContain('href: "/costos"')
    expect(sidebar).not.toContain('href: "/costos-v2"')
  })

  it("legacy costos page still present and not importing V2 API", () => {
    const legacy = fs.readFileSync(
      path.join(root, "app/(dashboard)/costos/page.tsx"),
      "utf8",
    )
    expect(legacy).toContain("CostosPage")
    expect(legacy).not.toContain("costos-v2")
    expect(legacy).not.toContain("getCostV2")
  })

  it("fingerprints section is collapsible in detail drawer", () => {
    const drawer = fs.readFileSync(
      path.join(root, "components/costos-v2/cost-v2-detail-drawer.tsx"),
      "utf8",
    )
    expect(drawer).toContain("Accordion")
    expect(drawer).toContain("source_history_fingerprint")
    expect(drawer).toContain("additionalTaxCategoryLabel")
    expect(drawer).toContain("displayCorrectedGross")
    expect(drawer.toLowerCase()).not.toContain("iva adicional")
    expect(drawer).toContain("collapsible")
  })
})
