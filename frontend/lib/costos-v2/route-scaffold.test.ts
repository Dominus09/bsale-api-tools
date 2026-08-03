import { describe, expect, it } from "vitest"
import fs from "node:fs"
import path from "node:path"

const root = path.resolve(__dirname, "../..")

describe("costos-v2 route scaffolding E.7.3", () => {
  it("is product control center", () => {
    const page = path.join(root, "app/(dashboard)/costos-v2/page.tsx")
    expect(fs.existsSync(page)).toBe(true)
    const src = fs.readFileSync(page, "utf8")
    expect(src).toContain("Control de costos")
    expect(src).toContain("getCostV2CompanyProducts")
    expect(src).toContain("getCostV2CompanySummary")
    expect(src).toContain('value="resumen"')
    expect(src).not.toContain("impacto total")
    expect(src).not.toContain("PieChart")
  })

  it("product detail isolates technical data in dedicated tab", () => {
    const drawer = fs.readFileSync(
      path.join(root, "components/costos-v2/cost-v2-product-detail-drawer.tsx"),
      "utf8",
    )
    expect(drawer).toContain('value="tecnico"')
    expect(drawer).toContain("source_history_fingerprint")
    expect(drawer).toContain("getCostV2CompanyProductHistory")
    expect(drawer).toContain("Costo vigente")
    expect(drawer.toLowerCase()).not.toContain("iva adicional")
  })
})
