import { describe, expect, it } from "vitest"
import fs from "node:fs"
import path from "node:path"

const root = path.resolve(__dirname, "../../..")

describe("control de costos route (/costos)", () => {
  it("renders consolidated module on /costos", () => {
    const page = fs.readFileSync(
      path.join(root, "app/(dashboard)/costos/page.tsx"),
      "utf8",
    )
    expect(page).toContain("Control de costos")
    expect(page).toContain("getCostV2CompanyProducts")
    expect(page).toContain("getCostV2CompanySummary")
    expect(page).toContain("CostCompanyFilters")
    expect(page).toContain("CostControlKpis")
    expect(page).toContain("CostProductsTable")
    expect(page).toContain("CostProductDetailDrawer")
    expect(page).toContain("CostSymbologyPanel")
    expect(page).not.toContain("CostMainTable")
    expect(page).not.toContain("adapt-cost-analytics")
    expect(page).not.toContain("Costos V2")
    expect(page).not.toContain("/costos-v2")
    expect(page).not.toContain("Vista de validación")
  })

  it("costos-v2 route no longer exists and has no redirect stub", () => {
    const v2Page = path.join(root, "app/(dashboard)/costos-v2/page.tsx")
    const v2Dir = path.join(root, "app/(dashboard)/costos-v2")
    expect(fs.existsSync(v2Page)).toBe(false)
    expect(fs.existsSync(v2Dir)).toBe(false)
  })

  it("sidebar points only to /costos", () => {
    const sidebar = fs.readFileSync(
      path.join(root, "components/layout/sidebar.tsx"),
      "utf8",
    )
    expect(sidebar).toContain('href: "/costos"')
    expect(sidebar).not.toContain("/costos-v2")
  })

  it("no costos-v2 folders remain under components/lib", () => {
    expect(fs.existsSync(path.join(root, "components/costos-v2"))).toBe(false)
    expect(fs.existsSync(path.join(root, "lib/costos-v2"))).toBe(false)
  })

  it("products table has at most 7 columns and no costo anterior", () => {
    const table = fs.readFileSync(
      path.join(root, "components/costos/cost-products-table.tsx"),
      "utf8",
    )
    expect(table).toContain("COST_V2_PRODUCT_TABLE_COLUMNS")
    expect(table).toContain("Costo vigente")
    expect(table).toContain("Cambio")
    expect(table).not.toContain(">Costo anterior<")
    expect(table).not.toContain("costos-v2")
  })

  it("drawer uses company APIs and office/history tabs", () => {
    const drawer = fs.readFileSync(
      path.join(root, "components/costos/cost-product-detail-drawer.tsx"),
      "utf8",
    )
    expect(drawer).toContain("getCostV2CompanyProduct")
    expect(drawer).toContain("getCostV2CompanyProductHistory")
    expect(drawer).toContain("Historial de costos")
    expect(drawer).toContain("Oficinas")
    expect(drawer).toContain("Detalle técnico")
    expect(drawer).not.toContain("@/components/costos-v2")
  })

  it("legacy shared format still used by margins", () => {
    const margins = fs.readFileSync(
      path.join(root, "components/margins/price-control-table.tsx"),
      "utf8",
    )
    expect(margins).toContain('@/lib/costos/format')
    expect(margins).toContain('@/lib/costos/quality-labels')
  })
})

describe("no visible costos-v2 links in frontend sources", () => {
  const scanRoots = [
    "app",
    "components",
    "lib",
  ]

  it("no /costos-v2 href or Costos V2 UI title in operational sources", () => {
    const hits: string[] = []
    for (const rel of scanRoots) {
      const dir = path.join(root, rel)
      if (!fs.existsSync(dir)) continue
      const walk = (d: string) => {
        for (const ent of fs.readdirSync(d, { withFileTypes: true })) {
          if (ent.name === "node_modules" || ent.name === "tmp") continue
          const p = path.join(d, ent.name)
          if (ent.isDirectory()) walk(p)
          else if (/\.(tsx?|jsx?)$/.test(ent.name) && !ent.name.includes(".test.")) {
            const src = fs.readFileSync(p, "utf8")
            if (src.includes("/costos-v2") || src.includes('href: "/costos-v2"')) {
              hits.push(path.relative(root, p))
            }
            if (/>\s*Costos V2\s*</.test(src) || src.includes('title: "Costos V2"')) {
              hits.push(`UI:${path.relative(root, p)}`)
            }
          }
        }
      }
      walk(dir)
    }
    expect(hits).toEqual([])
  })
})
