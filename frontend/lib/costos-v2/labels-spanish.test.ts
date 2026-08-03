import { describe, expect, it } from "vitest"
import fs from "node:fs"
import path from "node:path"

import {
  COST_V2_STATUS_LABEL,
  COST_V2_WARNING_LABEL,
  FILTER_STATUS_OPTIONS,
  FILTER_WARNING_OPTIONS,
  SYMBOLOGY_ALERTS,
  SYMBOLOGY_SCOPE_NOTE,
  SYMBOLOGY_STATUSES,
  containsTechnicalCode,
  statusLabel,
  warningLabel,
} from "./labels"

const root = path.resolve(__dirname, "../..")

describe("costos-v2 spanish labels", () => {
  it("maps statuses to Spanish", () => {
    expect(statusLabel("missing_taxes_in_gross")).toBe("Impuestos no incluidos")
    expect(statusLabel("incomplete_tax_context")).toBe(
      "Contexto tributario incompleto",
    )
    expect(statusLabel("missing_cost")).toBe("Costo faltante")
    expect(statusLabel("valid_gross")).toBe("Costo correcto")
    expect(statusLabel("duplicated_taxes_in_gross")).toBe(
      "Posible impuesto duplicado",
    )
    expect(statusLabel("gross_component_mismatch")).toBe("Descuadre en el costo")
  })

  it("maps warnings to Spanish", () => {
    expect(warningLabel("suspicious_outlier")).toBe("Costo atípico")
    expect(warningLabel("reception_tax_context_unavailable")).toBe(
      "Sin información tributaria suficiente",
    )
    expect(warningLabel("stored_components_rounding")).toBe("Diferencia de redondeo")
  })

  it("filter options keep technical values with Spanish labels", () => {
    const statusOpt = FILTER_STATUS_OPTIONS.find(
      (o) => o.value === "missing_taxes_in_gross",
    )
    expect(statusOpt?.label).toBe("Impuestos no incluidos")
    expect(statusOpt?.value).toBe("missing_taxes_in_gross")

    const warnOpt = FILTER_WARNING_OPTIONS.find(
      (o) => o.value === "suspicious_outlier",
    )
    expect(warnOpt?.label).toBe("Costo atípico")
    expect(warnOpt?.value).toBe("suspicious_outlier")
  })

  it("symbology content has descriptions and no company_id", () => {
    expect(SYMBOLOGY_STATUSES).toHaveLength(6)
    expect(SYMBOLOGY_ALERTS).toHaveLength(3)
    expect(SYMBOLOGY_SCOPE_NOTE).toContain("La Quillotana SpA")
    expect(SYMBOLOGY_SCOPE_NOTE).not.toContain("company_id")
    expect(SYMBOLOGY_SCOPE_NOTE).not.toContain("office_id")
    for (const s of SYMBOLOGY_STATUSES) {
      expect(s.description.length).toBeGreaterThan(20)
      expect(s.action.length).toBeGreaterThan(5)
      expect(s.label).toBe(COST_V2_STATUS_LABEL[s.code])
    }
  })
})

describe("costos-v2 UI no technical codes in main surfaces", () => {
  const mainFiles = [
    "app/(dashboard)/costos-v2/page.tsx",
    "components/costos-v2/cost-v2-filters.tsx",
    "components/costos-v2/cost-v2-control-kpis.tsx",
    "components/costos-v2/cost-v2-alerts-panel.tsx",
    "components/costos-v2/cost-v2-products-table.tsx",
    "components/costos-v2/cost-v2-symbology-panel.tsx",
  ]

  it("main UI files do not display raw technical codes as user text", () => {
    for (const rel of mainFiles) {
      const src = fs.readFileSync(path.join(root, rel), "utf8")
      // Allowed: value={technical} in SelectItem, filter draft assignments
      // Disallowed as visible copy: bare codes in JSX text content patterns
      expect(src).not.toMatch(/>\s*missing_taxes_in_gross\s*</)
      expect(src).not.toMatch(/>\s*suspicious_outlier\s*</)
      expect(src).not.toMatch(/>\s*Warning\s*</)
      expect(src).not.toContain("Costo válido") // replaced by Costo correcto
    }
  })

  it("filters use Alerta label and Spanish options", () => {
    const filters = fs.readFileSync(
      path.join(root, "components/costos-v2/cost-v2-filters.tsx"),
      "utf8",
    )
    expect(filters).toContain(">Alerta<")
    expect(filters).not.toContain(">Warning<")
    expect(filters).toContain("FILTER_STATUS_OPTIONS")
    expect(filters).toContain("FILTER_WARNING_OPTIONS")
    expect(filters).toContain("Todas")
  })

  it("symbology tab present and static panel has no fetch", () => {
    const page = fs.readFileSync(
      path.join(root, "app/(dashboard)/costos-v2/page.tsx"),
      "utf8",
    )
    expect(page).toContain('value="simbologia"')
    expect(page).toContain("CostV2SymbologyPanel")
    const panel = fs.readFileSync(
      path.join(root, "components/costos-v2/cost-v2-symbology-panel.tsx"),
      "utf8",
    )
    expect(panel).toContain("Simbología de costos")
    expect(panel).toContain("SYMBOLOGY_SCOPE_NOTE")
    expect(panel).not.toContain("fetch(")
    expect(panel).not.toContain("getCostV2")
    expect(panel).not.toContain("company_id")
    expect(panel).not.toContain("office_id")
    // Códigos solo como keys de datos, no como texto visible hardcoded
    expect(panel).not.toMatch(/>\s*missing_taxes_in_gross\s*</)
  })

  it("product drawer uses Estado del costo and Ver simbología", () => {
    const drawer = fs.readFileSync(
      path.join(root, "components/costos-v2/cost-v2-product-detail-drawer.tsx"),
      "utf8",
    )
    expect(drawer).toContain("Estado del costo")
    expect(drawer).toContain("Ver simbología")
    expect(drawer).toContain("Detalle técnico")
    expect(drawer).toContain("effective_quality_status")
    expect(drawer).not.toContain("Calidad tributaria")
  })

  it("legacy /costos intact", () => {
    const legacy = fs.readFileSync(
      path.join(root, "app/(dashboard)/costos/page.tsx"),
      "utf8",
    )
    expect(legacy).not.toContain("CostV2SymbologyPanel")
    expect(legacy).not.toContain("statusLabel(")
  })
})

describe("label helpers never echo unknown technical as-is for status", () => {
  it("unknown status becomes Estado desconocido", () => {
    expect(statusLabel("foo_bar_status")).toBe("Estado desconocido")
    expect(Object.values(COST_V2_STATUS_LABEL)).not.toContain("foo_bar_status")
    expect(Object.values(COST_V2_WARNING_LABEL)).toContain("Costo atípico")
  })
})
