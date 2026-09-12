import { describe, expect, it } from "vitest"
import {
  formatPpum,
  parseProductContent,
  resolvePpumLabel,
} from "./etiquetas2-ppum"

describe("etiquetas2 PPUM", () => {
  it("Aceite Bonanza 900 cc → por litro", () => {
    const c = parseProductContent("ACEITE BONANZA", "VEGETAL 900 CC (SEC 12)")
    expect(c).toEqual({
      baseAmount: 0.9,
      unitLabel: "litro",
      kind: "volume",
    })
    expect(Math.round(2290 / 0.9)).toBe(2544)
    expect(formatPpum(2290, c!)).toBe(
      new Intl.NumberFormat("es-CL", {
        style: "currency",
        currency: "CLP",
        maximumFractionDigits: 0,
      }).format(2544) + " por litro",
    )
    expect(resolvePpumLabel(1825, "ACEITE BONANZA", "VEGETAL 900 CC")).toMatch(
      /por litro$/,
    )
    expect(Math.round(1825 / 0.9)).toBe(2028)
  })

  it("Cristal Retornable 1.2 LT → por litro", () => {
    const c = parseProductContent("CRISTAL RETORNABLE 1.2 LT")
    expect(c?.baseAmount).toBeCloseTo(1.2, 5)
    expect(c?.unitLabel).toBe("litro")
    expect(formatPpum(1200, c)).toMatch(/por litro$/)
  })

  it("Azúcar Iansa 1 kg → por kg", () => {
    const c = parseProductContent("AZUCAR IANSA 1 KG")
    expect(c).toEqual({ baseAmount: 1, unitLabel: "kg", kind: "weight" })
    expect(formatPpum(990, c)).toMatch(/por kg$/)
  })

  it("sin contenido confiable → no disponible", () => {
    expect(parseProductContent("SERVICIO DELIVERY")).toBeNull()
    expect(resolvePpumLabel(1000, "SERVICIO DELIVERY")).toBe("PPUM no disponible")
    expect(formatPpum(null, null)).toBe("PPUM no disponible")
  })

  it("500 g → kg base", () => {
    const c = parseProductContent("HARINA 500 G")
    expect(c?.baseAmount).toBeCloseTo(0.5, 5)
    expect(c?.kind).toBe("weight")
  })
})
