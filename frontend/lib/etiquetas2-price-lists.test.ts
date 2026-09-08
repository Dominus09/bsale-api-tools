/** Unit tests — config y estados Etiquetas V2 (sin BD / sin red). */

import { describe, expect, it } from "vitest"
import {
  resolveEtiquetas2PriceLists,
  findStaticPriceConfigForCompany,
} from "./etiquetas2-price-lists"
import { socioPriceStatus, statusLabel } from "./etiquetas2-status"

describe("etiquetas2 price lists by company", () => {
  it("matches La Quillotana SpA and uses lists 12/13", () => {
    const cfg = findStaticPriceConfigForCompany(1, "La Quillotana SpA")
    expect(cfg).not.toBeNull()
    expect(cfg?.lists.normal).toBe(12)
    expect(cfg?.lists.socio).toBe(13)
    expect(cfg?.lists.wow).toBeNull()
    expect(cfg?.socioProvisional).toBe(true)
  })

  it("resolves when live lists contain 12 and 13", () => {
    const resolved = resolveEtiquetas2PriceLists(1, "La Quillotana SpA", [
      { id: 12, name: "Supermercado La Quillotana" },
      { id: 13, name: "Ruta Factura" },
      { id: 99, name: "Otra" },
    ])
    expect(resolved.configured).toBe(true)
    expect(resolved.normal?.id).toBe(12)
    expect(resolved.socio?.id).toBe(13)
    expect(resolved.socio?.provisional).toBe(true)
    expect(resolved.wow).toBeNull()
  })

  it("is pending for Minimarket until configured", () => {
    const resolved = resolveEtiquetas2PriceLists(2, "Minimarket La Quillotana", [
      { id: 20, name: "Minimarket" },
    ])
    expect(resolved.configured).toBe(false)
    expect(resolved.pendingReason).toContain("CONFIGURACIÓN DE LISTAS PENDIENTE")
  })

  it("is pending if configured IDs are missing from live lists", () => {
    const resolved = resolveEtiquetas2PriceLists(1, "La Quillotana SpA", [
      { id: 99, name: "Otra lista" },
    ])
    expect(resolved.configured).toBe(false)
  })
})

describe("etiquetas2 socio price status", () => {
  it("OK when socio < normal", () => {
    expect(socioPriceStatus(1000, 900)).toBe("OK")
    expect(statusLabel("OK")).toBe("OK")
  })

  it("SIN_PRECIO_SOCIO when socio missing", () => {
    expect(socioPriceStatus(1000, null)).toBe("SIN_PRECIO_SOCIO")
  })

  it("REVISAR when socio >= normal", () => {
    expect(socioPriceStatus(1500, 1600)).toBe("REVISAR_PRECIO_SOCIO")
    expect(socioPriceStatus(1500, 1500)).toBe("REVISAR_PRECIO_SOCIO")
  })
})
