import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"

const ORIGINAL_ENV = { ...process.env }

async function loadHelper() {
  vi.resetModules()
  return import("@/lib/carto-basemap")
}

function setEnv(values: Record<string, string | undefined>) {
  for (const [k, v] of Object.entries(values)) {
    if (v === undefined) delete process.env[k]
    else process.env[k] = v
  }
}

describe("carto-basemap", () => {
  beforeEach(() => {
    process.env = { ...ORIGINAL_ENV }
  })
  afterEach(() => {
    process.env = { ...ORIGINAL_ENV }
    vi.restoreAllMocks()
  })

  it("agrega la key a la plantilla voyager conservando {s}{z}{x}{y}{r}", async () => {
    setEnv({ NEXT_PUBLIC_CARTO_BASEMAPS_KEY: "test-key", NODE_ENV: "production" })
    const { cartoTileUrlTemplate } = await loadHelper()
    expect(cartoTileUrlTemplate("voyager")).toBe(
      "https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png?key=test-key",
    )
  })

  it("light_all y variante sin {r}", async () => {
    setEnv({ NEXT_PUBLIC_CARTO_BASEMAPS_KEY: "test-key", NODE_ENV: "production" })
    const { cartoTileUrlTemplate } = await loadHelper()
    expect(cartoTileUrlTemplate("light_all")).toBe(
      "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png?key=test-key",
    )
    expect(cartoTileUrlTemplate("voyager", { retina: false })).toBe(
      "https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}.png?key=test-key",
    )
  })

  it("tile concreto para canvas y key codificada", async () => {
    setEnv({ NEXT_PUBLIC_CARTO_BASEMAPS_KEY: "a b&c", NODE_ENV: "production" })
    const { cartoTileUrl } = await loadHelper()
    expect(cartoTileUrl("voyager", "b", 12, 1234, 2345)).toBe(
      "https://b.basemaps.cartocdn.com/rastertiles/voyager/12/1234/2345.png?key=a%20b%26c",
    )
  })

  it("en desarrollo sin key lanza error claro sin exponer valores", async () => {
    setEnv({ NEXT_PUBLIC_CARTO_BASEMAPS_KEY: undefined, NODE_ENV: "development" })
    const { cartoTileUrlTemplate } = await loadHelper()
    expect(() => cartoTileUrlTemplate("voyager")).toThrow(/NEXT_PUBLIC_CARTO_BASEMAPS_KEY/)
  })

  it("en producción sin key no rompe: URL sin key y un solo warning", async () => {
    setEnv({ NEXT_PUBLIC_CARTO_BASEMAPS_KEY: "  ", NODE_ENV: "production" })
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {})
    const { cartoTileUrlTemplate } = await loadHelper()
    expect(cartoTileUrlTemplate("voyager")).toBe(
      "https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png",
    )
    cartoTileUrlTemplate("light_all")
    expect(warn).toHaveBeenCalledTimes(1)
  })

  it("atribución visible requerida", async () => {
    const { CARTO_ATTRIBUTION_TEXT, CARTO_ATTRIBUTION_HTML } = await loadHelper()
    expect(CARTO_ATTRIBUTION_TEXT).toBe("© OpenStreetMap contributors © CARTO")
    expect(CARTO_ATTRIBUTION_HTML).toContain("OpenStreetMap</a> contributors")
    expect(CARTO_ATTRIBUTION_HTML).toContain("CARTO</a>")
  })
})
