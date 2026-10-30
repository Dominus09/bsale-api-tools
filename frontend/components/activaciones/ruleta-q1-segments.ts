/** Ruleta Quillotana Q1 — 9 sectores SVG iguales (40°) y sustituciones locales. */

export type RuletaCode =
  | "SORPRESA"
  | "VASOS"
  | "P10"
  | "BEBIDA"
  | "PROMO"
  | "NONE"
  | "RETRY"
  | "P03"
  | "P05"

export type SlotOverride = "original" | "RETRY" | "RANDOM"

export type RuletaIconKind =
  | "gift"
  | "cups"
  | "percent"
  | "can"
  | "sad"
  | "retry"
  | "random"
  | "stars"

export type RuletaSegmentBase = {
  index: number
  code: RuletaCode
  label: string
  shortLabel: string
  /** Líneas de texto en el sector (mayúsculas). */
  lines: readonly string[]
  /** Si true, la primera línea se dibuja mucho más grande (%, etc.). */
  emphasizeFirstLine: boolean
  color: string
  textColor: string
  icon: RuletaIconKind
}

export type ActiveSlot = RuletaSegmentBase & {
  displayCode: string
  displayLabel: string
  displayShortLabel: string
  displayLines: readonly string[]
  displayEmphasizeFirstLine: boolean
  displayColor: string
  displayTextColor: string
  displayIcon: RuletaIconKind
  override: SlotOverride
  startDeg: number
  endDeg: number
  midDeg: number
  spanDeg: number
}

export type RuletaLocalConfig = {
  version: 3
  overrides: SlotOverride[]
}

export const STORAGE_KEY = "quillotana.ruleta.q1.config.v3"
const LEGACY_STORAGE_KEYS = [
  "quillotana.ruleta.q1.config.v2",
  "quillotana.ruleta.q1.config.v1",
] as const

export const RULETA_SLICE_DEG = 40
export const RULETA_SLOT_COUNT = 9

/** Logo circular original (único asset raster; capa fija). */
export const RULETA_HUB_LOGO_SRC = "/activaciones/ruleta/quillotana-hub-logo.png"

export const RULETA_SEGMENTS: readonly RuletaSegmentBase[] = [
  {
    index: 0,
    code: "SORPRESA",
    label: "Premio sorpresa",
    shortLabel: "Sorpresa",
    lines: ["PREMIO", "SORPRESA"],
    emphasizeFirstLine: false,
    color: "#E53935",
    textColor: "#FFFFFF",
    icon: "gift",
  },
  {
    index: 1,
    code: "VASOS",
    label: "Vasos Quillotana",
    shortLabel: "Vasos",
    lines: ["VASOS", "QUILLOTANA"],
    emphasizeFirstLine: false,
    color: "#7B1FA2",
    textColor: "#FFFFFF",
    icon: "cups",
  },
  {
    index: 2,
    code: "P10",
    label: "10% descuento",
    shortLabel: "10%",
    lines: ["10%", "DESCUENTO"],
    emphasizeFirstLine: true,
    color: "#FB8C00",
    textColor: "#FFFFFF",
    icon: "percent",
  },
  {
    index: 3,
    code: "BEBIDA",
    label: "Bebida gratis",
    shortLabel: "Bebida",
    lines: ["BEBIDA", "GRATIS"],
    emphasizeFirstLine: false,
    color: "#43A047",
    textColor: "#FFFFFF",
    icon: "can",
  },
  {
    index: 4,
    code: "PROMO",
    label: "Promoción gratis",
    shortLabel: "Promo",
    lines: ["PROMOCIÓN", "GRATIS"],
    emphasizeFirstLine: false,
    color: "#D81B60",
    textColor: "#FFFFFF",
    icon: "gift",
  },
  {
    index: 5,
    code: "NONE",
    label: "¡Esta vez no ganaste!",
    shortLabel: "Sin premio",
    lines: ["¡ESTA VEZ", "NO GANASTE!"],
    emphasizeFirstLine: false,
    color: "#78909C",
    textColor: "#FFFFFF",
    icon: "sad",
  },
  {
    index: 6,
    code: "RETRY",
    label: "¡Vuelve a girar!",
    shortLabel: "Otra vez",
    lines: ["¡VUELVE", "A GIRAR!"],
    emphasizeFirstLine: false,
    color: "#00897B",
    textColor: "#FFFFFF",
    icon: "retry",
  },
  {
    index: 7,
    code: "P03",
    label: "3% descuento",
    shortLabel: "3%",
    lines: ["3%", "DESCUENTO"],
    emphasizeFirstLine: true,
    color: "#1E88E5",
    textColor: "#FFFFFF",
    icon: "percent",
  },
  {
    index: 8,
    code: "P05",
    label: "5% descuento",
    shortLabel: "5%",
    lines: ["5%", "DESCUENTO"],
    emphasizeFirstLine: true,
    color: "#FDD835",
    textColor: "#FFFFFF",
    icon: "percent",
  },
] as const

const RETRY_DISPLAY = {
  displayCode: "RETRY",
  displayLabel: "¡Vuelve a girar!",
  displayShortLabel: "¡VUELVE A GIRAR!",
  displayLines: ["¡VUELVE", "A GIRAR!"] as const,
  displayEmphasizeFirstLine: false,
  displayIcon: "retry" as const,
}

const RANDOM_DISPLAY = {
  displayCode: "RANDOM",
  displayLabel: "¡Premio random!",
  displayShortLabel: "¡PREMIO RANDOM!",
  displayLines: ["¡PREMIO", "RANDOM!"] as const,
  displayEmphasizeFirstLine: false,
  displayIcon: "random" as const,
}

export function defaultConfig(): RuletaLocalConfig {
  return {
    version: 3,
    overrides: RULETA_SEGMENTS.map(() => "original" as SlotOverride),
  }
}

function resolveDisplay(base: RuletaSegmentBase, override: SlotOverride) {
  if (override === "RETRY") {
    return {
      ...RETRY_DISPLAY,
      displayColor: base.color,
      displayTextColor: "#FFFFFF",
    }
  }
  if (override === "RANDOM") {
    return {
      ...RANDOM_DISPLAY,
      displayColor: base.color,
      displayTextColor: "#FFFFFF",
    }
  }
  return {
    displayCode: base.code,
    displayLabel: base.label,
    displayShortLabel: base.shortLabel,
    displayLines: base.lines,
    displayEmphasizeFirstLine: base.emphasizeFirstLine,
    displayColor: base.color,
    displayTextColor: base.textColor,
    displayIcon: base.icon,
  }
}

/**
 * Centro angular del sector i.
 * 0° = arriba, sentido horario (mismo sistema que `rotate()` SVG/CSS).
 * Puntero fijo inferior = 180°.
 */
export function slotVisualMidDeg(index: number): number {
  return ((index * RULETA_SLICE_DEG) % 360 + 360) % 360
}

export function normalizeConfig(raw: unknown): RuletaLocalConfig {
  const base = defaultConfig()
  if (!raw || typeof raw !== "object") return base
  const obj = raw as { overrides?: unknown }
  if (!Array.isArray(obj.overrides)) return base
  const overs = obj.overrides.map((o) =>
    o === "RETRY" || o === "RANDOM" || o === "original" ? o : "original",
  )
  while (overs.length < RULETA_SEGMENTS.length) overs.push("original")
  return { version: 3, overrides: overs.slice(0, RULETA_SEGMENTS.length) }
}

export function loadConfigFromStorage(): RuletaLocalConfig {
  if (typeof window === "undefined") return defaultConfig()
  try {
    let raw = window.localStorage.getItem(STORAGE_KEY)
    let fromLegacy = false
    if (!raw) {
      for (const key of LEGACY_STORAGE_KEYS) {
        const legacy = window.localStorage.getItem(key)
        if (legacy) {
          raw = legacy
          fromLegacy = true
          break
        }
      }
    }
    if (!raw) return defaultConfig()
    const parsed = JSON.parse(raw) as { overrides?: unknown }
    const cfg = normalizeConfig(parsed)
    if (fromLegacy) {
      for (const key of LEGACY_STORAGE_KEYS) window.localStorage.removeItem(key)
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(cfg))
    }
    return cfg
  } catch {
    return defaultConfig()
  }
}

export function saveConfigToStorage(config: RuletaLocalConfig): void {
  if (typeof window === "undefined") return
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(normalizeConfig(config)))
}

export function clearConfigStorage(): void {
  if (typeof window === "undefined") return
  window.localStorage.removeItem(STORAGE_KEY)
  for (const key of LEGACY_STORAGE_KEYS) window.localStorage.removeItem(key)
}

/** Nueve sectores fijos de 40°. La sustitución cambia contenido SVG, no el ángulo. */
export function buildActiveSlots(config: RuletaLocalConfig): ActiveSlot[] {
  const cfg = normalizeConfig(config)
  return RULETA_SEGMENTS.map((base, i) => {
    const override = cfg.overrides[i] ?? "original"
    const midDeg = slotVisualMidDeg(i)
    const half = RULETA_SLICE_DEG / 2
    const display = resolveDisplay(base, override)
    return {
      ...base,
      ...display,
      override,
      startDeg: midDeg - half,
      endDeg: midDeg + half,
      midDeg,
      spanDeg: RULETA_SLICE_DEG,
    }
  })
}

/** Índice uniforme 0..n-1 con CSPRNG (rechazo para evitar sesgo de módulo). */
export function pickUniformSlotIndex(count = RULETA_SEGMENTS.length): number {
  if (count <= 0) throw new Error("count inválido")
  if (typeof crypto !== "undefined" && typeof crypto.getRandomValues === "function") {
    const buf = new Uint32Array(1)
    const max = 0x100000000
    const limit = max - (max % count)
    let x = 0
    do {
      crypto.getRandomValues(buf)
      x = buf[0]!
    } while (x >= limit)
    return x % count
  }
  return Math.floor(Math.random() * count)
}

export function winnerTone(slot: ActiveSlot): "win" | "none" | "retry" | "random" {
  if (slot.displayCode === "NONE") return "none"
  if (slot.displayCode === "RETRY") return "retry"
  if (slot.displayCode === "RANDOM") return "random"
  return "win"
}
