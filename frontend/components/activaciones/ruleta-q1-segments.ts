/** Casillas oficiales Ruleta Quillotana Q1 — 9 sectores iguales (40°) y sustituciones locales. */

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

export type RuletaSegmentBase = {
  index: number
  code: RuletaCode
  label: string
  shortLabel: string
  color: string
  textColor: string
}

export type ActiveSlot = RuletaSegmentBase & {
  displayCode: string
  displayLabel: string
  displayShortLabel: string
  displayColor: string
  displayTextColor: string
  override: SlotOverride
  startDeg: number
  endDeg: number
  midDeg: number
  spanDeg: number
}

export type RuletaLocalConfig = {
  version: 2
  overrides: SlotOverride[]
}

export const STORAGE_KEY = "quillotana.ruleta.q1.config.v2"
/** Clave anterior (probabilidades); se limpia al migrar. */
const LEGACY_STORAGE_KEY = "quillotana.ruleta.q1.config.v1"

export const RULETA_SLICE_DEG = 40

/**
 * Geometría relativa al radio del disco en la foto física (fracción de radius = size/2).
 * Calibrada sobre ruleta-fisica-q1.jpg para no tapar emblema ni borde metálico.
 */
export const RULETA_HUB_DIAMETER_FRAC = 0.295
export const RULETA_R_INNER_FRAC = 0.31
export const RULETA_R_OUTER_FRAC = 0.875

export const RULETA_SEGMENTS: readonly RuletaSegmentBase[] = [
  {
    index: 0,
    code: "SORPRESA",
    label: "Premio sorpresa",
    shortLabel: "Sorpresa",
    color: "#E53935",
    textColor: "#FFFFFF",
  },
  {
    index: 1,
    code: "VASOS",
    label: "Vasos Quillotana",
    shortLabel: "Vasos",
    color: "#7B1FA2",
    textColor: "#FFFFFF",
  },
  {
    index: 2,
    code: "P10",
    label: "10% descuento",
    shortLabel: "10%",
    color: "#FB8C00",
    textColor: "#1A1208",
  },
  {
    index: 3,
    code: "BEBIDA",
    label: "Bebida gratis",
    shortLabel: "Bebida",
    color: "#43A047",
    textColor: "#FFFFFF",
  },
  {
    index: 4,
    code: "PROMO",
    label: "Promoción gratis",
    shortLabel: "Promo",
    color: "#D81B60",
    textColor: "#FFFFFF",
  },
  {
    index: 5,
    code: "NONE",
    label: "¡Esta vez no ganaste!",
    shortLabel: "Sin premio",
    color: "#78909C",
    textColor: "#FFFFFF",
  },
  {
    index: 6,
    code: "RETRY",
    label: "¡Vuelve a girar!",
    shortLabel: "Otra vez",
    color: "#00897B",
    textColor: "#FFFFFF",
  },
  {
    index: 7,
    code: "P03",
    label: "3% descuento",
    shortLabel: "3%",
    color: "#1E88E5",
    textColor: "#FFFFFF",
  },
  {
    index: 8,
    code: "P05",
    label: "5% descuento",
    shortLabel: "5%",
    color: "#FDD835",
    textColor: "#1A1208",
  },
] as const

const RETRY_META = {
  displayCode: "RETRY",
  displayLabel: "¡Vuelve a girar!",
  displayShortLabel: "¡VUELVE A GIRAR!",
  displayColor: "#00897B",
  displayTextColor: "#FFFFFF",
} as const

const RANDOM_META = {
  displayCode: "RANDOM",
  displayLabel: "¡Premio random!",
  displayShortLabel: "¡PREMIO RANDOM!",
  displayColor: "#6D28D9",
  displayTextColor: "#FFFFFF",
} as const

export function defaultConfig(): RuletaLocalConfig {
  return {
    version: 2,
    overrides: RULETA_SEGMENTS.map(() => "original" as SlotOverride),
  }
}

function resolveDisplay(base: RuletaSegmentBase, override: SlotOverride) {
  // Sustitución: solo cambia el texto; conserva color original del sector.
  if (override === "RETRY") {
    return {
      ...RETRY_META,
      displayColor: base.color,
      displayTextColor: base.textColor,
    }
  }
  if (override === "RANDOM") {
    return {
      ...RANDOM_META,
      displayColor: base.color,
      displayTextColor: base.textColor,
    }
  }
  return {
    displayCode: base.code,
    displayLabel: base.label,
    displayShortLabel: base.shortLabel,
    displayColor: base.color,
    displayTextColor: base.textColor,
  }
}

/**
 * Ángulo visual del centro del sector i en la foto física (sin espejo).
 * Índice 0 (Sorpresa/rojo) centrado arriba (0°). Sentido horario CSS.
 * El indicador fijo apunta desde abajo (180°).
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
  return { version: 2, overrides: overs.slice(0, RULETA_SEGMENTS.length) }
}

export function loadConfigFromStorage(): RuletaLocalConfig {
  if (typeof window === "undefined") return defaultConfig()
  try {
    // Migrar desde v1 (probabilidades): conservar solo overrides si existen.
    const legacy = window.localStorage.getItem(LEGACY_STORAGE_KEY)
    const raw = window.localStorage.getItem(STORAGE_KEY) ?? legacy
    if (!raw) return defaultConfig()
    const parsed = JSON.parse(raw) as { overrides?: unknown }
    const cfg = normalizeConfig(parsed)
    if (legacy) {
      window.localStorage.removeItem(LEGACY_STORAGE_KEY)
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
  window.localStorage.removeItem(LEGACY_STORAGE_KEY)
}

/**
 * Nueve sectores fijos de 40° en coordenadas de la foto (antes de girar).
 * La sustitución solo cambia el texto visible; el ángulo y la probabilidad (1/9) no cambian.
 */
export function buildActiveSlots(config: RuletaLocalConfig): ActiveSlot[] {
  const cfg = normalizeConfig(config)
  return RULETA_SEGMENTS.map((base, i) => {
    const override = cfg.overrides[i] ?? "original"
    const midDeg = slotVisualMidDeg(i)
    const half = RULETA_SLICE_DEG / 2
    // Ángulos en espacio de la foto; se permiten valores fuera de [0,360) para arcos contiguos.
    const startDeg = midDeg - half
    const endDeg = midDeg + half
    const display = resolveDisplay(base, override)
    return {
      ...base,
      ...display,
      override,
      startDeg,
      endDeg,
      midDeg,
      spanDeg: RULETA_SLICE_DEG,
    }
  })
}

/** Índice uniforme 0..8 con CSPRNG (rechazo para evitar sesgo de módulo). */
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
