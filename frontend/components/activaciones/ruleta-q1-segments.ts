/** Casillas oficiales Ruleta Quillotana Q1 — probabilidades y sustituciones locales. */

export type RuletaCode =
  | "SORPRESA"
  | "VASOS"
  | "P10"
  | "BEBIDA"
  | "PROMO"
  | "NONE"
  | "RETRY"
  | "P05"
  | "P03"

export type SlotOverride = "original" | "RETRY" | "RANDOM"

export type RuletaSegmentBase = {
  index: number
  code: RuletaCode
  label: string
  shortLabel: string
  color: string
  textColor: string
  defaultProbability: number
}

export type ActiveSlot = RuletaSegmentBase & {
  /** Código visual/resultado tras override. */
  displayCode: string
  displayLabel: string
  displayShortLabel: string
  override: SlotOverride
  probability: number
  startDeg: number
  endDeg: number
  midDeg: number
  spanDeg: number
}

export type RuletaLocalConfig = {
  version: 1
  probabilities: number[]
  overrides: SlotOverride[]
}

export const STORAGE_KEY = "quillotana.ruleta.q1.config.v1"

export const RULETA_SEGMENTS: readonly RuletaSegmentBase[] = [
  {
    index: 0,
    code: "SORPRESA",
    label: "Premio sorpresa",
    shortLabel: "Sorpresa",
    color: "#E53935",
    textColor: "#FFFFFF",
    defaultProbability: 8,
  },
  {
    index: 1,
    code: "VASOS",
    label: "Vasos Quillotana",
    shortLabel: "Vasos",
    color: "#7B1FA2",
    textColor: "#FFFFFF",
    defaultProbability: 11,
  },
  {
    index: 2,
    code: "P10",
    label: "10% descuento",
    shortLabel: "10%",
    color: "#FB8C00",
    textColor: "#1A1208",
    defaultProbability: 3,
  },
  {
    index: 3,
    code: "BEBIDA",
    label: "Bebida gratis",
    shortLabel: "Bebida",
    color: "#43A047",
    textColor: "#FFFFFF",
    defaultProbability: 12,
  },
  {
    index: 4,
    code: "PROMO",
    label: "Promoción gratis",
    shortLabel: "Promo",
    color: "#D81B60",
    textColor: "#FFFFFF",
    defaultProbability: 7,
  },
  {
    index: 5,
    code: "NONE",
    label: "¡Esta vez no ganaste!",
    shortLabel: "Sin premio",
    color: "#78909C",
    textColor: "#FFFFFF",
    defaultProbability: 25,
  },
  {
    index: 6,
    code: "RETRY",
    label: "¡Vuelve a girar!",
    shortLabel: "Otra vez",
    color: "#00897B",
    textColor: "#FFFFFF",
    defaultProbability: 16,
  },
  {
    index: 7,
    code: "P03",
    label: "3% descuento",
    shortLabel: "3%",
    color: "#1E88E5",
    textColor: "#FFFFFF",
    defaultProbability: 10,
  },
  {
    index: 8,
    code: "P05",
    label: "5% descuento",
    shortLabel: "5%",
    color: "#FDD835",
    textColor: "#1A1208",
    defaultProbability: 8,
  },
] as const

const RETRY_META = {
  displayCode: "RETRY",
  displayLabel: "¡Vuelve a girar!",
  displayShortLabel: "Otra vez",
  color: "#00897B",
  textColor: "#FFFFFF",
} as const

const RANDOM_META = {
  displayCode: "RANDOM",
  displayLabel: "¡Premio random!",
  displayShortLabel: "Random",
  color: "#6D28D9",
  textColor: "#FFFFFF",
} as const

export function defaultConfig(): RuletaLocalConfig {
  return {
    version: 1,
    probabilities: RULETA_SEGMENTS.map((s) => s.defaultProbability),
    overrides: RULETA_SEGMENTS.map(() => "original" as SlotOverride),
  }
}

export function sumProbabilities(probs: number[]): number {
  return probs.reduce((a, b) => a + (Number.isFinite(b) ? b : 0), 0)
}

export function probabilitiesAreValid(probs: number[]): boolean {
  if (probs.length !== RULETA_SEGMENTS.length) return false
  if (probs.some((p) => !Number.isFinite(p) || p < 0)) return false
  return Math.abs(sumProbabilities(probs) - 100) < 0.001
}

export function normalizeConfig(raw: unknown): RuletaLocalConfig {
  const base = defaultConfig()
  if (!raw || typeof raw !== "object") return base
  const obj = raw as Partial<RuletaLocalConfig>
  const probs = Array.isArray(obj.probabilities)
    ? obj.probabilities.map((p) => Number(p))
    : base.probabilities
  const overs = Array.isArray(obj.overrides)
    ? obj.overrides.map((o) =>
        o === "RETRY" || o === "RANDOM" || o === "original" ? o : "original",
      )
    : base.overrides
  while (probs.length < RULETA_SEGMENTS.length) probs.push(0)
  while (overs.length < RULETA_SEGMENTS.length) overs.push("original")
  const clippedP = probs.slice(0, RULETA_SEGMENTS.length)
  const clippedO = overs.slice(0, RULETA_SEGMENTS.length)
  if (!probabilitiesAreValid(clippedP)) return base
  return { version: 1, probabilities: clippedP, overrides: clippedO }
}

export function loadConfigFromStorage(): RuletaLocalConfig {
  if (typeof window === "undefined") return defaultConfig()
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY)
    if (!raw) return defaultConfig()
    return normalizeConfig(JSON.parse(raw))
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
}

function resolveDisplay(base: RuletaSegmentBase, override: SlotOverride) {
  if (override === "RETRY") {
    return {
      displayCode: RETRY_META.displayCode,
      displayLabel: RETRY_META.displayLabel,
      displayShortLabel: RETRY_META.displayShortLabel,
      color: RETRY_META.color,
      textColor: RETRY_META.textColor,
    }
  }
  if (override === "RANDOM") {
    return {
      displayCode: RANDOM_META.displayCode,
      displayLabel: RANDOM_META.displayLabel,
      displayShortLabel: RANDOM_META.displayShortLabel,
      color: RANDOM_META.color,
      textColor: RANDOM_META.textColor,
    }
  }
  return {
    displayCode: base.code,
    displayLabel: base.label,
    displayShortLabel: base.shortLabel,
    color: base.color,
    textColor: base.textColor,
  }
}

/** Construye sectores con ángulos proporcionales a la probabilidad. */
export function buildActiveSlots(config: RuletaLocalConfig): ActiveSlot[] {
  const cfg = normalizeConfig(config)
  const total = sumProbabilities(cfg.probabilities)
  let cursor = 0
  return RULETA_SEGMENTS.map((base, i) => {
    const probability = cfg.probabilities[i] ?? 0
    const override = cfg.overrides[i] ?? "original"
    const spanDeg = total > 0 ? (probability / total) * 360 : 0
    const startDeg = cursor
    const endDeg = cursor + spanDeg
    const midDeg = startDeg + spanDeg / 2
    cursor = endDeg
    const display = resolveDisplay(base, override)
    return {
      ...base,
      ...display,
      override,
      probability,
      startDeg,
      endDeg,
      midDeg,
      spanDeg,
    }
  })
}

/** Selección ponderada con CSPRNG (rechazo para evitar sesgo). */
export function pickWeightedSlotIndex(probabilities: number[]): number {
  if (probabilities.length === 0) throw new Error("sin casillas")
  const weights = probabilities.map((p) => Math.max(0, Number(p) || 0))
  const total = weights.reduce((a, b) => a + b, 0)
  if (total <= 0) throw new Error("probabilidades inválidas")

  const buf = new Uint32Array(1)
  const max = 0x100000000
  // Escala a entero: pesos * 1000 para centésimas de %
  const scaled = weights.map((w) => Math.round(w * 1000))
  const scaledTotal = scaled.reduce((a, b) => a + b, 0)
  if (scaledTotal <= 0) throw new Error("probabilidades inválidas")
  const limit = max - (max % scaledTotal)
  let x = 0
  if (typeof crypto !== "undefined" && typeof crypto.getRandomValues === "function") {
    do {
      crypto.getRandomValues(buf)
      x = buf[0]!
    } while (x >= limit)
    x = x % scaledTotal
  } else {
    x = Math.floor(Math.random() * scaledTotal)
  }
  let acc = 0
  for (let i = 0; i < scaled.length; i++) {
    acc += scaled[i]!
    if (x < acc) return i
  }
  return scaled.length - 1
}

export function winnerTone(slot: ActiveSlot): "win" | "none" | "retry" | "random" {
  if (slot.displayCode === "NONE") return "none"
  if (slot.displayCode === "RETRY") return "retry"
  if (slot.displayCode === "RANDOM") return "random"
  return "win"
}
