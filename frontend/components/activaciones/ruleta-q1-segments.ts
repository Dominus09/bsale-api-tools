/** Casillas oficiales Ruleta Quillotana Q1 (9 espacios iguales, horario desde arriba). */

export type RuletaSegment = {
  index: number
  code: string
  label: string
  /** Color de relleno de la casilla (hex). */
  color: string
  /** Texto sobre la casilla (contraste). */
  textColor: string
}

export const RULETA_SEGMENTS: readonly RuletaSegment[] = [
  { index: 0, code: "SORPRESA", label: "Premio sorpresa", color: "#E53935", textColor: "#FFFFFF" },
  { index: 1, code: "VASOS", label: "Vasos Quillotana", color: "#7B1FA2", textColor: "#FFFFFF" },
  { index: 2, code: "P10", label: "10% descuento", color: "#FB8C00", textColor: "#1A1208" },
  { index: 3, code: "BEBIDA", label: "Bebida gratis", color: "#43A047", textColor: "#FFFFFF" },
  { index: 4, code: "PROMO", label: "Promoción gratis", color: "#D81B60", textColor: "#FFFFFF" },
  { index: 5, code: "NONE", label: "¡Esta vez no ganaste!", color: "#78909C", textColor: "#FFFFFF" },
  { index: 6, code: "RETRY", label: "¡Vuelve a girar!", color: "#00897B", textColor: "#FFFFFF" },
  { index: 7, code: "P03", label: "3% descuento", color: "#1E88E5", textColor: "#FFFFFF" },
  { index: 8, code: "P05", label: "5% descuento", color: "#FDD835", textColor: "#1A1208" },
] as const

export const RULETA_SLICE_DEG = 360 / RULETA_SEGMENTS.length

/** Índice uniforme con CSPRNG del navegador (no Math.random). */
export function pickUniformSegmentIndex(count = RULETA_SEGMENTS.length): number {
  if (count <= 0) throw new Error("count inválido")
  if (typeof crypto !== "undefined" && typeof crypto.getRandomValues === "function") {
    const buf = new Uint32Array(1)
    // Rechazo para evitar sesgo de módulo
    const max = 0x100000000
    const limit = max - (max % count)
    let x = 0
    do {
      crypto.getRandomValues(buf)
      x = buf[0]!
    } while (x >= limit)
    return x % count
  }
  // Fallback extremo (entornos sin crypto): aún así no usar para lógica de negocio ERP.
  return Math.floor(Math.random() * count)
}
