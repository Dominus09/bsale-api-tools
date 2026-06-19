export type CommercialSemaphore = "green" | "yellow" | "red"

export const SEMAPHORE_EMOJI: Record<CommercialSemaphore, string> = {
  green: "🟢",
  yellow: "🟡",
  red: "🔴",
}

export const SEMAPHORE_RING_COLOR: Record<CommercialSemaphore, string> = {
  green: "#16a34a",
  yellow: "#ca8a04",
  red: "#dc2626",
}

export const SEMAPHORE_BORDER_CLASS: Record<CommercialSemaphore, string> = {
  green: "border-emerald-500/40 bg-emerald-500/5",
  yellow: "border-amber-500/40 bg-amber-500/5",
  red: "border-red-500/40 bg-red-500/5",
}

/** Umbral comercial ORS 2.0: verde ≥300k, amarillo ≥100k, rojo <100k. */
export function commercialSemaphore(ventaClp: number): CommercialSemaphore {
  const v = Number(ventaClp) || 0
  if (v >= 300_000) return "green"
  if (v >= 100_000) return "yellow"
  return "red"
}

export type CommercialSemaphoreCounts = {
  green: number
  yellow: number
  red: number
}

export function countCommercialSemaphores(
  rows: { semaphore: CommercialSemaphore }[],
): CommercialSemaphoreCounts {
  const out: CommercialSemaphoreCounts = { green: 0, yellow: 0, red: 0 }
  for (const row of rows) {
    out[row.semaphore] += 1
  }
  return out
}
