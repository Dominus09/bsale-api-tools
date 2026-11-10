/** Agrupa planes por fecha operativa en America/Santiago (sin inventar fechas). */

export type PlanDateBucket = "today" | "upcoming" | "history" | "unknown"

/** YYYY-MM-DD del calendario Chile ahora. */
export function santiagoTodayYmd(now: Date = new Date()): string {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Santiago",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(now)
  const y = parts.find((p) => p.type === "year")?.value
  const m = parts.find((p) => p.type === "month")?.value
  const d = parts.find((p) => p.type === "day")?.value
  if (!y || !m || !d) return now.toISOString().slice(0, 10)
  return `${y}-${m}-${d}`
}

/** Normaliza planning_date a YYYY-MM-DD. */
export function planDateYmd(value: string | null | undefined): string | null {
  if (!value) return null
  const d = String(value).trim().slice(0, 10)
  if (!/^\d{4}-\d{2}-\d{2}$/.test(d)) return null
  return d
}

export function bucketPlanDate(
  planningDate: string | null | undefined,
  todayYmd: string = santiagoTodayYmd(),
): PlanDateBucket {
  const ymd = planDateYmd(planningDate)
  if (!ymd) return "unknown"
  if (ymd === todayYmd) return "today"
  if (ymd > todayYmd) return "upcoming"
  return "history"
}

export function partitionPlansByDate<T extends { planning_date?: string | null }>(
  items: T[],
  todayYmd: string = santiagoTodayYmd(),
): { today: T[]; upcoming: T[]; history: T[]; unknown: T[] } {
  const today: T[] = []
  const upcoming: T[] = []
  const history: T[] = []
  const unknown: T[] = []
  for (const p of items) {
    const b = bucketPlanDate(p.planning_date, todayYmd)
    if (b === "today") today.push(p)
    else if (b === "upcoming") upcoming.push(p)
    else if (b === "history") history.push(p)
    else unknown.push(p)
  }
  const byDateAsc = (a: T, b: T) =>
    String(planDateYmd(a.planning_date) || "").localeCompare(
      String(planDateYmd(b.planning_date) || ""),
    )
  const byDateDesc = (a: T, b: T) => -byDateAsc(a, b)
  today.sort(byDateDesc)
  upcoming.sort(byDateAsc)
  history.sort(byDateDesc)
  return { today, upcoming, history, unknown }
}
