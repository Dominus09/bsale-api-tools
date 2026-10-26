/** Fechas civiles en zona America/Santiago (sin usar UTC crudo para día/mes). */

export const CHILE_TIMEZONE = "America/Santiago"

export type ReceptionPeriodPreset = "current_month" | "previous_month" | "last_90" | "custom"

const chileDateFmt = new Intl.DateTimeFormat("en-CA", {
  timeZone: CHILE_TIMEZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
})

/** Hoy (YYYY-MM-DD) según el calendario de Chile. */
export function chileTodayIso(now: Date = new Date()): string {
  return chileDateFmt.format(now)
}

function pad2(n: number): string {
  return String(n).padStart(2, "0")
}

function lastDayOfMonth(year: number, month1to12: number): number {
  // Date.UTC(y, month1to12, 0) → último día del mes month1to12 (índice 1..12 como día 0 del mes siguiente).
  return new Date(Date.UTC(year, month1to12, 0)).getUTCDate()
}

/** Mes en curso en Chile: desde el día 1 hasta hoy. */
export function chileCurrentMonthRange(now: Date = new Date()): { from: string; to: string } {
  const to = chileTodayIso(now)
  return { from: `${to.slice(0, 7)}-01`, to }
}

/** Mes calendario anterior completo en Chile. */
export function chilePreviousMonthRange(now: Date = new Date()): { from: string; to: string } {
  const today = chileTodayIso(now)
  const y = Number(today.slice(0, 4))
  const m = Number(today.slice(5, 7))
  const prevMonth = m === 1 ? 12 : m - 1
  const prevYear = m === 1 ? y - 1 : y
  const last = lastDayOfMonth(prevYear, prevMonth)
  return {
    from: `${prevYear}-${pad2(prevMonth)}-01`,
    to: `${prevYear}-${pad2(prevMonth)}-${pad2(last)}`,
  }
}

/** Últimos 90 días inclusive del extremo inferior (hoy − 90) hasta hoy en Chile. */
export function chileLast90DaysRange(now: Date = new Date()): { from: string; to: string } {
  const to = chileTodayIso(now)
  const y = Number(to.slice(0, 4))
  const m = Number(to.slice(5, 7))
  const d = Number(to.slice(8, 10))
  // Mediodía UTC del día civil chileno evita saltos por DST al restar días.
  const utcNoon = Date.UTC(y, m - 1, d, 12, 0, 0)
  const fromDate = new Date(utcNoon - 90 * 86_400_000)
  const from = `${fromDate.getUTCFullYear()}-${pad2(fromDate.getUTCMonth() + 1)}-${pad2(fromDate.getUTCDate())}`
  return { from, to }
}

export function rangeForReceptionPreset(
  preset: Exclude<ReceptionPeriodPreset, "custom">,
  now: Date = new Date(),
): { from: string; to: string } {
  if (preset === "current_month") return chileCurrentMonthRange(now)
  if (preset === "previous_month") return chilePreviousMonthRange(now)
  return chileLast90DaysRange(now)
}

export const RECEPTION_PERIOD_OPTIONS: { value: ReceptionPeriodPreset; label: string }[] = [
  { value: "current_month", label: "Mes actual" },
  { value: "previous_month", label: "Mes anterior" },
  { value: "last_90", label: "Últimos 90 días" },
  { value: "custom", label: "Personalizado" },
]
