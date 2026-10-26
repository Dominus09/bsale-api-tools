/**
 * Prueba rápida de fechas Chile (sin framework). Ejecutar:
 *   node frontend/lib/analytics-v2/chile-dates.test.mjs
 *
 * Nota: duplica la lógica mínima para no depender de transpile TS en CI local.
 */
import assert from "node:assert/strict"

const CHILE_TZ = "America/Santiago"
const fmt = new Intl.DateTimeFormat("en-CA", {
  timeZone: CHILE_TZ,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
})

function chileTodayIso(now) {
  return fmt.format(now)
}

function pad2(n) {
  return String(n).padStart(2, "0")
}

function lastDayOfMonth(year, month1to12) {
  return new Date(Date.UTC(year, month1to12, 0)).getUTCDate()
}

function chileCurrentMonthRange(now) {
  const to = chileTodayIso(now)
  return { from: `${to.slice(0, 7)}-01`, to }
}

function chilePreviousMonthRange(now) {
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

// Fijo: 2026-10-26 15:00 UTC = tarde en Chile (UTC-3) → aún 26-10
const midOct = new Date("2026-10-26T15:00:00.000Z")
assert.equal(chileTodayIso(midOct), "2026-10-26")
assert.deepEqual(chileCurrentMonthRange(midOct), { from: "2026-10-01", to: "2026-10-26" })
assert.deepEqual(chilePreviousMonthRange(midOct), { from: "2026-09-01", to: "2026-09-30" })

// Cambio de año
const jan5 = new Date("2026-01-05T15:00:00.000Z")
assert.deepEqual(chileCurrentMonthRange(jan5), { from: "2026-01-01", to: "2026-01-05" })
assert.deepEqual(chilePreviousMonthRange(jan5), { from: "2025-12-01", to: "2025-12-31" })

// Borde UTC cerca de medianoche Chile: 2026-10-27 02:30 UTC = 26-10 noche en Chile (UTC-3)
const nearMidnight = new Date("2026-10-27T02:30:00.000Z")
assert.equal(chileTodayIso(nearMidnight), "2026-10-26")
assert.equal(chileCurrentMonthRange(nearMidnight).from, "2026-10-01")

console.log("chile-dates OK")
