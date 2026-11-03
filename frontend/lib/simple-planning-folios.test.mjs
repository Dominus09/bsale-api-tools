/**
 * Ejecutar: node --test frontend/lib/simple-planning-folios.test.mjs
 * Réplica mínima de parseo (alineada con simple-planning-folios.ts).
 */

import assert from "node:assert/strict"
import { describe, it } from "node:test"

const SIMPLE_PLANNING_MAX_FOLIOS = 200

function cleanRaw(raw) {
  if (raw == null) return ""
  let s = String(raw).replace(/\u00a0/g, " ").trim()
  if (s.endsWith(".0") && /^\d+\.0$/.test(s)) s = s.slice(0, -2)
  return s
}

function digitsOnly(s) {
  return s.replace(/[^\d]/g, "")
}

function parseFolioToken(raw) {
  const input = cleanRaw(raw)
  if (!input) return { ok: false, input: "", reason: "empty" }
  const digits = digitsOnly(input)
  if (!digits || !/^\d+$/.test(digits)) return { ok: false, input, reason: "invalid" }
  const folio = Number(digits)
  if (!Number.isSafeInteger(folio) || folio <= 0) return { ok: false, input, reason: "invalid" }
  return { ok: true, folio, input }
}

function parseFoliosFromText(text) {
  const lines = text.split(/\r?\n/)
  const folios = []
  const issues = []
  const seen = new Map()
  let skippedEmpty = 0
  for (const raw of lines) {
    const parsed = parseFolioToken(raw)
    if (!parsed.ok) {
      if (parsed.reason === "empty") {
        skippedEmpty += 1
        continue
      }
      issues.push({ kind: "invalid", input: parsed.input })
      continue
    }
    if (seen.has(parsed.folio)) {
      issues.push({ kind: "duplicate", folio: parsed.folio })
      continue
    }
    if (folios.length >= SIMPLE_PLANNING_MAX_FOLIOS) {
      issues.push({ kind: "too_many", folio: parsed.folio })
      break
    }
    seen.set(parsed.folio, folios.length)
    folios.push(parsed.folio)
  }
  return { folios, issues, skippedEmpty }
}

function uniqueClientCount(orders) {
  const keys = new Set()
  for (const o of orders) {
    const id = o.client?.client_id
    if (id != null) {
      keys.add(`id:${id}`)
      continue
    }
    const name = (o.client?.client_name || "").trim().toLowerCase()
    if (name) keys.add(`n:${name}`)
  }
  return keys.size
}

describe("simple-planning folios parse", () => {
  it("accepts plain and excel-style numbers", () => {
    assert.deepEqual(parseFolioToken("70625"), { ok: true, folio: 70625, input: "70625" })
    assert.deepEqual(parseFolioToken("70625.0"), { ok: true, folio: 70625, input: "70625" })
    assert.equal(parseFolioToken(" 70 625 ").ok, true)
  })

  it("rejects invalid tokens", () => {
    assert.equal(parseFolioToken("abc").ok, false)
    assert.equal(parseFolioToken("0").ok, false)
    assert.equal(parseFolioToken("").reason, "empty")
  })

  it("dedupes and skips empties", () => {
    const r = parseFoliosFromText("100\n\n200\n100\nxyz\n300")
    assert.deepEqual(r.folios, [100, 200, 300])
    assert.ok(r.skippedEmpty >= 1)
    assert.ok(r.issues.some((i) => i.kind === "duplicate"))
    assert.ok(r.issues.some((i) => i.kind === "invalid"))
  })

  it("caps at max folios", () => {
    const lines = Array.from({ length: SIMPLE_PLANNING_MAX_FOLIOS + 5 }, (_, i) => String(i + 1))
    const r = parseFoliosFromText(lines.join("\n"))
    assert.equal(r.folios.length, SIMPLE_PLANNING_MAX_FOLIOS)
    assert.ok(r.issues.some((i) => i.kind === "too_many"))
  })

  it("unique clients", () => {
    assert.equal(
      uniqueClientCount([
        { client: { client_id: 1, client_name: "A" } },
        { client: { client_id: 1, client_name: "A" } },
        { client: { client_id: 2, client_name: "B" } },
        { client: { client_name: "C" } },
      ]),
      3,
    )
  })
})
