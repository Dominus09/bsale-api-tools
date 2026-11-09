import assert from "node:assert/strict"
import test from "node:test"
import { formatRutDisplay } from "./format-rut.ts"

test("formatRutDisplay con y sin guion", () => {
  assert.equal(formatRutDisplay("12345678-5"), "12.345.678-5")
  assert.equal(formatRutDisplay("123456785"), "12.345.678-5")
  assert.equal(formatRutDisplay("1-9"), "1-9")
  assert.equal(formatRutDisplay(""), "")
  assert.equal(formatRutDisplay(null), "")
})
