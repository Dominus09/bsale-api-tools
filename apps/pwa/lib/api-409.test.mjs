import assert from "node:assert/strict"
import { describe, it } from "node:test"
import { readFileSync } from "node:fs"
import { fileURLToPath } from "node:url"
import { dirname, join } from "node:path"

const here = dirname(fileURLToPath(import.meta.url))
const apiSrc = readFileSync(join(here, "api.ts"), "utf8")

describe("ApiError 409 structured detail (fuente)", () => {
  it("ApiError expone code opcional", () => {
    assert.match(apiSrc, /export class ApiError extends Error/)
    assert.match(apiSrc, /code: string \| null/)
    assert.match(apiSrc, /constructor\(status: number, message: string, code: string \| null = null\)/)
  })

  it("parseError lee detail.code y detail.message", () => {
    assert.match(apiSrc, /obj\.code/)
    assert.match(apiSrc, /obj\.message \|\| obj\.message_es/)
    assert.match(apiSrc, /throw new ApiError\(res\.status, parsed\.message, parsed\.code\)/)
  })
})
