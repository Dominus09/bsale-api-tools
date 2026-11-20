import assert from "node:assert/strict"
import { describe, it } from "node:test"
import { readFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")

describe("api-base lab direct", () => {
  it("documenta LAB_API_DIRECT solo para localhost / lq-ops.dev", () => {
    const src = readFileSync(join(root, "lib/api-base.ts"), "utf8")
    assert.match(src, /NEXT_PUBLIC_LAB_API_DIRECT/)
    assert.match(src, /127\.0\.0\.1/)
    assert.match(src, /lq-ops\.dev/)
    assert.doesNotMatch(src, /quillotana\.cl.*LAB_API_DIRECT|LAB_API_DIRECT.*quillotana/)
  })
})
