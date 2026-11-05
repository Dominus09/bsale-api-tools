import assert from "node:assert/strict"
import { describe, it } from "node:test"

import { formatPeonetasDisplay } from "./peonetas-display.ts"

describe("formatPeonetasDisplay", () => {
  it("shows No informado for historical zero", () => {
    assert.equal(formatPeonetasDisplay({ assistant_count: 0 }), "No informado")
    assert.equal(
      formatPeonetasDisplay({ assistant_count: 0, peonetas_recorded: false }),
      "No informado",
    )
  })

  it("shows 0 when explicitly recorded", () => {
    assert.equal(
      formatPeonetasDisplay({ assistant_count: 0, peonetas_recorded: true }),
      "0",
    )
  })

  it("shows positive ORS counts without flag", () => {
    assert.equal(formatPeonetasDisplay({ assistant_count: 2 }), "2")
  })
})
