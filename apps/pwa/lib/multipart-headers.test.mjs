import assert from "node:assert/strict"
import { describe, it } from "node:test"
import {
  hasJsonContentType,
  multipartAuthHeaders,
} from "./multipart-headers.ts"

describe("multipartAuthHeaders", () => {
  it("elimina Content-Type application/json (causa raíz iPhone firma)", () => {
    const h = multipartAuthHeaders({
      Accept: "application/json",
      "Content-Type": "application/json",
      Authorization: "Bearer x",
    })
    assert.equal(h["Content-Type"], undefined)
    assert.equal(h["content-type"], undefined)
    assert.equal(h.Authorization, "Bearer x")
    assert.equal(h.Accept, "application/json")
    assert.equal(hasJsonContentType(h), false)
  })

  it("detecta JSON content-type peligroso para FormData", () => {
    assert.equal(
      hasJsonContentType({ "Content-Type": "application/json" }),
      true,
    )
  })
})
