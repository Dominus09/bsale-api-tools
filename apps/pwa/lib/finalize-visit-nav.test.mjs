import assert from "node:assert/strict"
import { describe, it } from "node:test"
import {
  isClosedVisitStatus,
  resultadoHref,
} from "./finalize-visit-nav.ts"

describe("finalize-visit-nav", () => {
  it("arma href de resultado con visitId", () => {
    assert.equal(
      resultadoHref(82, "id:3575", 99),
      "/despachos/82/clientes/id%3A3575/entrega/resultado?visitId=99",
    )
  })

  it("detecta estados cerrados", () => {
    assert.equal(isClosedVisitStatus("completed"), true)
    assert.equal(isClosedVisitStatus("partial"), true)
    assert.equal(isClosedVisitStatus("in_progress"), false)
  })
})
