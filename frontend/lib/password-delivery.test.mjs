/**
 * Enlace de restablecimiento / invitación.
 * node --test frontend/lib/password-delivery.test.mjs
 */

import assert from "node:assert/strict"
import { describe, it } from "node:test"

function buildPasswordResetUrl(delivery, origin = "https://work.quillotana.cl") {
  const path =
    delivery.one_time_path ||
    `/restablecer-contrasena?token=${encodeURIComponent(delivery.one_time_token)}`
  return `${origin}${path.startsWith("/") ? path : `/${path}`}`
}

describe("password delivery URL", () => {
  it("usa one_time_path del backend", () => {
    const url = buildPasswordResetUrl({
      one_time_token: "abc",
      one_time_path: "/restablecer-contrasena?token=abc%2Fxyz",
    })
    assert.equal(url, "https://work.quillotana.cl/restablecer-contrasena?token=abc%2Fxyz")
  })

  it("arma path si solo hay token", () => {
    const url = buildPasswordResetUrl({ one_time_token: "tok+1" })
    assert.match(url, /\/restablecer-contrasena\?token=tok%2B1$/)
  })

  it("no abre ventana: es solo string de URL", () => {
    const url = buildPasswordResetUrl({ one_time_token: "x" })
    assert.equal(typeof url, "string")
    assert.ok(!url.includes("javascript:"))
  })
})
