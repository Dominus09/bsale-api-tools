import assert from "node:assert/strict"
import { describe, it } from "node:test"
import {
  FAMILY_WHATSAPP_INVITE_URL,
  buildWhatsappTextUrl,
} from "./family-whatsapp.ts"

describe("family whatsapp demo", () => {
  it("expone el enlace autorizado del grupo familiar", () => {
    assert.match(FAMILY_WHATSAPP_INVITE_URL, /^https:\/\/chat\.whatsapp\.com\//)
    assert.ok(FAMILY_WHATSAPP_INVITE_URL.includes("K62IagsvHLD6ehQCknBnOI"))
  })

  it("arma wa.me con texto precargado", () => {
    const url = buildWhatsappTextUrl("Hola\nMundo")
    assert.match(url, /^https:\/\/wa\.me\/\?text=/)
    assert.ok(url.includes(encodeURIComponent("Hola\nMundo")))
  })
})
