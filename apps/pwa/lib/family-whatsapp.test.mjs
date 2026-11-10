import assert from "node:assert/strict"
import { describe, it } from "node:test"
import { FAMILY_WHATSAPP_INVITE_URL } from "./family-whatsapp.ts"

describe("family whatsapp demo", () => {
  it("expone el enlace autorizado del grupo familiar", () => {
    assert.match(FAMILY_WHATSAPP_INVITE_URL, /^https:\/\/chat\.whatsapp\.com\//)
    assert.ok(FAMILY_WHATSAPP_INVITE_URL.includes("K62IagsvHLD6ehQCknBnOI"))
  })
})
