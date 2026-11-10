import assert from "node:assert/strict"
import { describe, it } from "node:test"
import {
  FAMILY_WHATSAPP_INVITE_URL,
  OPERATIONAL_WHATSAPP_INVITE_URL,
  buildWhatsappInviteWithText,
  buildWhatsappTextUrl,
  resolveWhatsappSendUrl,
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

  it("conserva params del invite y fija text sin duplicar", () => {
    const message = "L1\nL2\nL3\nL4\nL5\nL6"
    const url = buildWhatsappInviteWithText(FAMILY_WHATSAPP_INVITE_URL, message)
    const parsed = new URL(url)
    assert.equal(parsed.origin + parsed.pathname, "https://chat.whatsapp.com/K62IagsvHLD6ehQCknBnOI")
    assert.equal(parsed.searchParams.get("text"), message)
    assert.equal(parsed.searchParams.get("s"), "cl")
    assert.equal(parsed.searchParams.getAll("text").length, 1)
  })

  it("reemplaza text previo en lugar de duplicarlo", () => {
    const base = "https://chat.whatsapp.com/ABC?text=viejo&s=cl"
    const url = buildWhatsappInviteWithText(base, "nuevo")
    const parsed = new URL(url)
    assert.equal(parsed.searchParams.get("text"), "nuevo")
    assert.equal(parsed.searchParams.getAll("text").length, 1)
    assert.equal(parsed.searchParams.get("s"), "cl")
  })

  it("demo usa solo el grupo familiar", () => {
    const res = resolveWhatsappSendUrl("demo msg", "demo")
    assert.equal(res.destination, "family_invite")
    assert.ok(res.url.startsWith("https://chat.whatsapp.com/K62IagsvHLD6ehQCknBnOI"))
    assert.equal(new URL(res.url).searchParams.get("text"), "demo msg")
  })

  it("real no usa el grupo familiar cuando no hay ops", () => {
    assert.equal(OPERATIONAL_WHATSAPP_INVITE_URL, "")
    const res = resolveWhatsappSendUrl("cliente real", "real")
    assert.equal(res.destination, "wa_me")
    assert.match(res.url, /^https:\/\/wa\.me\/\?text=/)
    assert.ok(!res.url.includes("chat.whatsapp.com"))
    assert.ok(!res.url.includes("K62IagsvHLD6ehQCknBnOI"))
  })
})
