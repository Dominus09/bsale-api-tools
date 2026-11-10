import assert from "node:assert/strict"
import { describe, it } from "node:test"
import {
  FAMILY_WHATSAPP_INVITE_URL,
  OPERATIONAL_WHATSAPP_INVITE_URL,
  buildWhatsappInviteWithText,
  buildWhatsappTextUrl,
  isUsableWhatsappInviteUrl,
  resolveWhatsappSendUrl,
} from "./family-whatsapp.ts"

/** Invite sintético (no es el enlace real operativo). */
const SAMPLE_OPS_INVITE =
  "https://chat.whatsapp.com/SAMPLEOPSGROUP01?s=cl&p=i&mlu=4&ilr=4&iam=2"

const COMPACT_REPORT = [
  "✅ Entrega OK",
  "Cliente: Café Ñuñoá",
  "Docs: F-123",
  "Cobranza: $12.500",
  "Chofer: Juan",
  "15:30",
].join("\n")

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
    assert.equal(parsed.searchParams.get("p"), "i")
    assert.equal(parsed.searchParams.get("mlu"), "4")
    assert.equal(parsed.searchParams.get("ilr"), "4")
    assert.equal(parsed.searchParams.get("iam"), "2")
    assert.equal(parsed.searchParams.getAll("text").length, 1)
  })

  it("conserva emojis, tildes y saltos de línea en text", () => {
    const url = buildWhatsappInviteWithText(SAMPLE_OPS_INVITE, COMPACT_REPORT)
    const parsed = new URL(url)
    assert.equal(parsed.searchParams.get("text"), COMPACT_REPORT)
    assert.ok(parsed.searchParams.get("text").includes("Ñuñoá"))
    assert.ok(parsed.searchParams.get("text").includes("✅"))
    assert.equal(parsed.searchParams.get("text").split("\n").length, 6)
  })

  it("reemplaza text previo en lugar de duplicarlo", () => {
    const base = "https://chat.whatsapp.com/ABC?text=viejo&s=cl"
    const url = buildWhatsappInviteWithText(base, "nuevo")
    const parsed = new URL(url)
    assert.equal(parsed.searchParams.get("text"), "nuevo")
    assert.equal(parsed.searchParams.getAll("text").length, 1)
    assert.equal(parsed.searchParams.get("s"), "cl")
  })

  it("valida invite usable", () => {
    assert.equal(isUsableWhatsappInviteUrl(""), false)
    assert.equal(isUsableWhatsappInviteUrl("   "), false)
    assert.equal(isUsableWhatsappInviteUrl("not-a-url"), false)
    assert.equal(isUsableWhatsappInviteUrl("ftp://chat.whatsapp.com/x"), false)
    assert.equal(isUsableWhatsappInviteUrl(SAMPLE_OPS_INVITE), true)
  })

  it("demo usa solo el grupo familiar", () => {
    const res = resolveWhatsappSendUrl("demo msg", "demo", SAMPLE_OPS_INVITE)
    assert.equal(res.destination, "family_invite")
    assert.ok(res.url.startsWith("https://chat.whatsapp.com/K62IagsvHLD6ehQCknBnOI"))
    assert.equal(new URL(res.url).searchParams.get("text"), "demo msg")
    assert.ok(!res.url.includes("SAMPLEOPSGROUP01"))
  })

  it("real sin ops usa wa.me y nunca el familiar", () => {
    assert.equal(OPERATIONAL_WHATSAPP_INVITE_URL, "")
    const res = resolveWhatsappSendUrl("cliente real", "real", "")
    assert.equal(res.destination, "wa_me")
    assert.match(res.url, /^https:\/\/wa\.me\/\?text=/)
    assert.ok(!res.url.includes("chat.whatsapp.com"))
    assert.ok(!res.url.includes("K62IagsvHLD6ehQCknBnOI"))
  })

  it("real con ops configurada usa invite + text precargado", () => {
    const res = resolveWhatsappSendUrl(COMPACT_REPORT, "real", SAMPLE_OPS_INVITE)
    assert.equal(res.destination, "ops_invite")
    const parsed = new URL(res.url)
    assert.equal(parsed.origin + parsed.pathname, "https://chat.whatsapp.com/SAMPLEOPSGROUP01")
    assert.equal(parsed.searchParams.get("text"), COMPACT_REPORT)
    assert.equal(parsed.searchParams.get("s"), "cl")
    assert.ok(!res.url.includes("K62IagsvHLD6ehQCknBnOI"))
  })

  it("real con URL inválida cae a wa.me", () => {
    const res = resolveWhatsappSendUrl("cliente real", "real", "no-es-url")
    assert.equal(res.destination, "wa_me")
    assert.match(res.url, /^https:\/\/wa\.me\/\?text=/)
  })
})
