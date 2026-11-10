import assert from "node:assert/strict"
import { describe, it, beforeEach, afterEach } from "node:test"
import {
  CONNECT_ERROR_ES,
  getApiProxyTarget,
  isLikelyPublicApiHost,
} from "./api-proxy-target.ts"

const ORIG = { ...process.env }

beforeEach(() => {
  delete process.env.API_PROXY_TARGET
  delete process.env.NEXT_PUBLIC_API_URL
})

afterEach(() => {
  for (const k of ["API_PROXY_TARGET", "NEXT_PUBLIC_API_URL"]) {
    if (ORIG[k] === undefined) delete process.env[k]
    else process.env[k] = ORIG[k]
  }
})

describe("getApiProxyTarget", () => {
  it("prioriza API_PROXY_TARGET interno", () => {
    process.env.API_PROXY_TARGET = "http://quillotana-api:8000/"
    process.env.NEXT_PUBLIC_API_URL = "https://api.quillotana.cl"
    assert.equal(getApiProxyTarget(), "http://quillotana-api:8000")
  })

  it("cae a NEXT_PUBLIC_API_URL sin target interno", () => {
    process.env.NEXT_PUBLIC_API_URL = "https://api.quillotana.cl/"
    assert.equal(getApiProxyTarget(), "https://api.quillotana.cl")
  })

  it("fallback público por defecto", () => {
    assert.equal(getApiProxyTarget(), "https://api.quillotana.cl")
  })
})

describe("isLikelyPublicApiHost", () => {
  it("detecta host público Quillotana", () => {
    assert.equal(isLikelyPublicApiHost("https://api.quillotana.cl"), true)
    assert.equal(isLikelyPublicApiHost("http://quillotana-api:8000"), false)
  })
})

describe("CONNECT_ERROR_ES", () => {
  it("mensaje estable para el chofer", () => {
    assert.match(CONNECT_ERROR_ES, /No fue posible conectar con el servidor/)
  })
})
