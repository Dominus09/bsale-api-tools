import assert from "node:assert/strict"
import test from "node:test"
import { clearSession, getSessionEmail, getToken, setSession } from "./auth.ts"

test("setSession / getToken / clearSession limpian credenciales PWA", () => {
  const store = new Map()
  const session = new Map()
  globalThis.localStorage = {
    getItem: (k) => (store.has(k) ? store.get(k) : null),
    setItem: (k, v) => store.set(k, String(v)),
    removeItem: (k) => store.delete(k),
  }
  globalThis.sessionStorage = {
    getItem: (k) => (session.has(k) ? session.get(k) : null),
    setItem: (k, v) => session.set(k, String(v)),
    removeItem: (k) => session.delete(k),
    clear: () => session.clear(),
  }
  globalThis.window = globalThis

  setSession("tok-abc", "chofer@quillotana.cl", 42)
  assert.equal(getToken(), "tok-abc")
  assert.equal(getSessionEmail(), "chofer@quillotana.cl")
  session.set("geo_pref_plan_1", "1")

  clearSession()
  assert.equal(getToken(), null)
  assert.equal(getSessionEmail(), null)
  assert.equal(store.has("pwa_user_id"), false)
  assert.equal(session.size, 0)
})
