import assert from "node:assert/strict"
import test from "node:test"
import { readFileSync } from "node:fs"
import { dirname, join } from "node:path"
import { fileURLToPath } from "node:url"

const root = join(dirname(fileURLToPath(import.meta.url)), "..")

test("AppMenu porta el drawer a document.body (evita containing block del topbar)", () => {
  const src = readFileSync(join(root, "components/app-menu.tsx"), "utf8")
  assert.match(src, /createPortal/)
  assert.match(src, /document\.body/)
  assert.match(src, /logoutAndRedirect/)
  assert.match(src, /Cerrar sesión/)
})

test("CSS drawer documenta containing block y usa 100dvh + z-index alto", () => {
  const css = readFileSync(join(root, "app/globals.css"), "utf8")
  assert.match(css, /containing block/)
  assert.match(css, /100dvh/)
  assert.match(css, /z-index:\s*200/)
  assert.match(css, /\.drawer-footer[\s\S]*flex:\s*0 0 auto/)
})
