/**
 * Offline V2 2A.5 — Playwright + IndexedDB persistente (NO memory store).
 * Requiere: Next lab (3011) + FastAPI lab (18765) + PG aislado.
 */
import { test, expect, chromium, type BrowserContext, type Page } from "@playwright/test"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"

const PWA = process.env.LAB_PWA_URL || "http://127.0.0.1:3015"
const API = process.env.LAB_API_URL || "http://127.0.0.1:18765"
const LAB_PATH = "/pruebas/offline-v2-lab"

async function waitLab(page: Page) {
  await page.goto(`${PWA}${LAB_PATH}`)
  await page.waitForFunction(() => !!(window as any).__LQ_OFFLINE_LAB, null, {
    timeout: 60_000,
  })
}

async function inspect(page: Page) {
  return page.evaluate(async () => {
    const lab = (window as any).__LQ_OFFLINE_LAB
    return lab.inspectOutbox()
  })
}

test.describe.configure({ mode: "serial" })

test("2A.5 IndexedDB persistente + sync único a PG lab", async () => {
  // Health API lab
  const health = await fetch(`${API}/health`)
  expect(health.ok).toBeTruthy()

  const userDataDir = fs.mkdtempSync(path.join(os.tmpdir(), "lq-2a5-idb-"))
  let context: BrowserContext | null = null

  try {
    context = await chromium.launchPersistentContext(userDataDir, {
      headless: true,
      args: ["--disable-dev-shm-usage"],
    })
    const page = await context.newPage()

    // --- Online: sesión lab ---
    await waitLab(page)
    await page.evaluate(() => {
      const lab = (window as any).__LQ_OFFLINE_LAB
      lab.seedSession()
      lab.enableLabSyncFlag()
    })

    // --- Offline: registrar 5 ops en IndexedDB REAL ---
    await context.setOffline(true)
    const reg = await page.evaluate(async () => {
      const lab = (window as any).__LQ_OFFLINE_LAB
      return lab.registerFiveOfflineOps()
    })
    expect(reg.ok, reg.error || "register failed").toBeTruthy()
    expect(reg.pending).toBe(5)
    expect(reg.photoBytes).toBeGreaterThan(32)

    let snap = await inspect(page)
    expect(snap.total).toBe(5)
    expect(snap.pending).toBe(5)
    expect(snap.serverConfirmed).toBe(0)
    expect(snap.synced).toBe(0)
    const closed = snap.ops.find((o: any) => o.op_type === "closed_store")
    expect(closed?.hasPhoto).toBeTruthy()
    expect(closed?.photoBytes).toBeGreaterThan(32)

    // Local ≠ synced
    expect(snap.ops.every((o: any) => o.status === "pending")).toBeTruthy()
    expect(snap.ops.every((o: any) => o.server_confirmed === false)).toBeTruthy()

    // --- Cerrar app (contexto) y reabrir mismo perfil ---
    await context.close()
    context = null

    context = await chromium.launchPersistentContext(userDataDir, {
      headless: true,
      args: ["--disable-dev-shm-usage"],
    })
    const page2 = await context.newPage()
    await waitLab(page2)
    await page2.evaluate(() => {
      const lab = (window as any).__LQ_OFFLINE_LAB
      lab.seedSession()
      lab.enableLabSyncFlag()
    })

    snap = await inspect(page2)
    expect(snap.total).toBe(5)
    expect(snap.pending).toBe(5)
    const closed2 = snap.ops.find((o: any) => o.op_type === "closed_store")
    expect(closed2?.hasPhoto).toBeTruthy()
    expect(closed2?.photoBytes).toBe(closed?.photoBytes)

    // Aislamiento otro usuario
    const other = await page2.evaluate(async () => {
      const lab = (window as any).__LQ_OFFLINE_LAB
      return lab.switchUser("99")
    })
    expect(other.visible).toBe(0)
    await page2.evaluate(() => {
      const lab = (window as any).__LQ_OFFLINE_LAB
      lab.seedSession()
      lab.enableLabSyncFlag()
    })

    // --- Online + sync ---
    await context.setOffline(false)
    const dbg = await page2.evaluate(() => (window as any).__LQ_OFFLINE_LAB.debugEnv())
    console.log("lab debugEnv", dbg)
    expect(dbg.syncEnabled, `sync flag off: ${JSON.stringify(dbg)}`).toBeTruthy()
    expect(dbg.hasToken).toBeTruthy()
    const syncRes = await page2.evaluate(async () => {
      const lab = (window as any).__LQ_OFFLINE_LAB
      return lab.syncNow()
    })
    console.log("lab syncRes", syncRes)
    expect(syncRes.synced, `sync failed: ${JSON.stringify(syncRes)}`).toBe(5)
    expect(syncRes.pending).toBe(0)

    snap = await inspect(page2)
    expect(snap.synced).toBe(5)
    expect(snap.serverConfirmed).toBe(5)
    expect(snap.ops.every((o: any) => o.status === "synced")).toBeTruthy()
    expect(snap.ops.every((o: any) => o.server_confirmed === true)).toBeTruthy()

    // PG: 5 visitas, sin dupes
    const stats = await (await fetch(`${API}/lab/pg-stats`)).json()
    expect(stats.visits).toBe(5)
    expect(stats.ops_completed).toBe(5)
    expect(stats.evidence_confirmed).toBe(1)
    expect(Array.isArray(stats.r2_keys)).toBeTruthy()
    expect(stats.r2_keys.length).toBeGreaterThanOrEqual(1)

    // Segunda sync (replay): sigue 5
    const sync2 = await page2.evaluate(async () => {
      const lab = (window as any).__LQ_OFFLINE_LAB
      return lab.syncNow()
    })
    expect(sync2.synced).toBe(5)
    const stats2 = await (await fetch(`${API}/lab/pg-stats`)).json()
    expect(stats2.visits).toBe(5)
    expect(stats2.ops_completed).toBe(5)
  } finally {
    if (context) await context.close()
    try {
      fs.rmSync(userDataDir, { recursive: true, force: true })
    } catch {
      /* ignore */
    }
  }
})
