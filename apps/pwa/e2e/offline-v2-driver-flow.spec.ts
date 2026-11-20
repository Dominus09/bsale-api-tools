/**
 * 2A.5+ — circuito visual real del chofer (páginas offline-completa / cerrado).
 * No usa window.__LQ_OFFLINE_LAB para registrar ops.
 * Sync: OfflineSyncProvider + outboxSyncRuntime (mismo motor productivo).
 */
import { test, expect, chromium, type BrowserContext, type Page } from "@playwright/test"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"

const PWA = process.env.LAB_PWA_URL || "http://127.0.0.1:3015"
const API = process.env.LAB_API_URL || "http://127.0.0.1:18765"
const PLAN = 920

const CLIENTS = [
  { key: "id:9201", cid: 9201, doc: 91055, d1: 91001, d2: 91002 },
  { key: "id:9202", cid: 9202, doc: 91056, d1: 91003, d2: 91004 },
  { key: "id:9203", cid: 9203, doc: 91057, d1: 91005, d2: 91006 },
  { key: "id:9204", cid: 9204, doc: 91058, d1: 91007, d2: 91008 },
  { key: "id:9205", cid: 9205, doc: 91059, d1: 91009, d2: 91010 },
]

async function seedOfflineSnapshots(page: Page) {
  await page.goto(`${PWA}/login`)
  await page.evaluate(async (clients) => {
    localStorage.setItem("pwa_token", "lab-token-driver-flow")
    localStorage.setItem("pwa_email", "chofer@lab.local")
    localStorage.setItem("pwa_user_id", "42")
    localStorage.setItem("lq:offline:syncWrites", "1")

    function openKv(): Promise<IDBDatabase> {
      return new Promise((resolve, reject) => {
        const req = indexedDB.open("quillotana-ops-offline", 1)
        req.onupgradeneeded = () => {
          const db = req.result
          if (!db.objectStoreNames.contains("kv")) db.createObjectStore("kv")
        }
        req.onsuccess = () => resolve(req.result)
        req.onerror = () => reject(req.error)
      })
    }
    async function put(key: string, value: unknown) {
      const db = await openKv()
      await new Promise<void>((resolve, reject) => {
        const tx = db.transaction("kv", "readwrite")
        tx.objectStore("kv").put(value, key)
        tx.oncomplete = () => resolve()
        tx.onerror = () => reject(tx.error)
      })
    }
    const now = new Date().toISOString()
    await put("u:42:meta", {
      userId: "42",
      email: "chofer@lab.local",
      profiles: ["chofer"],
      syncedAt: now,
    })
    await put("u:42:plans", {
      items: [{ id: 920, planning_code: "PLAN-LAB-DRIVER", status: "confirmed" }],
      count: 1,
      syncedAt: now,
    })
    await put("u:42:plan:920:clients", {
      items: clients.map((c: any) => ({
        customer_key: c.key,
        client_id: c.cid,
        client_name: `Cliente ${c.cid}`,
      })),
      count: clients.length,
      syncedAt: now,
    })
    await put("u:42:plan:920:ready", {
      planId: 920,
      clientsCount: clients.length,
      detailsCount: clients.length,
      documentsCount: clients.length,
      syncedAt: now,
      dataComplete: true,
      navReady: true,
      complete: true,
      pickingId: 501,
      pickingVersion: "3",
    })
    for (const c of clients as any[]) {
      const safe = encodeURIComponent(c.key)
      await put(`u:42:plan:920:client:${safe}`, {
        payload: {
          client: {
            client_id: c.cid,
            customer_key: c.key,
            client_name: `Cliente ${c.cid}`,
          },
          document_count: 1,
          documents: [
            {
              related_document_id: c.doc,
              document_number: 1000 + c.cid,
              document_type: "GD",
              document_total: 1000,
              product_count: 2,
              products: [
                { source_detail_id: c.d1, unidades: 4, sku: "A", description: "A" },
                { source_detail_id: c.d2, unidades: 2, sku: "B", description: "B" },
              ],
            },
          ],
        },
        syncedAt: now,
      })
    }
  }, CLIENTS)
}

async function countOutbox(page: Page) {
  return page.evaluate(async () => {
    function openOps(): Promise<IDBDatabase> {
      return new Promise((resolve, reject) => {
        const req = indexedDB.open("quillotana-ops-outbox-v2", 1)
        req.onupgradeneeded = () => {
          const db = req.result
          if (!db.objectStoreNames.contains("ops")) {
            db.createObjectStore("ops", { keyPath: "localUuid" })
          }
          if (!db.objectStoreNames.contains("blobs")) {
            db.createObjectStore("blobs", { keyPath: "id" })
          }
        }
        req.onsuccess = () => resolve(req.result)
        req.onerror = () => reject(req.error)
      })
    }
    const db = await openOps()
    const ops: any[] = await new Promise((resolve, reject) => {
      const tx = db.transaction("ops", "readonly")
      const req = tx.objectStore("ops").getAll()
      req.onsuccess = () => resolve(req.result || [])
      req.onerror = () => reject(req.error)
    })
    const mine = ops.filter((o) => String(o.userId) === "42")
    const closed = mine.find((o) => o.op_type === "closed_store")
    let photoOk = false
    if (closed?.evidence?.localBlobId) {
      const blob: any = await new Promise((resolve, reject) => {
        const tx = db.transaction("blobs", "readonly")
        const req = tx.objectStore("blobs").get(closed.evidence.localBlobId)
        req.onsuccess = () => resolve(req.result || null)
        req.onerror = () => reject(req.error)
      })
      photoOk = !!blob && (blob.sizeBytes || 0) > 32
    }
    return {
      pending: mine.filter((o) =>
        ["pending", "syncing", "failed_needs_action"].includes(o.status),
      ).length,
      synced: mine.filter((o) => o.status === "synced" && o.server_confirmed).length,
      total: mine.length,
      photoOk,
    }
  })
}

test.describe.configure({ mode: "serial" })

test("flujo UI real: completa + cerrado + reopen + sync runtime", async () => {
  expect((await fetch(`${API}/health`)).ok).toBeTruthy()
  // Evitar conflictos con E2E previo en el mismo PG lab
  const reset = await fetch(`${API}/lab/reset-plan/${PLAN}`, { method: "POST" })
  expect(reset.ok).toBeTruthy()

  const userDataDir = fs.mkdtempSync(path.join(os.tmpdir(), "lq-2a5-driver-"))
  let context: BrowserContext | null = null
  try {
    context = await chromium.launchPersistentContext(userDataDir, {
      headless: true,
      args: ["--disable-dev-shm-usage"],
      geolocation: { latitude: -33.45, longitude: -70.66 },
      permissions: ["geolocation"],
    })
    const page = await context.newPage()
    await seedOfflineSnapshots(page)
    // No usar context.setOffline(true): en Chromium también corta localhost.
    // Simular offline de app: navigator.onLine=false + abort API.
    await page.addInitScript(() => {
      Object.defineProperty(navigator, "onLine", { configurable: true, get: () => false })
    })
    await page.route("**/api-upstream/**", (route) => route.abort())
    await page.route(`**/${new URL(API).host}/**`, (route) => route.abort())

    for (let i = 0; i < 4; i++) {
      const c = CLIENTS[i]
      await page.goto(
        `${PWA}/despachos/${PLAN}/clientes/${encodeURIComponent(c.key)}/entrega/offline-completa`,
      )
      await expect(page.getByText("Entrega completa local")).toBeVisible({
        timeout: 45_000,
      })
      await page.locator("select").selectOption("transfer_declared")
      await page.getByRole("button", { name: "Guardar en este dispositivo" }).click()
      await expect(
        page.getByText("Guardado en este dispositivo · Pendiente de sincronización"),
      ).toBeVisible({ timeout: 30_000 })
    }

    const closed = CLIENTS[4]
    await page.goto(
      `${PWA}/despachos/${PLAN}/clientes/${encodeURIComponent(closed.key)}/entrega/offline-cerrado`,
    )
    await expect(page.getByText("Local cerrado local")).toBeVisible({
      timeout: 45_000,
    })
    await page.locator("textarea").fill("Local cerrado laboratorio flujo real")
    const photoPath = path.join(userDataDir, "closed.jpg")
    fs.writeFileSync(
      photoPath,
      Buffer.concat([Buffer.from([0xff, 0xd8, 0xff, 0xe0]), Buffer.alloc(220, 9)]),
    )
    await page.locator('input[type="file"]').setInputFiles(photoPath)
    await expect(page.getByText(/Foto lista/)).toBeVisible({ timeout: 15_000 })
    await page.getByRole("button", { name: "Guardar en este dispositivo" }).click()
    await expect(
      page.getByText("Guardado en este dispositivo · Pendiente de sincronización"),
    ).toBeVisible({ timeout: 30_000 })

    let snap = await countOutbox(page)
    expect(snap.pending).toBe(5)
    expect(snap.photoOk).toBeTruthy()

    await context.close()
    context = null

    context = await chromium.launchPersistentContext(userDataDir, {
      headless: true,
      args: ["--disable-dev-shm-usage"],
      geolocation: { latitude: -33.45, longitude: -70.66 },
      permissions: ["geolocation"],
    })
    const page2 = await context.newPage()
    await page2.addInitScript(() => {
      localStorage.setItem("pwa_token", "lab-token-driver-flow")
      localStorage.setItem("pwa_email", "chofer@lab.local")
      localStorage.setItem("pwa_user_id", "42")
      localStorage.setItem("lq:offline:syncWrites", "1")
    })
    await page2.goto(`${PWA}/login`)
    snap = await countOutbox(page2)
    expect(snap.pending).toBe(5)
    expect(snap.photoOk).toBeTruthy()

    // Sync con el mismo outboxSyncRuntime (página lab solo como host del motor;
    // el registro de las 5 ops fue 100% UI offline-completa/cerrado).
    await page2.unrouteAll({ behavior: "ignoreErrors" })
    await page2.goto(`${PWA}/pruebas/offline-v2-lab`)
    await page2.waitForFunction(() => !!(window as any).__LQ_OFFLINE_LAB, null, {
      timeout: 60_000,
    })
    const syncRes = await page2.evaluate(async () => {
      const lab = (window as any).__LQ_OFFLINE_LAB
      lab.seedSession()
      lab.enableLabSyncFlag()
      return lab.syncNow()
    })
    expect(syncRes.synced, JSON.stringify(syncRes)).toBe(5)
    expect(syncRes.pending).toBe(0)

    const stats = await (await fetch(`${API}/lab/pg-stats`)).json()
    expect(stats.visits).toBe(5)
    expect(stats.ops_completed).toBe(5)
    expect(stats.evidence_confirmed).toBe(1)
  } finally {
    if (context) await context.close()
    try {
      fs.rmSync(userDataDir, { recursive: true, force: true })
    } catch {
      /* ignore */
    }
  }
})
