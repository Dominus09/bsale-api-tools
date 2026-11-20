"use client"

/**
 * Laboratorio Offline V2 2A.5 — SOLO hosts no productivos.
 * Expone window.__LQ_OFFLINE_LAB sobre IndexedDB REAL (nunca memory store).
 * No escribe a api.quillotana.cl ni analytics.
 */

import { useEffect, useState } from "react"
import {
  isOutboxSyncWritesEnabled,
  isProductionQuillotanaHost,
  setOutboxSyncWritesLabFlag,
} from "@/lib/offline-outbox-policy"
import { setSession, clearSession, getSessionUserId, getToken } from "@/lib/auth"
import { getApiBase } from "@/lib/api-base"
import {
  saveCompleteDeliveryLocal,
  saveClosedStoreLocal,
  countPendingOutboxOps,
  listOutboxOpsVisibleToUser,
} from "@/lib/offline-outbox"
import {
  getOutboxBlob,
  listOutboxOpsForUser,
} from "@/lib/offline-outbox-store"
import { outboxSyncRuntime } from "@/lib/offline-outbox-sync"

const PLAN_ID = 920
const USER_ID = "42"

type LabApi = {
  seedSession: () => void
  clearSessionOnly: () => void
  enableLabSyncFlag: () => void
  registerFiveOfflineOps: () => Promise<{
    ok: boolean
    pending: number
    photoBytes: number | null
    error?: string
  }>
  inspectOutbox: () => Promise<{
    pending: number
    total: number
    synced: number
    conflicts: number
    serverConfirmed: number
    ops: Array<{
      localUuid: string
      status: string
      op_type: string
      customerKey: string
      server_confirmed: boolean
      hasPhoto: boolean
      photoBytes: number | null
    }>
  }>
  syncNow: () => Promise<{
    pending: number
    synced: number
    message: string | null
    phase: string
    apiBase: string
    syncEnabled: boolean
    hasToken: boolean
  }>
  debugEnv: () => {
    apiBase: string
    syncEnabled: boolean
    hasToken: boolean
    host: string
  }
  switchUser: (userId: string) => Promise<{ visible: number }>
}

declare global {
  interface Window {
    __LQ_OFFLINE_LAB?: LabApi
  }
}

function sampleFicha(docId: number, d1: number, d2: number) {
  return {
    client: { client_id: 9201, client_name: "Cliente Lab" },
    document_count: 1,
    documents: [
      {
        related_document_id: docId,
        document_number: 1001,
        document_type: "GD",
        document_total: 1000,
        product_count: 2,
        products: [
          { source_detail_id: d1, unidades: 4, sku: "A", description: "A" },
          { source_detail_id: d2, unidades: 2, sku: "B", description: "B" },
        ],
      },
    ],
  }
}

const CLIENTS = [
  { key: "id:9201", cid: 9201, doc: 91055, d1: 91001, d2: 91002 },
  { key: "id:9202", cid: 9202, doc: 91056, d1: 91003, d2: 91004 },
  { key: "id:9203", cid: 9203, doc: 91057, d1: 91005, d2: 91006 },
  { key: "id:9204", cid: 9204, doc: 91058, d1: 91007, d2: 91008 },
  { key: "id:9205", cid: 9205, doc: 91059, d1: 91009, d2: 91010 },
]

export default function OfflineV2LabPage() {
  const [blocked, setBlocked] = useState(true)
  const [status, setStatus] = useState("Inicializando…")

  useEffect(() => {
    const host = window.location.hostname
    if (isProductionQuillotanaHost(host)) {
      setBlocked(true)
      setStatus("Bloqueado en hosts *.quillotana.cl")
      return
    }
    setBlocked(false)

    const api: LabApi = {
      seedSession: () => {
        setSession("lab-token-2a5", "chofer@lab.local", USER_ID)
      },
      clearSessionOnly: () => {
        clearSession()
      },
      enableLabSyncFlag: () => {
        setOutboxSyncWritesLabFlag(true)
      },
      registerFiveOfflineOps: async () => {
        try {
          // Garantiza no usar memory store (undefined / false)
          const store = await import("@/lib/offline-outbox-store")
          store.useOutboxMemoryStoreForTests(false)

          for (let i = 0; i < 4; i++) {
            const c = CLIENTS[i]
            const res = await saveCompleteDeliveryLocal({
              userId: USER_ID,
              planId: PLAN_ID,
              customerKey: c.key,
              clientId: c.cid,
              clientName: `Cliente ${c.cid}`,
              fichaPayload: sampleFicha(c.doc, c.d1, c.d2),
              picking: { pickingId: 501, pickingVersion: "3" },
              payment: { method: "transfer_declared" },
              gps: {
                lat: -33.45,
                lng: -70.66,
                accuracy_m: 12,
                captured_at: new Date().toISOString(),
                source: "deferred_offline",
              },
              note: `Entrega lab ${i}`,
            })
            if (!res.ok) {
              return { ok: false, pending: 0, photoBytes: null, error: res.message }
            }
          }
          const closed = CLIENTS[4]
          const photo = new Uint8Array(256)
          photo[0] = 0xff
          photo[1] = 0xd8
          photo[2] = 0xff
          for (let i = 3; i < photo.length; i++) photo[i] = (i * 17) % 256
          const cres = await saveClosedStoreLocal({
            userId: USER_ID,
            planId: PLAN_ID,
            customerKey: closed.key,
            clientId: closed.cid,
            clientName: "Local cerrado lab",
            picking: { pickingId: 501, pickingVersion: "3" },
            note: "Local cerrado laboratorio 2A.5",
            gps: {
              lat: -33.4502,
              lng: -70.6602,
              accuracy_m: 18,
              captured_at: new Date().toISOString(),
              source: "deferred_offline",
            },
            photo: {
              bytes: photo.buffer,
              contentType: "image/jpeg",
              capturedAt: new Date().toISOString(),
            },
          })
          if (!cres.ok) {
            return { ok: false, pending: 0, photoBytes: null, error: cres.message }
          }
          const pending = await countPendingOutboxOps(USER_ID)
          const blobId = cres.op.evidence?.localBlobId
          const blob = blobId ? await getOutboxBlob(blobId) : null
          return {
            ok: true,
            pending,
            photoBytes: blob?.sizeBytes ?? null,
          }
        } catch (e) {
          return {
            ok: false,
            pending: 0,
            photoBytes: null,
            error: e instanceof Error ? e.message : String(e),
          }
        }
      },
      inspectOutbox: async () => {
        const ops = await listOutboxOpsForUser(USER_ID)
        const detailed = []
        for (const op of ops) {
          let photoBytes: number | null = null
          let hasPhoto = false
          if (op.evidence?.localBlobId) {
            const blob = await getOutboxBlob(op.evidence.localBlobId)
            hasPhoto = !!blob
            photoBytes = blob?.sizeBytes ?? null
          }
          detailed.push({
            localUuid: op.localUuid,
            status: op.status,
            op_type: op.op_type,
            customerKey: op.customerKey,
            server_confirmed: op.server_confirmed,
            hasPhoto,
            photoBytes,
          })
        }
        return {
          pending: ops.filter((o) => o.status === "pending" || o.status === "syncing")
            .length,
          total: ops.length,
          synced: ops.filter((o) => o.status === "synced").length,
          conflicts: ops.filter((o) => o.status === "conflict").length,
          serverConfirmed: ops.filter((o) => o.server_confirmed).length,
          ops: detailed,
        }
      },
      debugEnv: () => ({
        apiBase: getApiBase(),
        syncEnabled: isOutboxSyncWritesEnabled(),
        hasToken: !!getToken(),
        host: window.location.hostname,
      }),
      syncNow: async () => {
        setOutboxSyncWritesLabFlag(true)
        await outboxSyncRuntime.syncNow()
        const insp = await api.inspectOutbox()
        const st = outboxSyncRuntime.getState()
        return {
          pending: insp.pending,
          synced: insp.synced,
          message: st.lastResult || st.message,
          phase: st.phase,
          apiBase: getApiBase(),
          syncEnabled: isOutboxSyncWritesEnabled(),
          hasToken: !!getToken(),
        }
      },
      switchUser: async (userId: string) => {
        setSession("lab-token-other", "otro@lab.local", userId)
        const visible = await listOutboxOpsVisibleToUser(userId)
        return { visible: visible.length }
      },
    }

    window.__LQ_OFFLINE_LAB = api
    setStatus(
      `Lab listo · user=${getSessionUserId() || "sin sesión"} · IndexedDB real`,
    )
    return () => {
      delete window.__LQ_OFFLINE_LAB
    }
  }, [])

  if (blocked) {
    return (
      <main style={{ padding: 24, fontFamily: "system-ui" }}>
        <h1>Offline V2 Lab</h1>
        <p>{status}</p>
      </main>
    )
  }

  return (
    <main style={{ padding: 24, fontFamily: "system-ui", maxWidth: 640 }}>
      <h1>Offline V2 — laboratorio 2A.5</h1>
      <p data-testid="lab-status">{status}</p>
      <p>
        IndexedDB: <code>quillotana-ops-outbox-v2</code> (persistente). Memory store
        desactivado.
      </p>
      <ol>
        <li>Playwright llama a window.__LQ_OFFLINE_LAB</li>
        <li>Registra 5 ops offline con foto en IDB</li>
        <li>Cierra/reabre navegador → conserva ops + foto</li>
        <li>Sync contra FastAPI lab + PG aislado</li>
      </ol>
      <button
        type="button"
        data-testid="lab-seed"
        onClick={() => {
          window.__LQ_OFFLINE_LAB?.seedSession()
          window.__LQ_OFFLINE_LAB?.enableLabSyncFlag()
          setStatus("Sesión lab + flag sync local")
        }}
      >
        Seed sesión lab
      </button>
      <button
        type="button"
        data-testid="lab-register"
        style={{ marginLeft: 8 }}
        onClick={async () => {
          const r = await window.__LQ_OFFLINE_LAB?.registerFiveOfflineOps()
          setStatus(JSON.stringify(r))
        }}
      >
        Registrar 5 ops
      </button>
      <button
        type="button"
        data-testid="lab-inspect"
        style={{ marginLeft: 8 }}
        onClick={async () => {
          const r = await window.__LQ_OFFLINE_LAB?.inspectOutbox()
          setStatus(JSON.stringify(r))
        }}
      >
        Inspeccionar IDB
      </button>
      <button
        type="button"
        data-testid="lab-sync"
        style={{ marginLeft: 8 }}
        onClick={async () => {
          const r = await window.__LQ_OFFLINE_LAB?.syncNow()
          setStatus(JSON.stringify(r))
        }}
      >
        Sync ahora
      </button>
    </main>
  )
}
