"use client"

import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react"
import { getSessionUserId } from "@/lib/auth"
import {
  offlineSyncRuntime,
  type OfflineSyncRuntimeState,
} from "@/lib/offline-sync-runtime"
import { bindOutboxSyncTriggers } from "@/lib/offline-outbox-sync"
import { OfflinePrepModal } from "@/components/offline-prep-modal"
import { OfflineStatusPanel } from "@/components/offline-status-panel"

type Ctx = {
  state: OfflineSyncRuntimeState
  panelOpen: boolean
  setPanelOpen: (v: boolean) => void
  syncNow: () => void
  dismissPrep: () => void
  choosePlan: (planId: number) => void
}

const OfflineSyncCtx = createContext<Ctx | null>(null)

export function useOfflineSync(): Ctx {
  const ctx = useContext(OfflineSyncCtx)
  if (!ctx) {
    throw new Error("useOfflineSync outside provider")
  }
  return ctx
}

export function useOfflineSyncOptional(): Ctx | null {
  return useContext(OfflineSyncCtx)
}

export function OfflineSyncProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<OfflineSyncRuntimeState>(() =>
    offlineSyncRuntime.getState(),
  )
  const [panelOpen, setPanelOpen] = useState(false)

  useEffect(() => {
    const uid = getSessionUserId()
    offlineSyncRuntime.setUserId(uid)
    return offlineSyncRuntime.subscribe(setState)
  }, [])

  useEffect(() => {
    function onOnline() {
      void offlineSyncRuntime.probeApiReachability()
    }
    function onOffline() {
      offlineSyncRuntime.markApiReachable(false)
    }
    window.addEventListener("online", onOnline)
    window.addEventListener("offline", onOffline)
    const unbindOutbox = bindOutboxSyncTriggers()
    return () => {
      window.removeEventListener("online", onOnline)
      window.removeEventListener("offline", onOffline)
      unbindOutbox()
    }
  }, [])

  const value = useMemo<Ctx>(
    () => ({
      state,
      panelOpen,
      setPanelOpen,
      syncNow: () => {
        void offlineSyncRuntime.syncNow()
      },
      dismissPrep: () => offlineSyncRuntime.dismissPrepModal(),
      choosePlan: (planId: number) => offlineSyncRuntime.choosePlanForPrep(planId),
    }),
    [state, panelOpen],
  )

  return (
    <OfflineSyncCtx.Provider value={value}>
      {children}
      <OfflinePrepModal />
      {panelOpen ? <OfflineStatusPanel onClose={() => setPanelOpen(false)} /> : null}
    </OfflineSyncCtx.Provider>
  )
}
