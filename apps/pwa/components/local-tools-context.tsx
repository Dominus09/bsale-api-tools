"use client"

import { createContext, useCallback, useContext, useMemo, useState } from "react"

export type LocalToolAction = "gps" | "photo" | "navigate" | "history"

type LocalToolsApi = {
  available: boolean
  addressLabel: string | null
  locationLabel: string | null
  canNavigate: boolean
  navigationUrl: string | null
  run: (action: LocalToolAction) => void
}

type Ctx = {
  tools: LocalToolsApi | null
  registerTools: (api: LocalToolsApi | null) => void
  pendingAction: LocalToolAction | null
  consumePendingAction: () => LocalToolAction | null
  requestAction: (action: LocalToolAction) => void
}

const LocalToolsContext = createContext<Ctx | null>(null)

export function LocalToolsProvider({ children }: { children: React.ReactNode }) {
  const [tools, setTools] = useState<LocalToolsApi | null>(null)
  const [pendingAction, setPendingAction] = useState<LocalToolAction | null>(null)

  const registerTools = useCallback((api: LocalToolsApi | null) => {
    setTools(api)
  }, [])

  const consumePendingAction = useCallback(() => {
    const a = pendingAction
    setPendingAction(null)
    return a
  }, [pendingAction])

  const requestAction = useCallback(
    (action: LocalToolAction) => {
      if (tools?.available) {
        tools.run(action)
        return
      }
      setPendingAction(action)
    },
    [tools],
  )

  const value = useMemo(
    () => ({
      tools,
      registerTools,
      pendingAction,
      consumePendingAction,
      requestAction,
    }),
    [tools, registerTools, pendingAction, consumePendingAction, requestAction],
  )

  return (
    <LocalToolsContext.Provider value={value}>{children}</LocalToolsContext.Provider>
  )
}

export function useLocalTools() {
  const ctx = useContext(LocalToolsContext)
  if (!ctx) throw new Error("useLocalTools outside provider")
  return ctx
}
