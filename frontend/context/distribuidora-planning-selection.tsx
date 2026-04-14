"use client"

import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  type ReactNode,
} from "react"

type DistribuidoraPlanningContextValue = {
  /** document_id de OC seleccionadas para planificación de rutas */
  planningDocumentIds: ReadonlySet<number>
  planningDocumentIdsArray: number[]
  addPlanningDocuments: (ids: number[]) => void
  removePlanningDocument: (id: number) => void
  clearPlanningDocuments: () => void
  hasInPlanning: (id: number) => boolean
}

const DistribuidoraPlanningContext =
  createContext<DistribuidoraPlanningContextValue | null>(null)

export function DistribuidoraPlanningProvider({ children }: { children: ReactNode }) {
  const [ids, setIds] = useState<Set<number>>(() => new Set())

  const addPlanningDocuments = useCallback((next: number[]) => {
    setIds((prev) => {
      const n = new Set(prev)
      for (const x of next) {
        if (Number.isFinite(x) && x > 0) n.add(x)
      }
      return n
    })
  }, [])

  const removePlanningDocument = useCallback((id: number) => {
    setIds((prev) => {
      const n = new Set(prev)
      n.delete(id)
      return n
    })
  }, [])

  const clearPlanningDocuments = useCallback(() => {
    setIds(new Set())
  }, [])

  const hasInPlanning = useCallback((id: number) => ids.has(id), [ids])

  const value = useMemo<DistribuidoraPlanningContextValue>(
    () => ({
      planningDocumentIds: ids,
      planningDocumentIdsArray: Array.from(ids),
      addPlanningDocuments,
      removePlanningDocument,
      clearPlanningDocuments,
      hasInPlanning,
    }),
    [
      ids,
      addPlanningDocuments,
      removePlanningDocument,
      clearPlanningDocuments,
      hasInPlanning,
    ],
  )

  return (
    <DistribuidoraPlanningContext.Provider value={value}>
      {children}
    </DistribuidoraPlanningContext.Provider>
  )
}

export function useDistribuidoraPlanning(): DistribuidoraPlanningContextValue {
  const ctx = useContext(DistribuidoraPlanningContext)
  if (!ctx) {
    throw new Error(
      "useDistribuidoraPlanning debe usarse dentro de DistribuidoraPlanningProvider",
    )
  }
  return ctx
}
