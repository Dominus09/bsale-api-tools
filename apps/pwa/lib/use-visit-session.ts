"use client"

import { useCallback, useEffect, useState } from "react"
import {
  ApiError,
  type DeliveryVisit,
  type DriverDocument,
  getMyCliente,
  getVisitDetail,
} from "@/lib/api"

export type VisitSession = {
  clientName: string
  client: Awaited<ReturnType<typeof getMyCliente>>["client"]
  navigation: Awaited<ReturnType<typeof getMyCliente>>["navigation"]
  p3: Awaited<ReturnType<typeof getMyCliente>>["p3"]
  p4Ready: boolean
  documents: DriverDocument[]
  documentCount: number
  documentTotal: number
  activeVisit: DeliveryVisit | null
  /** Última visita del plan/cliente (activa o cerrada). */
  latestVisit: DeliveryVisit | null
  /** Última visita cerrada (puede coexistir con activeVisit). */
  lastClosedVisit: DeliveryVisit | null
  visitDetail: Awaited<ReturnType<typeof getVisitDetail>> | null
  reload: () => void
  loading: boolean
  error: string | null
}

export function useVisitSession(planId: number, customerKey: string): VisitSession {
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [token, setToken] = useState(0)
  const [clientPayload, setClientPayload] = useState<Awaited<
    ReturnType<typeof getMyCliente>
  > | null>(null)
  const [visitDetail, setVisitDetail] = useState<Awaited<
    ReturnType<typeof getVisitDetail>
  > | null>(null)

  const reload = useCallback(() => setToken((n) => n + 1), [])

  useEffect(() => {
    if (!Number.isFinite(planId) || planId < 1 || !customerKey) {
      setError("Cliente inválido")
      setLoading(false)
      return
    }
    let cancelled = false
    async function load() {
      setLoading(true)
      try {
        const res = await getMyCliente(planId, customerKey)
        if (cancelled) return
        setClientPayload(res)
        setError(null)
        // Detalle del circuito: prioriza activa; si no, última cerrada.
        const visit = res.active_visit || res.last_closed_visit || res.latest_visit
        if (visit?.id) {
          try {
            const detail = await getVisitDetail(planId, visit.id)
            if (!cancelled) setVisitDetail(detail)
          } catch {
            if (!cancelled) setVisitDetail(null)
          }
        } else if (!cancelled) {
          setVisitDetail(null)
        }
      } catch (err) {
        if (cancelled) return
        if (err instanceof ApiError && (err.status === 404 || err.status === 403)) {
          setError("Cliente no pertenece a tu despacho o no estás autorizado.")
        } else if (typeof navigator !== "undefined" && !navigator.onLine) {
          setError("Sin conexión. Reintenta cuando vuelva la red.")
        } else {
          setError(err instanceof ApiError ? err.message : "Error al cargar visita")
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    void load()
    return () => {
      cancelled = true
    }
  }, [planId, customerKey, token])

  const documents = clientPayload?.documents || []
  const documentTotal = documents.reduce(
    (acc, d) => acc + (typeof d.document_total === "number" ? d.document_total : 0),
    0,
  )

  return {
    clientName: clientPayload?.client?.client_name || "Cliente",
    client: clientPayload?.client || ({} as VisitSession["client"]),
    navigation: clientPayload?.navigation,
    p3: clientPayload?.p3,
    p4Ready: Boolean(clientPayload?.p4?.tables_ready),
    documents,
    documentCount: clientPayload?.document_count || documents.length,
    documentTotal,
    activeVisit: clientPayload?.active_visit || null,
    latestVisit: clientPayload?.latest_visit || clientPayload?.active_visit || null,
    lastClosedVisit: clientPayload?.last_closed_visit || null,
    visitDetail,
    reload,
    loading,
    error,
  }
}
