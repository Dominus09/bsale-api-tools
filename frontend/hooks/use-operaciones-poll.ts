"use client"

import { useCallback, useEffect, useRef, useState } from "react"

import { OPERACIONES_POLL_MS } from "@/services/operaciones"

export function useOperacionesPoll<T>(
  loader: () => Promise<T>,
  deps: unknown[],
  options?: { enabled?: boolean; intervalMs?: number },
) {
  const enabled = options?.enabled !== false
  const intervalMs = options?.intervalMs ?? OPERACIONES_POLL_MS
  const [data, setData] = useState<T | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const mounted = useRef(true)

  const refresh = useCallback(async () => {
    if (!enabled) return
    try {
      const res = await loader()
      if (mounted.current) {
        setData(res)
        setError(null)
      }
    } catch (e: unknown) {
      if (mounted.current) {
        setError(e instanceof Error ? e.message : "Error al cargar")
      }
    } finally {
      if (mounted.current) setLoading(false)
    }
  }, [enabled, loader])

  useEffect(() => {
    mounted.current = true
    setLoading(true)
    void refresh()
    if (!enabled || intervalMs <= 0) {
      return () => {
        mounted.current = false
      }
    }
    const id = window.setInterval(() => void refresh(), intervalMs)
    return () => {
      mounted.current = false
      window.clearInterval(id)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- deps passed explicitly
  }, [refresh, intervalMs, enabled, ...deps])

  return { data, loading, error, refresh, setLoading }
}
