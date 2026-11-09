"use client"

import { useEffect } from "react"

/** Registra SW solo en producción. Nunca cachea API. */
export function PwaRegister() {
  useEffect(() => {
    if (process.env.NODE_ENV !== "production") return
    if (!("serviceWorker" in navigator)) return
    navigator.serviceWorker.register("/sw.js").catch(() => {
      /* silencioso: instalación opcional */
    })
  }, [])
  return null
}
