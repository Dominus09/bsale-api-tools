"use client"

import { useEffect } from "react"

/**
 * Registra el service worker PWA solo en producción.
 * El worker (public/sw.js) cachea únicamente íconos, manifest y la página
 * /offline; nunca intercepta API ni datos autenticados.
 */
export function PwaRegister() {
  useEffect(() => {
    if (process.env.NODE_ENV !== "production") return
    if (!("serviceWorker" in navigator)) return
    const register = () => {
      navigator.serviceWorker.register("/sw.js", { scope: "/" }).catch(() => {
        // La PWA es una mejora progresiva: si el registro falla, la ERP
        // sigue funcionando como sitio normal.
      })
    }
    if (document.readyState === "complete") register()
    else {
      window.addEventListener("load", register, { once: true })
      return () => window.removeEventListener("load", register)
    }
  }, [])
  return null
}
