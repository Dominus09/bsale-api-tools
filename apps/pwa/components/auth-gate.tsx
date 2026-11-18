"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { fetchMe } from "@/lib/api"
import { clearSession, getSessionUserId, getToken } from "@/lib/auth"
import { getOfflineMeta, isLikelyOfflineError, saveOfflineMeta } from "@/lib/offline-store"

export function AuthGate({ children }: { children: React.ReactNode }) {
  const router = useRouter()
  const [ready, setReady] = useState(false)
  const [offlineMode, setOfflineMode] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    async function run() {
      const token = getToken()
      if (!token) {
        router.replace("/login")
        return
      }

      const offline = typeof navigator !== "undefined" && navigator.onLine === false
      if (offline) {
        const uid = getSessionUserId()
        if (uid) {
          const meta = await getOfflineMeta(uid)
          if (!cancelled) {
            setOfflineMode(true)
            setReady(true)
            if (!meta) {
              setError(null)
            }
          }
          return
        }
        if (!cancelled) router.replace("/login")
        return
      }

      try {
        const me = await fetchMe()
        if (cancelled) return
        if (!me.has_pwa_access) {
          clearSession()
          setError("Esta cuenta no tiene acceso a la app operativa.")
          router.replace("/login?denied=pwa")
          return
        }
        if (!me.profiles.includes("chofer")) {
          clearSession()
          setError("Se requiere perfil chofer.")
          router.replace("/login?denied=profile")
          return
        }
        const uid = me.id != null ? String(me.id) : getSessionUserId()
        if (uid) {
          await saveOfflineMeta({
            userId: uid,
            email: me.email,
            profiles: me.profiles || [],
            syncedAt: new Date().toISOString(),
          })
        }
        setOfflineMode(false)
        setReady(true)
      } catch (err) {
        if (cancelled) return
        // Offline / red: no borrar sesión; permitir shell de consulta.
        if (isLikelyOfflineError(err)) {
          const uid = getSessionUserId()
          if (uid && getToken()) {
            setOfflineMode(true)
            setReady(true)
            return
          }
        }
        clearSession()
        router.replace("/login?expired=1")
      }
    }
    void run()

    const onPageShow = (e: PageTransitionEvent) => {
      if (e.persisted && !getToken()) {
        router.replace("/login")
      }
    }
    const onOnline = () => {
      // Revalidar al recuperar red sin borrar datos offline previos.
      void run()
    }
    window.addEventListener("pageshow", onPageShow)
    window.addEventListener("online", onOnline)

    return () => {
      cancelled = true
      window.removeEventListener("pageshow", onPageShow)
      window.removeEventListener("online", onOnline)
    }
  }, [router])

  if (error) {
    return <div className="banner banner-error">{error}</div>
  }
  if (!ready) {
    return <div className="empty">Verificando sesión…</div>
  }
  return (
    <>
      {offlineMode ? (
        <div className="banner banner-warn" role="status">
          Modo sin conexión · consulta de datos ya sincronizados (solo lectura)
        </div>
      ) : null}
      {children}
    </>
  )
}
