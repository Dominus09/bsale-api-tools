"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { fetchMe } from "@/lib/api"
import { clearSession, getToken } from "@/lib/auth"

export function AuthGate({ children }: { children: React.ReactNode }) {
  const router = useRouter()
  const [ready, setReady] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    async function run() {
      const token = getToken()
      if (!token) {
        router.replace("/login")
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
        setReady(true)
      } catch {
        if (cancelled) return
        clearSession()
        router.replace("/login?expired=1")
      }
    }
    void run()
    return () => {
      cancelled = true
    }
  }, [router])

  if (error) {
    return <div className="banner banner-error">{error}</div>
  }
  if (!ready) {
    return <div className="empty">Verificando sesión…</div>
  }
  return <>{children}</>
}
