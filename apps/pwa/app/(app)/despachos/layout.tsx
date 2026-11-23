"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { fetchMe } from "@/lib/api"
import { getSessionUserId } from "@/lib/auth"
import { getOfflineMeta } from "@/lib/offline-store"
import { setActiveModuleId } from "@/lib/modules"

/**
 * Choferes sigue en /despachos. Solo perfil chofer (o offline con meta chofer).
 * No altera el flujo interno de despachos.
 */
export default function DespachosModuleLayout({
  children,
}: {
  children: React.ReactNode
}) {
  const router = useRouter()
  const [ok, setOk] = useState(false)

  useEffect(() => {
    let cancelled = false
    async function gate() {
      try {
        const me = await fetchMe()
        if (cancelled) return
        if (!(me.profiles || []).includes("chofer")) {
          router.replace("/modulos")
          return
        }
        setActiveModuleId("choferes")
        setOk(true)
      } catch {
        const uid = getSessionUserId()
        if (uid) {
          const meta = await getOfflineMeta(uid)
          if (!cancelled && meta?.profiles?.includes("chofer")) {
            setActiveModuleId("choferes")
            setOk(true)
            return
          }
        }
        if (!cancelled) router.replace("/modulos")
      }
    }
    void gate()
    return () => {
      cancelled = true
    }
  }, [router])

  if (!ok) {
    return <div className="empty">Verificando módulo Choferes…</div>
  }
  return children
}
