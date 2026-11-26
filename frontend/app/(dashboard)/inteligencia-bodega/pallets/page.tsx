"use client"

import { useMemo } from "react"
import { Loader2, ShieldAlert } from "lucide-react"
import { PalletRegister } from "@/components/warehouse/PalletRegister"
import { useAuthMe } from "@/lib/use-auth-me"
import { hasManagementAccess, staffUserFromLocalStorage } from "@/lib/permissions"

export default function PalletsPage() {
  const { meState, staff } = useAuthMe()

  const allowed = useMemo(() => {
    if (meState === "loading") return null
    if (meState === "ok" && staff) return hasManagementAccess(staff)
    // Fallback local solo si /auth/me no está disponible; backend sigue siendo SoT.
    if (meState === "unavailable") return hasManagementAccess(staffUserFromLocalStorage())
    return false
  }, [meState, staff])

  if (meState === "loading" || allowed === null) {
    return (
      <div className="flex h-[50vh] items-center justify-center gap-2 text-muted-foreground">
        <Loader2 className="h-5 w-5 animate-spin" />
        Verificando acceso…
      </div>
    )
  }

  if (!allowed) {
    return (
      <div className="mx-auto mt-16 max-w-lg rounded-lg border border-destructive/30 bg-destructive/10 p-6 text-center">
        <ShieldAlert className="mx-auto mb-3 h-10 w-10 text-destructive" />
        <h1 className="text-lg font-semibold text-destructive">Acceso restringido</h1>
        <p className="mt-2 text-sm text-destructive">
          Inteligencia de Bodega requiere permisos de gerencia ERP (roles gerencia / adm / admin u
          owner). El perfil PWA bodega no concede este acceso.
        </p>
      </div>
    )
  }

  return <PalletRegister />
}
