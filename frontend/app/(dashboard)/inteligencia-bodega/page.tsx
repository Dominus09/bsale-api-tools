"use client"

import { useMemo } from "react"
import { Loader2, ShieldAlert } from "lucide-react"
import { WarehouseEditor } from "@/components/warehouse/WarehouseEditor"
import { useAuthMe } from "@/lib/use-auth-me"
import { hasManagementAccess, staffUserFromLocalStorage } from "@/lib/permissions"

export default function InteligenciaBodegaPage() {
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
      <div className="flex h-[50vh] items-center justify-center gap-2 text-slate-600">
        <Loader2 className="h-5 w-5 animate-spin" />
        Verificando acceso…
      </div>
    )
  }

  if (!allowed) {
    return (
      <div className="mx-auto mt-16 max-w-lg rounded-lg border border-red-200 bg-red-50 p-6 text-center">
        <ShieldAlert className="mx-auto mb-3 h-10 w-10 text-red-600" />
        <h1 className="text-lg font-semibold text-red-900">Acceso restringido</h1>
        <p className="mt-2 text-sm text-red-800">
          Inteligencia de Bodega requiere permisos de gerencia ERP (roles gerencia / adm / admin u
          owner). El perfil PWA bodega no concede este acceso.
        </p>
      </div>
    )
  }

  return (
    <div className="space-y-3">
      <div>
        <h1 className="text-xl font-semibold tracking-tight text-slate-900">
          Inteligencia de Bodega
        </h1>
        <p className="text-sm text-slate-600">
          Editor Maestro de Bodegas — layout físico versionado. Sin inventario ni FIFO. Publicación
          formal disponible; no certifica seguridad ni autoriza apilamiento.
        </p>
      </div>
      <WarehouseEditor />
    </div>
  )
}
