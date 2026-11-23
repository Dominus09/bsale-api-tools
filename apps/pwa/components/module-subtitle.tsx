"use client"

import { useEffect, useState } from "react"
import { usePathname } from "next/navigation"
import { BRAND } from "@/lib/brand"
import { getActiveModuleId, moduleLabel, type PwaModuleId } from "@/lib/modules"

function moduleFromPath(pathname: string | null): PwaModuleId | null {
  if (!pathname) return null
  if (pathname.startsWith("/despachos")) return "choferes"
  if (pathname.startsWith("/vendedores")) return "vendedores"
  if (pathname.startsWith("/bodega")) return "bodega"
  if (pathname.startsWith("/supervision")) return "supervision"
  if (pathname.startsWith("/modulos")) return null
  return getActiveModuleId()
}

export function ModuleSubtitle() {
  const pathname = usePathname()
  const [label, setLabel] = useState("Operativa")

  useEffect(() => {
    const id = moduleFromPath(pathname) || getActiveModuleId()
    setLabel(moduleLabel(id))
  }, [pathname])

  return (
    <div className="muted topbar-sub">
      {BRAND.subtitle} · {label}
    </div>
  )
}
