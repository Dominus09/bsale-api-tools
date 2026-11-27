"use client"

import { useEffect, useState } from "react"
import { usePathname } from "next/navigation"
import { BRAND } from "@/lib/brand"
import { getActiveModuleId, moduleLabel, type PwaModuleId } from "@/lib/modules"

function moduleFromPath(pathname: string | null): PwaModuleId | null {
  if (!pathname || pathname === "/modulos" || pathname.startsWith("/modulos/")) return null
  if (pathname.startsWith("/despachos")) return "choferes"
  if (pathname.startsWith("/vendedores")) return "vendedores"
  if (pathname.startsWith("/bodega")) return "bodega"
  if (pathname.startsWith("/supervision")) return "supervision"
  return getActiveModuleId()
}

export function ModuleSubtitle() {
  const pathname = usePathname()
  const [id, setId] = useState<PwaModuleId | null>(null)

  useEffect(() => {
    setId(moduleFromPath(pathname))
  }, [pathname])

  const text =
    pathname === "/modulos" || pathname?.startsWith("/modulos/")
      ? BRAND.subtitle
      : id === "choferes"
        ? "Gestión de despachos · Choferes"
        : id
          ? moduleLabel(id)
          : BRAND.subtitle

  return <div className="muted topbar-sub">{text}</div>
}
