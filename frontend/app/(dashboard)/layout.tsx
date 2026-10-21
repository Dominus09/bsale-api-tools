"use client"

import { useEffect, useState } from "react"
import { usePathname, useRouter } from "next/navigation"
import { isAuthenticated } from "@/lib/api"
import { Sidebar } from "@/components/layout/sidebar"
import { Header } from "@/components/layout/header"
import { AppearanceScope } from "@/components/layout/appearance-context"
import { Loader2, X } from "lucide-react"
import { cn } from "@/lib/utils"

const MOBILE_MQ = "(max-width: 767px)"

function useIsMobile() {
  const [isMobile, setIsMobile] = useState(false)
  useEffect(() => {
    if (typeof window === "undefined") return
    const mq = window.matchMedia(MOBILE_MQ)
    const apply = () => setIsMobile(mq.matches)
    apply()
    mq.addEventListener("change", apply)
    return () => mq.removeEventListener("change", apply)
  }, [])
  return isMobile
}

export default function DashboardLayout({
  children,
}: {
  children: React.ReactNode
}) {
  const router = useRouter()
  const pathname = usePathname()
  const isMobile = useIsMobile()
  const [isChecking, setIsChecking] = useState(true)
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const [sidebarCompact, setSidebarCompact] = useState(false)
  const [desktopReady, setDesktopReady] = useState(false)

  useEffect(() => {
    if (!isAuthenticated()) {
      router.push("/login")
      return
    }

    const companyId = localStorage.getItem("company_id")
    if (!companyId) {
      router.push("/company-selector")
      return
    }

    setIsChecking(false)
  }, [router])

  // Desktop: abrir sidebar una vez; mobile: siempre cerrada por defecto
  useEffect(() => {
    if (isChecking) return
    if (isMobile) {
      setSidebarOpen(false)
      setSidebarCompact(false)
      setDesktopReady(true)
      return
    }
    if (!desktopReady) {
      setSidebarOpen(true)
      setDesktopReady(true)
    }
  }, [isMobile, isChecking, desktopReady])

  // Al navegar en mobile, cerrar drawer
  useEffect(() => {
    if (isMobile) setSidebarOpen(false)
  }, [pathname, isMobile])

  const isMapPage =
    pathname.includes("/mapa-entregas") ||
    /\/planificaciones\/[^/]+\/mapa\/?$/.test(pathname) ||
    /\/logistica\/cargas\/[^/]+\/mapa\/?$/.test(pathname)

  if (isChecking) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-background">
        <Loader2 className="h-8 w-8 animate-spin text-primary" />
      </div>
    )
  }

  const sidebarEl = (
    <Sidebar
      compact={!isMobile && sidebarCompact}
      onToggleCompact={
        isMobile
          ? undefined
          : () => setSidebarCompact((c) => !c)
      }
    />
  )

  return (
    <AppearanceScope className="flex h-[100dvh] max-h-[100dvh] overflow-hidden bg-background text-foreground transition-colors duration-200 pt-safe">
      {/* Estructura responsive por CSS, no por JS: el sidebar en flujo solo
          existe ≥768 px y el drawer solo <768 px. Así, aunque la hidratación
          tarde o el hook de matchMedia difiera, un teléfono JAMÁS pinta el
          sidebar anclado comprimiendo el contenido (defecto visto en iPhone).
          El estado `sidebarOpen` solo controla apertura/cierre. */}
      {sidebarOpen ? (
        <div className="hidden shrink-0 md:block">{sidebarEl}</div>
      ) : null}

      {/* Drawer móvil (<768 px por CSS). Es `fixed` (se ancla al viewport e
          ignora el padding seguro del contenedor): su contenido se envuelve
          con .pt-safe/.pb-safe para no quedar bajo la Dynamic Island ni
          sobre el indicador de inicio, con botón de cierre propio de 44 px
          alineado con la cabecera del menú. */}
      {sidebarOpen ? (
        <div className="md:hidden">
          <button
            type="button"
            aria-label="Cerrar menú"
            className="fixed inset-0 z-40 bg-black/40"
            onClick={() => setSidebarOpen(false)}
          />
          <div className="fixed inset-y-0 left-0 z-50 max-w-[85vw] shadow-xl">
            <div className="h-full bg-sidebar pt-safe pb-safe">
              {sidebarEl}
            </div>
            <button
              type="button"
              aria-label="Cerrar menú lateral"
              onClick={() => setSidebarOpen(false)}
              className="absolute right-2 top-[calc(env(safe-area-inset-top)+0.625rem)] z-10 flex size-11 items-center justify-center rounded-md text-white/70 transition-colors hover:bg-white/10 hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary/70"
            >
              <X className="h-5 w-5" aria-hidden />
            </button>
          </div>
        </div>
      ) : null}

      <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
        <Header
          sidebarOpen={sidebarOpen}
          onToggleSidebar={() => setSidebarOpen((o) => !o)}
        />
        <main
          className={cn(
            "min-w-0 flex-1 overflow-x-hidden overflow-y-auto",
            isMapPage ? "p-0" : "p-3 sm:p-4 md:p-6",
          )}
        >
          {children}
        </main>
      </div>
    </AppearanceScope>
  )
}
