"use client"

import { useEffect, useState } from "react"
import { usePathname, useRouter } from "next/navigation"
import { isAuthenticated } from "@/lib/api"
import { Sidebar } from "@/components/layout/sidebar"
import { Header } from "@/components/layout/header"
import { AppearanceScope } from "@/components/layout/appearance-context"
import { ViewportDiagnostics } from "@/components/layout/viewport-diagnostics"
import { Loader2 } from "lucide-react"
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

  // Al navegar bajo el breakpoint estructural (drawer, <1024 px), cerrar
  // el menú. Se usa el mismo criterio CSS `lg` que decide su visibilidad,
  // para que iPad (768–1023 px) también cierre al navegar.
  useEffect(() => {
    if (window.matchMedia("(max-width: 1023px)").matches) {
      setSidebarOpen(false)
    }
  }, [pathname])

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

  // Dos instancias según contexto CSS (mismo criterio `lg` que decide la
  // visibilidad): la de escritorio conserva colapso/expansión; la del
  // drawer jamás lo recibe y en su lugar muestra solo la X de cierre.
  // Antes ambas compartían elemento y el control se decidía con el JS
  // `useIsMobile` (≤767 px): entre 768–1023 px (iPad) el drawer recibía
  // la flecha de escritorio superpuesta con la X.
  const desktopSidebarEl = (
    <Sidebar
      compact={sidebarCompact}
      onToggleCompact={() => setSidebarCompact((c) => !c)}
    />
  )
  const drawerSidebarEl = (
    <Sidebar onCloseDrawer={() => setSidebarOpen(false)} />
  )

  return (
    <AppearanceScope className="flex h-[100dvh] max-h-[100dvh] overflow-hidden bg-background text-foreground transition-colors duration-200 pt-safe">
      {/* Estructura responsive por CSS, no por JS: el sidebar en flujo solo
          existe ≥1024 px (lg) y el drawer por debajo. Así, teléfonos en
          vertical Y en horizontal (hasta ~932 px CSS) y tablets angostas
          usan navegación superpuesta a ancho completo, y aunque la
          hidratación tarde, un teléfono JAMÁS pinta el sidebar anclado
          comprimiendo el contenido. El estado solo controla abrir/cerrar. */}
      {sidebarOpen ? (
        <div className="hidden shrink-0 lg:block">{desktopSidebarEl}</div>
      ) : null}

      {/* Drawer superpuesto (<1024 px por CSS). Es `fixed` (se ancla al
          viewport e ignora el padding seguro del contenedor): su contenido
          se envuelve con .pt-safe/.pb-safe para no quedar bajo la Dynamic
          Island ni sobre el indicador de inicio. La X de cierre vive en el
          encabezado del propio Sidebar (instancia drawer), alineada con el
          logo y sin superponerse a ningún otro control. */}
      {sidebarOpen ? (
        <div className="lg:hidden">
          <button
            type="button"
            aria-label="Cerrar menú"
            className="fixed inset-0 z-40 bg-black/40"
            onClick={() => setSidebarOpen(false)}
          />
          <div className="fixed inset-y-0 left-0 z-50 max-w-[85vw] shadow-xl">
            <div className="h-full bg-sidebar pt-safe pb-safe pl-[env(safe-area-inset-left)]">
              {drawerSidebarEl}
            </div>
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
      {/* Solo visible en modo de prueba con ?lqdiag=1 */}
      <ViewportDiagnostics />
    </AppearanceScope>
  )
}
