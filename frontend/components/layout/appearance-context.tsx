"use client"

import { createContext, useContext, useEffect, useMemo, useState, type ReactNode } from "react"
import { usePathname } from "next/navigation"

import { cn } from "@/lib/utils"
import {
  APPEARANCE_BOOTSTRAP_SCRIPT,
  isDarkThemeRoute,
  readAppearanceTheme,
  storeAppearanceTheme,
  type AppearanceTheme,
} from "@/lib/appearance"

type AppearanceContextValue = {
  /** Preferencia elegida por el usuario (persistida). */
  theme: AppearanceTheme
  /** true si la ruta actual admite modo oscuro. */
  routeSupportsDark: boolean
  /** true cuando el oscuro está efectivamente aplicado en pantalla. */
  isDarkActive: boolean
  toggleTheme: () => void
}

const AppearanceContext = createContext<AppearanceContextValue>({
  theme: "light",
  routeSupportsDark: false,
  isDarkActive: false,
  toggleTheme: () => {},
})

export function useAppearance() {
  return useContext(AppearanceContext)
}

/**
 * Contenedor temático del dashboard.
 *
 * Aplica la clase `dark` solo a este contenedor y solo cuando la ruta está
 * adaptada; el resto de la ERP permanece en claro. El script inline inicial
 * evita el parpadeo del tema incorrecto al cargar o actualizar la página.
 */
export function AppearanceScope({ children, className }: { children: ReactNode; className?: string }) {
  const pathname = usePathname()
  const [theme, setTheme] = useState<AppearanceTheme>("light")

  useEffect(() => {
    setTheme(readAppearanceTheme())
  }, [])

  const routeSupportsDark = isDarkThemeRoute(pathname)
  const isDarkActive = theme === "dark" && routeSupportsDark

  // Los portales (modales, drawers, dropdowns) se montan fuera de este
  // contenedor: se sincroniza la clase en <html> solo mientras la ruta
  // adaptada está activa, y se retira al salir de ella o desmontar.
  useEffect(() => {
    document.documentElement.classList.toggle("dark", isDarkActive)
    return () => {
      document.documentElement.classList.remove("dark")
    }
  }, [isDarkActive])

  const value = useMemo<AppearanceContextValue>(
    () => ({
      theme,
      routeSupportsDark,
      isDarkActive,
      toggleTheme: () => {
        setTheme((current) => {
          const next: AppearanceTheme = current === "dark" ? "light" : "dark"
          storeAppearanceTheme(next)
          return next
        })
      },
    }),
    [theme, routeSupportsDark, isDarkActive],
  )

  return (
    <AppearanceContext.Provider value={value}>
      <div suppressHydrationWarning className={cn(isDarkActive && "dark", className)}>
        <script dangerouslySetInnerHTML={{ __html: APPEARANCE_BOOTSTRAP_SCRIPT }} />
        {children}
      </div>
    </AppearanceContext.Provider>
  )
}
