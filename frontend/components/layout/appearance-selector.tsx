"use client"

import { Moon, Sun } from "lucide-react"

import { Button } from "@/components/ui/button"
import { useAppearance } from "@/components/layout/appearance-context"
import { cn } from "@/lib/utils"

/**
 * Selector luna/sol del encabezado.
 *
 * - Luna: activa Graphite Dark. Sol: vuelve a Graphite claro.
 * - Sin recargar la página; la preferencia queda persistida en el navegador.
 * - Solo se muestra en las rutas con ambos temas adaptados
 *   (`DARK_THEME_ROUTE_PREFIXES` en lib/appearance.ts). En el resto de la ERP
 *   no aparece, para que el usuario nunca encuentre una interfaz mezclada.
 */
export function AppearanceSelector({ className }: { className?: string }) {
  const { theme, routeSupportsDark, toggleTheme } = useAppearance()

  if (!routeSupportsDark) return null

  const isDark = theme === "dark"

  return (
    <Button
      type="button"
      variant="ghost"
      size="icon"
      onClick={toggleTheme}
      className={cn(
        "h-10 w-10 shrink-0 text-lq-on-graphite-muted transition-colors hover:bg-white/10 hover:text-white focus-visible:ring-2 focus-visible:ring-primary/70",
        className,
      )}
      aria-label={isDark ? "Activar modo claro" : "Activar modo oscuro"}
      title={isDark ? "Activar modo claro" : "Activar modo oscuro"}
    >
      {isDark ? <Sun className="h-5 w-5" /> : <Moon className="h-5 w-5" />}
    </Button>
  )
}
