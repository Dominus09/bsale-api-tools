"use client"

import { useEffect, useState } from "react"
import { Check, Monitor, Moon, Sun } from "lucide-react"

import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import {
  readAppearancePreference,
  storeAppearancePreference,
  type AppearancePreference,
} from "@/lib/appearance"

const OPTIONS: {
  value: AppearancePreference
  label: string
  icon: typeof Sun
  available: boolean
  hint?: string
}[] = [
  { value: "light", label: "Claro", icon: Sun, available: true },
  { value: "dark", label: "Oscuro", icon: Moon, available: false, hint: "Próximamente" },
  { value: "system", label: "Sistema", icon: Monitor, available: false, hint: "Próximamente" },
]

/**
 * Selector de apariencia (área de usuario).
 *
 * Solo el modo claro está activo: el oscuro y el de sistema se muestran como
 * preparación de la arquitectura, deshabilitados hasta que cada área esté
 * validada. La preferencia elegida queda persistida en el equipo.
 */
export function AppearanceSelector() {
  const [preference, setPreference] = useState<AppearancePreference>("light")

  useEffect(() => {
    setPreference(readAppearancePreference())
  }, [])

  const choose = (value: AppearancePreference) => {
    setPreference(value)
    storeAppearancePreference(value)
  }

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button
          type="button"
          variant="ghost"
          size="icon"
          className="h-10 w-10 shrink-0 text-muted-foreground hover:text-foreground"
          aria-label="Apariencia de la interfaz"
          title="Apariencia"
        >
          <Sun className="h-5 w-5" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-60">
        <DropdownMenuLabel>Apariencia</DropdownMenuLabel>
        <DropdownMenuSeparator />
        {OPTIONS.map((option) => (
          <DropdownMenuItem
            key={option.value}
            disabled={!option.available}
            onSelect={() => choose(option.value)}
            className="flex items-center gap-2"
          >
            <option.icon className="h-4 w-4 shrink-0" />
            <span className="flex-1">{option.label}</span>
            {option.hint ? <span className="text-[11px] text-muted-foreground">{option.hint}</span> : null}
            {option.available && preference === option.value ? <Check className="h-4 w-4 shrink-0" /> : null}
          </DropdownMenuItem>
        ))}
        <DropdownMenuSeparator />
        <p className="px-2 py-1.5 text-[11px] leading-4 text-muted-foreground">
          El modo oscuro se habilitará por áreas una vez validadas, para no mezclar temas en una misma pantalla.
        </p>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}
