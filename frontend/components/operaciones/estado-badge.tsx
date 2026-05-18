"use client"

import { Badge } from "@/components/ui/badge"
import { cn } from "@/lib/utils"
import type { EstadoConexion } from "@/services/operaciones"

const STYLES: Record<EstadoConexion, string> = {
  activo: "bg-emerald-500/15 text-emerald-700 dark:text-emerald-400 border-emerald-500/30",
  atrasado: "bg-amber-500/15 text-amber-800 dark:text-amber-400 border-amber-500/30",
  offline: "bg-red-500/15 text-red-700 dark:text-red-400 border-red-500/30",
}

const LABELS: Record<EstadoConexion, string> = {
  activo: "Activo",
  atrasado: "Atrasado",
  offline: "Offline",
}

export function EstadoConexionBadge({ estado, className }: { estado: EstadoConexion; className?: string }) {
  return (
    <Badge variant="outline" className={cn("font-medium", STYLES[estado], className)}>
      {LABELS[estado]}
    </Badge>
  )
}
