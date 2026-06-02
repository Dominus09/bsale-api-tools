"use client"

import Image from "next/image"
import { Loader2 } from "lucide-react"

export function PlanningRowsLoadingOverlay() {
  return (
    <div
      className="fixed inset-0 z-50 flex flex-col items-center justify-center gap-4 bg-background/85 backdrop-blur-sm"
      role="status"
      aria-live="polite"
      aria-busy="true"
    >
      <div className="relative flex flex-col items-center gap-3 rounded-2xl border border-border/60 bg-card px-10 py-8 shadow-lg">
        <Image
          src="/placeholder-logo.png"
          alt="Quillotana"
          width={120}
          height={48}
          className="h-12 w-auto object-contain opacity-95"
          priority
        />
        <Loader2 className="size-8 animate-spin text-primary" aria-hidden />
        <p className="text-sm font-medium text-foreground">Cargando órdenes…</p>
        <p className="max-w-xs text-center text-xs text-muted-foreground">
          Sincronizando órdenes de compra desde el servidor. Los filtros de estado se
          aplican al terminar, sin volver a cargar.
        </p>
      </div>
    </div>
  )
}
