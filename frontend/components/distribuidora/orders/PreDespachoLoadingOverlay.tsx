"use client"

import { Loader2 } from "lucide-react"

import { QUILLOTANA_LOGO_GRUPO_URL } from "@/lib/quillotana-brand"
import { cn } from "@/lib/utils"

export type PreDespachoLoadingPhase =
  | "consulting-orders"
  | "analyzing-billing"
  | "generating-observations"
  | "communal-summary"
  | "syncing-bsale"

const PHASE_SUBTITLES: Record<PreDespachoLoadingPhase, string> = {
  "consulting-orders": "Consultando órdenes",
  "analyzing-billing": "Analizando facturación",
  "generating-observations": "Generando observaciones",
  "communal-summary": "Calculando resumen comunal",
  "syncing-bsale": "Sincronizando con Bsale",
}

type PreDespachoLoadingOverlayProps = {
  phase: PreDespachoLoadingPhase
  className?: string
}

export function PreDespachoLoadingOverlay({
  phase,
  className,
}: PreDespachoLoadingOverlayProps) {
  const subtitle = PHASE_SUBTITLES[phase]

  return (
    <div
      className={cn(
        "fixed inset-0 z-[100] flex items-center justify-center",
        "bg-slate-950/40 backdrop-blur-[2px]",
        className,
      )}
      role="alertdialog"
      aria-modal="true"
      aria-busy="true"
      aria-live="polite"
      aria-label="Cargando órdenes de pre-despacho"
    >
      <div
        className="pointer-events-auto mx-4 w-full max-w-md rounded-xl border border-slate-200/90 bg-white px-10 py-9 shadow-2xl dark:border-slate-700 dark:bg-slate-900"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex flex-col items-center gap-5 text-center">
          <img
            src={QUILLOTANA_LOGO_GRUPO_URL}
            alt="Grupo Quillotana"
            width={200}
            height={64}
            className="h-14 w-auto max-w-[220px] object-contain"
            decoding="async"
          />
          <Loader2
            className="size-9 animate-spin text-emerald-700 dark:text-emerald-500"
            aria-hidden
          />
          <div className="space-y-1.5">
            <p className="text-base font-semibold tracking-tight text-slate-900 dark:text-slate-50">
              Cargando órdenes…
            </p>
            <p
              key={phase}
              className="text-sm text-slate-600 animate-in fade-in duration-300 dark:text-slate-400"
            >
              {subtitle}
            </p>
          </div>
          <div
            className="h-1 w-full overflow-hidden rounded-full bg-slate-100 dark:bg-slate-800"
            aria-hidden
          >
            <div className="h-full w-2/5 animate-pulse rounded-full bg-emerald-600/80" />
          </div>
        </div>
      </div>
    </div>
  )
}
