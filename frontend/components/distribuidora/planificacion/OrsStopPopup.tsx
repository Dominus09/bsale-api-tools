"use client"

import {
  SEMAPHORE_BORDER_CLASS,
  SEMAPHORE_EMOJI,
} from "@/lib/ors-commercial-semaphore"
import type { OrsStopPopupData } from "@/lib/ors-map-ui"
import { formatClp } from "@/lib/ors-map-ui"
import { cn } from "@/lib/utils"

type OrsStopPopupProps = OrsStopPopupData

export function OrsStopPopup({
  nombre,
  direccion,
  comuna,
  ventaTotal,
  pesoKg,
  ocCount,
  observaciones,
  diaEntregaLabel,
  semaphore,
  isolated,
  isolatedDistanceKm,
}: OrsStopPopupProps) {
  const obsText = (observaciones ?? []).filter(Boolean).join(" · ")

  return (
    <div className="min-w-[11rem] max-w-[16rem] space-y-2 text-xs text-foreground">
      <div className="flex items-start gap-1.5">
        {semaphore ? (
          <span className="shrink-0 text-sm leading-none" aria-hidden>
            {isolated ? "⚫" : SEMAPHORE_EMOJI[semaphore]}
          </span>
        ) : null}
        <p className="font-semibold leading-snug">{nombre}</p>
      </div>
      {isolated ? (
        <p className="rounded border border-slate-500/30 bg-slate-500/10 px-1.5 py-0.5 text-[10px] font-medium text-slate-700 dark:text-slate-200">
          Cliente aislado
          {isolatedDistanceKm != null
            ? ` · ~${isolatedDistanceKm.toFixed(1)} km del grupo`
            : ""}
        </p>
      ) : null}
      {semaphore ? (
        <span
          className={cn(
            "inline-block rounded px-1.5 py-0.5 text-[10px] font-medium",
            SEMAPHORE_BORDER_CLASS[semaphore],
          )}
        >
          Venta {formatClp(ventaTotal)}
        </span>
      ) : null}
      <dl className="grid grid-cols-[auto_1fr] gap-x-2 gap-y-1 text-[11px]">
        <dt className="text-muted-foreground">Venta</dt>
        <dd className="font-medium tabular-nums">{formatClp(ventaTotal)}</dd>
        {pesoKg != null && pesoKg > 0 ? (
          <>
            <dt className="text-muted-foreground">Peso</dt>
            <dd className="font-medium tabular-nums">
              {pesoKg.toLocaleString("es-CL", { maximumFractionDigits: 1 })} kg
            </dd>
          </>
        ) : null}
        <dt className="text-muted-foreground">OC</dt>
        <dd className="font-medium tabular-nums">{ocCount}</dd>
        {comuna?.trim() ? (
          <>
            <dt className="text-muted-foreground">Comuna</dt>
            <dd>{comuna.trim()}</dd>
          </>
        ) : null}
        {diaEntregaLabel?.trim() ? (
          <>
            <dt className="text-muted-foreground">Día</dt>
            <dd className="capitalize">{diaEntregaLabel.trim()}</dd>
          </>
        ) : null}
        {direccion?.trim() ? (
          <>
            <dt className="text-muted-foreground">Dirección</dt>
            <dd className="leading-snug">{direccion.trim()}</dd>
          </>
        ) : null}
      </dl>
      {obsText ? (
        <div className="border-t border-border/60 pt-2">
          <p className="text-[10px] font-medium text-muted-foreground">Observación</p>
          <p className="mt-0.5 text-[11px] leading-snug text-foreground">{obsText}</p>
        </div>
      ) : null}
    </div>
  )
}
