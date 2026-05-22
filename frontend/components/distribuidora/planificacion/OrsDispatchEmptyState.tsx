"use client"

import Link from "next/link"
import { MapPinOff, Route } from "lucide-react"
import { Button } from "@/components/ui/button"

export function OrsDispatchEmptyState() {
  return (
    <div className="flex min-h-[calc(100dvh-8rem)] flex-col items-center justify-center gap-6 px-6 py-16">
      <div className="flex size-16 items-center justify-center rounded-2xl border border-dashed border-border bg-muted/30">
        <MapPinOff className="size-8 text-muted-foreground" aria-hidden />
      </div>
      <div className="max-w-md space-y-2 text-center">
        <p className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
          Dispatch center
        </p>
        <h1 className="text-2xl font-semibold tracking-tight">Planif. mapa ORS</h1>
        <p className="text-sm leading-relaxed text-muted-foreground">
          No hay órdenes en cola. Asigne camiones y georreferencia en pre-despacho, luego envíe
          el lote a planificación.
        </p>
      </div>
      <Button asChild className="gap-2">
        <Link href="/distribuidora/orders">
          <Route className="size-4" aria-hidden />
          Ir a pre-despacho OC
        </Link>
      </Button>
    </div>
  )
}
