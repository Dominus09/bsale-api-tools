"use client"

import { Skeleton } from "@/components/ui/skeleton"

export function OrsMapSkeleton() {
  return (
    <div className="relative flex h-full min-h-[420px] w-full flex-col overflow-hidden rounded-lg border border-border/70 bg-muted/20">
      <div className="absolute inset-0 bg-[linear-gradient(135deg,hsl(var(--muted)/0.4)_0%,hsl(var(--background))_50%,hsl(var(--muted)/0.25)_100%)]" />
      <div className="relative z-10 flex flex-1 flex-col items-center justify-center gap-3 p-8">
        <Skeleton className="h-4 w-40" />
        <Skeleton className="h-3 w-56" />
        <div className="mt-4 grid w-full max-w-lg grid-cols-3 gap-3">
          <Skeleton className="h-24 rounded-lg" />
          <Skeleton className="col-span-2 h-24 rounded-lg" />
          <Skeleton className="col-span-2 h-20 rounded-lg" />
          <Skeleton className="h-20 rounded-lg" />
        </div>
        <p className="text-xs text-muted-foreground">Calculando rutas ORS…</p>
      </div>
    </div>
  )
}
