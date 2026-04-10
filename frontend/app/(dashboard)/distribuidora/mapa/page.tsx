"use client"

import dynamic from "next/dynamic"

const MapaRuteroClient = dynamic(() => import("@/components/distribuidora/mapa-rutero-client"), {
  ssr: false,
  loading: () => (
    <div className="p-4">
      <div className="rounded-xl bg-white p-4 shadow dark:bg-card">
        <div className="mb-3 flex justify-between">
          <div className="space-y-2">
            <div className="h-6 w-40 animate-pulse rounded bg-muted" />
            <div className="h-4 w-32 animate-pulse rounded bg-muted" />
          </div>
          <div className="flex gap-2">
            <div className="h-9 w-40 animate-pulse rounded-md bg-muted" />
            <div className="h-9 w-36 animate-pulse rounded-md bg-muted" />
          </div>
        </div>
        <div className="relative h-[75vh] overflow-hidden rounded-lg bg-muted/30 ring-1 ring-black/5 dark:ring-white/10">
          <div className="h-full animate-pulse bg-muted/40" />
        </div>
      </div>
    </div>
  ),
})

export default function MapaPage() {
  return <MapaRuteroClient />
}
