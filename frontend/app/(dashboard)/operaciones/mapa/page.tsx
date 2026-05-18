"use client"

import dynamic from "next/dynamic"

const MapaOperacionalClient = dynamic(
  () => import("@/components/operaciones/mapa-operacional-client"),
  { ssr: false, loading: () => <div className="h-[75vh] animate-pulse rounded-xl bg-muted" /> },
)

export default function OperacionesMapaPage() {
  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Mapa operacional</h1>
        <p className="text-sm text-muted-foreground">
          Abra desde la tabla de vendedores o use <code className="text-xs">?ruta=ID</code>
        </p>
      </div>
      <MapaOperacionalClient />
    </div>
  )
}
