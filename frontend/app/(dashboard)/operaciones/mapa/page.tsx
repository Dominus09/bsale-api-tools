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
        <h1 className="text-[28px] font-semibold leading-8 tracking-[-0.02em] text-lq-ink sm:text-[32px] sm:leading-9">Mapa operacional</h1>
        <p className="text-sm text-muted-foreground">
          Vista global de vendedores activos con última posición. Use{" "}
          <code className="text-xs">?ruta=ID</code> para una ruta puntual.
        </p>
      </div>
      <MapaOperacionalClient />
    </div>
  )
}
