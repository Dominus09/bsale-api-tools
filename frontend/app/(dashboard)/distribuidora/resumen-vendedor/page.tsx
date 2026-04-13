"use client"

import dynamic from "next/dynamic"

const ResumenVendedorClient = dynamic(
  () => import("@/components/distribuidora/resumen-vendedor-client"),
  {
    ssr: false,
    loading: () => (
      <div className="p-4 text-sm text-muted-foreground">Cargando resumen…</div>
    ),
  },
)

export default function ResumenVendedorPage() {
  return <ResumenVendedorClient />
}
