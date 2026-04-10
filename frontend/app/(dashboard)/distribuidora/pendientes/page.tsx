"use client"

import { DistribuidoraRecordsView } from "@/components/distribuidora/distribuidora-records-view"
import { getDistribuidoraPendientes } from "@/lib/api"

export default function DistribuidoraPendientesPage() {
  return (
    <DistribuidoraRecordsView
      title="Pendientes"
      description="Clientes con vendedor asignado sin día de atención o sin coordenadas."
      loadRows={getDistribuidoraPendientes}
    />
  )
}
