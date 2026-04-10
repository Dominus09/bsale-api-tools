"use client"

import { DistribuidoraRecordsView } from "@/components/distribuidora/distribuidora-records-view"
import { getDistribuidoraSinGeoref } from "@/lib/api"

export default function DistribuidoraSinGeorefPage() {
  return (
    <DistribuidoraRecordsView
      title="Sin georef"
      description="Puntos del rutero activos sin latitud o longitud."
      loadRows={getDistribuidoraSinGeoref}
    />
  )
}
