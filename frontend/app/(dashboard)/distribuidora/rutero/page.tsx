"use client"

import { DistribuidoraRecordsView } from "@/components/distribuidora/distribuidora-records-view"
import { getDistribuidoraRutero } from "@/lib/api"

export default function DistribuidoraRuteroPage() {
  return (
    <DistribuidoraRecordsView
      title="Rutero"
      description="Registros activos del rutero (company_id = 3)."
      loadRows={getDistribuidoraRutero}
    />
  )
}
