"use client"

import { useEffect, useState } from "react"
import Link from "next/link"
import { useParams } from "next/navigation"
import { Loader2 } from "lucide-react"

import { DeliveryMapClient } from "@/components/logistica/DeliveryMapClient"
import {
  getDeliveryMapByLoad,
  type DeliveryMapPayload,
} from "@/lib/delivery-map-api"
import { Button } from "@/components/ui/button"

export default function CargaMapaPage() {
  const params = useParams()
  const loadId = Number(params.id)
  const [data, setData] = useState<DeliveryMapPayload | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!Number.isFinite(loadId) || loadId <= 0) {
      setError("Carga inválida")
      return
    }
    getDeliveryMapByLoad(loadId)
      .then(setData)
      .catch((e) => setError(e instanceof Error ? e.message : "Error"))
  }, [loadId])

  if (error) {
    return (
      <div className="mx-auto max-w-lg space-y-4 p-4">
        <p className="text-destructive">{error}</p>
        <p className="text-sm text-muted-foreground">
          El mapa usa la planificación asociada al número de picking (ej.
          PLAN-00044). Si esta carga es solo certificación de productos y no
          coincide con un PLAN, ábrelo desde Planificaciones.
        </p>
        <Button asChild variant="outline" className="w-full">
          <Link href={`/logistica/cargas/${loadId}`}>Volver a la carga</Link>
        </Button>
        <Button asChild className="w-full">
          <Link href="/distribuidora/planificaciones">Ir a Planificaciones</Link>
        </Button>
      </div>
    )
  }

  if (!data) {
    return (
      <div className="flex justify-center py-24 text-muted-foreground">
        <Loader2 className="size-6 animate-spin" />
      </div>
    )
  }

  return (
    <DeliveryMapClient
      initial={data}
      loadId={loadId}
      backHref={`/logistica/cargas/${loadId}`}
      backLabel="Volver a la carga"
    />
  )
}
