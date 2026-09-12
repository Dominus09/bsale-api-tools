"use client"

import { useEffect, useState } from "react"
import Link from "next/link"
import { useParams } from "next/navigation"
import { Loader2 } from "lucide-react"

import { DeliveryMapClient } from "@/components/logistica/DeliveryMapClient"
import {
  getDeliveryMapByPlan,
  type DeliveryMapPayload,
} from "@/lib/delivery-map-api"
import { Button } from "@/components/ui/button"

export default function PlanificacionMapaPage() {
  const params = useParams()
  const planId = Number(params.id)
  const [data, setData] = useState<DeliveryMapPayload | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!Number.isFinite(planId) || planId <= 0) {
      setError("Plan inválido")
      return
    }
    getDeliveryMapByPlan(planId)
      .then(setData)
      .catch((e) => setError(e instanceof Error ? e.message : "Error"))
  }, [planId])

  if (error) {
    return (
      <div className="space-y-4 p-4">
        <p className="text-destructive">{error}</p>
        <Button asChild variant="outline">
          <Link href={`/distribuidora/planificaciones/${planId}`}>Volver</Link>
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
      backHref={`/distribuidora/planificaciones/${planId}`}
      backLabel="Volver al plan"
    />
  )
}
