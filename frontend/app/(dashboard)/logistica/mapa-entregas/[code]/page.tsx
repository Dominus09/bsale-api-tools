"use client"

import { useEffect, useState } from "react"
import Link from "next/link"
import { useParams } from "next/navigation"
import { Loader2 } from "lucide-react"

import { DeliveryMapClient } from "@/components/logistica/DeliveryMapClient"
import {
  getDeliveryMapByCode,
  type DeliveryMapPayload,
} from "@/lib/delivery-map-api"
import { Button } from "@/components/ui/button"

/** Acceso directo por código: /logistica/mapa-entregas/PLAN-00044 */
export default function MapaEntregasByCodePage() {
  const params = useParams()
  const raw = String(params.code || "")
  const code = decodeURIComponent(raw)
  const [data, setData] = useState<DeliveryMapPayload | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (!code) {
      setError("Código requerido")
      return
    }
    getDeliveryMapByCode(code)
      .then(setData)
      .catch((e) => setError(e instanceof Error ? e.message : "Error"))
  }, [code])

  if (error) {
    return (
      <div className="space-y-4 p-4">
        <p className="text-destructive">{error}</p>
        <Button asChild variant="outline">
          <Link href="/distribuidora/planificaciones">Volver</Link>
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
      backHref={`/distribuidora/planificaciones/${data.load.plan_id}`}
      backLabel="Volver al plan"
    />
  )
}
