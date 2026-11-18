"use client"

import { useEffect } from "react"
import { useRouter } from "next/navigation"
import { buildEntregaHref } from "@/lib/delivery-flow"
import { useDeliveryParams } from "@/lib/use-delivery-params"

/** Compat: el paso Firma quedó reemplazado por Cierre (sin receptor/firma). */
export default function EntregaFirmaRedirectPage() {
  const router = useRouter()
  const { planId, customerKey } = useDeliveryParams()

  useEffect(() => {
    router.replace(buildEntregaHref(planId, customerKey, "cierre"))
  }, [router, planId, customerKey])

  return <div className="empty">Redirigiendo al cierre…</div>
}
