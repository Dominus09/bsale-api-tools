"use client"

import { useEffect } from "react"
import { useRouter } from "next/navigation"
import { buildEntregaHref } from "@/lib/delivery-flow"
import { useDeliveryParams } from "@/lib/use-delivery-params"

export default function EntregaIndexPage() {
  const router = useRouter()
  const { planId, customerKey } = useDeliveryParams()

  useEffect(() => {
    if (!Number.isFinite(planId) || planId < 1 || !customerKey) return
    router.replace(buildEntregaHref(planId, customerKey, 1))
  }, [router, planId, customerKey])

  return <div className="empty">Abriendo entrega…</div>
}
