"use client"

import { useParams } from "next/navigation"
import { decodeCustomerKey } from "@/lib/delivery-flow"

/** Params de ruta del circuito de entrega (mismo patrón que la ficha). */
export function useDeliveryParams() {
  const params = useParams()
  const planId = Number(params.planId)
  const customerKey = decodeCustomerKey(params.customerKey as string | string[] | undefined)
  const rawDoc = params.docKey as string | string[] | undefined
  const docKey = rawDoc ? decodeCustomerKey(rawDoc) : null
  return { planId, customerKey, docKey }
}
