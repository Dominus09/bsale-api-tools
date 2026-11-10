"use client"

import Link from "next/link"
import { useParams } from "next/navigation"

/**
 * Evita pantalla blanca inutilizable tras excepción de cliente
 * (p. ej. fallo de chunk al navegar a resultado post-cierre).
 */
export default function EntregaError({
  error,
  reset,
}: {
  error: Error & { digest?: string }
  reset: () => void
}) {
  const params = useParams()
  const planId = Number(params.planId)
  const rawKey = params.customerKey
  const customerKey = decodeURIComponent(
    Array.isArray(rawKey) ? rawKey.join("/") : String(rawKey || ""),
  )
  const ficha =
    Number.isFinite(planId) && customerKey
      ? `/despachos/${planId}/clientes/${encodeURIComponent(customerKey)}`
      : "/despachos"
  const clients =
    Number.isFinite(planId) && planId > 0
      ? `/despachos/${planId}/clientes`
      : "/despachos"

  return (
    <div className="stack-gap" style={{ padding: 16 }}>
      <div className="banner banner-error" role="alert">
        Ocurrió un error en la pantalla de entrega. Si ya confirmó el cierre,
        la visita puede estar guardada: vuelva a la ficha y abra el resultado.
      </div>
      <p className="muted" style={{ fontSize: "0.8rem", margin: 0 }}>
        {error?.message ? String(error.message).slice(0, 160) : "Error de interfaz"}
      </p>
      <button type="button" className="btn btn-primary" onClick={() => reset()}>
        Reintentar pantalla
      </button>
      <Link href={ficha} className="btn btn-secondary">
        Ir a ficha del cliente
      </Link>
      <Link href={clients} className="btn btn-ghost">
        Lista de clientes
      </Link>
    </div>
  )
}
