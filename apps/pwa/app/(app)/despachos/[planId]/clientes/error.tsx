"use client"

import { useEffect } from "react"
import { AppLink } from "@/components/app-link"

/**
 * Evita la pantalla negra genérica de Next cuando falla el render de Clientes
 * (p.ej. HTML/chunks offline inconsistentes).
 */
export default function ClientesError({
  error,
  reset,
}: {
  error: Error & { digest?: string }
  reset: () => void
}) {
  useEffect(() => {
    console.error("[pwa] clientes route error", error?.message, error?.digest)
  }, [error])

  return (
    <div className="stack-gap">
      <div className="banner banner-error" role="alert">
        No se pudo abrir Clientes.
        {error?.message ? (
          <div style={{ marginTop: 8, fontSize: "0.82rem", opacity: 0.9 }}>
            {error.message}
          </div>
        ) : null}
      </div>
      <div className="btn-row">
        <button type="button" className="btn btn-primary" onClick={() => reset()}>
          Reintentar
        </button>
        <AppLink href="/despachos" className="btn btn-secondary">
          Mis despachos
        </AppLink>
      </div>
      <p className="muted" style={{ margin: 0, fontSize: "0.78rem" }}>
        Si estás sin conexión, vuelve online un momento, abre Clientes y espera a
        que diga listo sin conexión; luego reintenta en modo avión.
      </p>
    </div>
  )
}
