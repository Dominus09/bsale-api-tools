/** Navegación post-cierre: evita soft-nav frágil en iOS PWA tras finalize. */

export function resultadoHref(
  planId: number,
  customerKey: string,
  visitId: number,
): string {
  const base = `/despachos/${planId}/clientes/${encodeURIComponent(customerKey)}/entrega/resultado`
  return `${base}?visitId=${encodeURIComponent(String(visitId))}`
}

/**
 * Tras cierre persistido: preferir navegación dura en PWA/iOS.
 * Soft router.push a resultado ha mostrado Application error (chunk/load).
 */
export function navigateToResultado(
  planId: number,
  customerKey: string,
  visitId: number,
): void {
  const href = resultadoHref(planId, customerKey, visitId)
  if (typeof window !== "undefined") {
    window.location.assign(href)
    return
  }
}

export function isClosedVisitStatus(status: string | null | undefined): boolean {
  const s = String(status || "")
  return (
    s === "completed" ||
    s === "partial" ||
    s === "not_delivered" ||
    s === "cancelled"
  )
}
