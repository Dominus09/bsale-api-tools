"use client"

/** Circuito de entrega en modo foco: sin P3 ni herramientas de ficha. */
export default function EntregaLayout({ children }: { children: React.ReactNode }) {
  return <div className="entrega-focus stack-gap">{children}</div>
}
