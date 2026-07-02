"use client"

import { useState } from "react"
import { Calculator, Loader2 } from "lucide-react"

import { postCommercialSimulator } from "@/lib/api"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"

function formatCLP(n: number): string {
  return n.toLocaleString("es-CL", { style: "currency", currency: "CLP", maximumFractionDigits: 0 })
}

export function CommercialSimulator({
  dateFrom,
  dateTo,
  documentType,
  seller,
}: {
  dateFrom: string
  dateTo: string
  documentType?: string
  seller?: string
}) {
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState<Awaited<ReturnType<typeof postCommercialSimulator>> | null>(null)
  const [error, setError] = useState<string | null>(null)

  const run = async (scenario: string) => {
    setLoading(true)
    setError(null)
    try {
      const r = await postCommercialSimulator({
        scenario,
        date_from: dateFrom,
        date_to: dateTo,
        document_type: documentType,
        seller: seller || undefined,
      })
      setResult(r)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Error en simulación")
      setResult(null)
    } finally {
      setLoading(false)
    }
  }

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="flex items-center gap-2 text-base">
          <Calculator className="h-4 w-4" />
          Simulador Comercial
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <p className="text-sm text-muted-foreground">Proyecciones hipotéticas — no modifica datos reales.</p>
        <div className="flex flex-wrap gap-2">
          <Button size="sm" variant="outline" disabled={loading} onClick={() => void run("recuperar_clientes")}>
            Recuperar clientes
          </Button>
          <Button size="sm" variant="outline" disabled={loading} onClick={() => void run("subir_ticket")}>
            Subir ticket
          </Button>
          <Button size="sm" variant="outline" disabled={loading} onClick={() => void run("cross_selling")}>
            Cross selling
          </Button>
        </div>
        {loading && <Loader2 className="h-5 w-5 animate-spin text-muted-foreground" />}
        {error && <p className="text-sm text-destructive">{error}</p>}
        {result && (
          <div className="rounded-lg border bg-muted/30 p-4 text-sm space-y-2">
            <p>
              Proyección base: <strong>{formatCLP(result.proyeccion_base)}</strong>
            </p>
            <p>
              Incremento esperado: <strong className="text-emerald-600">+{formatCLP(result.incremento_esperado)}</strong>
            </p>
            <p>
              Venta proyectada: <strong>{formatCLP(result.venta_proyectada)}</strong>
            </p>
            {result.impacto_por_vendedor.length > 0 && (
              <ul className="mt-2 space-y-1 border-t pt-2 text-xs">
                {result.impacto_por_vendedor.slice(0, 5).map((x) => (
                  <li key={x.seller_name} className="flex justify-between">
                    <span>{x.seller_name}</span>
                    <span>+{formatCLP(x.incremento)}</span>
                  </li>
                ))}
              </ul>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  )
}
