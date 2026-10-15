"use client"

import { useEffect, useState } from "react"
import Link from "next/link"
import { Info } from "lucide-react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { AnalyticsV2Error, getPacksSummary } from "@/lib/analytics-v2/api"
import { formatPct } from "@/lib/analytics-v2/labels"
import type { PacksSummary } from "@/lib/analytics-v2/packs-types"

const CONCEPTS: { title: string; text: string }[] = [
  {
    title: "Pack Bsale",
    text: "Producto de Bsale compuesto por otros productos (pack_details). Su costo es la suma de sus componentes con sus propios impuestos. No es una promoción ni justifica por sí mismo un precio bajo política.",
  },
  {
    title: "Descuento Bsale",
    text: "Rebaja aplicada en el documento de venta de Bsale. No modifica la lista de precios ni se registra como campaña ERP.",
  },
  {
    title: "Oferta mensual ERP",
    text: "Campaña de Promociones V2 con precio, vigencia y aprobación. Solo si está aprobada, vigente y su precio coincide con Bsale justifica una excepción de política.",
  },
  {
    title: "Remate ERP",
    text: "Campaña de liquidación de Promociones V2 con motivo registrado. Igual que la oferta, solo justifica una excepción si está autorizada y verificada.",
  },
]

export function PromotionPacksInfo({ companyId }: { companyId: number }) {
  const [summary, setSummary] = useState<PacksSummary | null>(null)
  const [unavailable, setUnavailable] = useState<string | null>(null)

  useEffect(() => {
    const ac = new AbortController()
    getPacksSummary(companyId, "promedio_bsale", ac.signal)
      .then(setSummary)
      .catch((e: unknown) => {
        if (ac.signal.aborted) return
        setUnavailable(
          e instanceof AnalyticsV2Error && e.status === 403
            ? "Su perfil no tiene acceso a la rentabilidad de packs."
            : "No se pudo cargar el resumen de packs.",
        )
      })
    return () => ac.abort()
  }, [companyId])

  return (
    <div className="space-y-3">
      <Alert>
        <Info className="h-4 w-4" />
        <AlertTitle>Sección informativa</AlertTitle>
        <AlertDescription>
          Los packs Bsale no son campañas: no se crean, aprueban ni importan desde aquí, y no excluyen alertas de margen. La rentabilidad
          histórica de ventas de packs no está implementada.
        </AlertDescription>
      </Alert>

      <section className="grid gap-2 md:grid-cols-2 xl:grid-cols-4">
        {CONCEPTS.map((c) => (
          <div key={c.title} className="rounded-md border p-3">
            <h3 className="text-sm font-semibold">{c.title}</h3>
            <p className="mt-1 text-xs text-muted-foreground">{c.text}</p>
          </div>
        ))}
      </section>

      {summary ? (
        <section className="flex flex-wrap items-center gap-x-6 gap-y-2 rounded-md border p-3 text-sm">
          <span>
            Packs activos: <strong className="tabular-nums">{summary.packs_active}</strong>
          </span>
          <span>
            Con costo calculable: <strong className="tabular-nums">{summary.packs_active_calculable}</strong>
            {summary.coverage_pct != null ? ` (${formatPct(summary.coverage_pct)})` : ""}
          </span>
          <span>
            Pendientes: <strong className="tabular-nums">{summary.packs_active_pending}</strong>
          </span>
          <span>
            Packs bajo costo / bajo política:{" "}
            <strong className="tabular-nums">
              {summary.packs_by_problem.precio_bajo_costo ?? 0} / {summary.packs_by_problem.bajo_politica ?? 0}
            </strong>
          </span>
          <Link href="/analitica-v2/packs" className="text-primary hover:underline">
            Ver rentabilidad de packs
          </Link>
        </section>
      ) : unavailable ? (
        <p className="text-xs text-muted-foreground">{unavailable}</p>
      ) : null}
    </div>
  )
}
