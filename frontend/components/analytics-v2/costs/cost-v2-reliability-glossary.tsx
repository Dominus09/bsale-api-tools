"use client"

import { ChevronDown } from "lucide-react"

import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible"
import { ANOMALY_LABELS } from "@/lib/analytics-v2/labels"
import type { AnomalyCode, CostSummary } from "@/lib/analytics-v2/types"
import { cn } from "@/lib/utils"

type GlossaryEntry = {
  key: string
  title: string
  meaning: string
  causes: string
  admin: string
  critical: string
  extra?: string
}

const DEFAULT_STALE_DAYS = 180

const DEFAULT_REVIEW_CODES: AnomalyCode[] = [
  "alza_anormal",
  "baja_sospechosa",
  "recepcion_modificada",
  "cambio_variante",
  "cantidad_extrema",
  "detalle_incompleto",
  "diferencia_promedio_bsale",
]

function buildEntries(staleDays: number, reviewCodes: AnomalyCode[]): GlossaryEntry[] {
  const reviewLabels = reviewCodes
    .map((c) => ANOMALY_LABELS[c] ?? c)
    .join(", ")
  return [
    {
      key: "consistente",
      title: "Costos consistentes",
      meaning:
        "Producto con costo utilizable que no presenta las alertas evaluadas por el motor. No significa validación contable definitiva.",
      causes:
        "Existe compra comparable válida (costo > 0), sin anomalías de revisión en esa última compra, y sin superar el umbral de antigüedad. Si la fecha comparable falta, el motor no marca desactualizado y también cae aquí.",
      admin:
        "Puede usarse como referencia operativa. Si hay duda documental, contrastar de todas formas con la factura.",
      critical: "No es alerta crítica.",
    },
    {
      key: "solo_promedio_bsale",
      title: "Solo promedio Bsale",
      meaning:
        "Existe costo promedio Bsale, pero no una recepción comparable suficiente para verificarlo mediante compras.",
      causes:
        "No hay compra comparable desde el inicio del historial operativo (o el costo/cantidad comparable no es utilizable), y el promedio Bsale es mayor que cero.",
      admin:
        "Buscar recepciones omitidas, guías mal clasificadas o transferencias que no cuentan como compra. Confirmar que el promedio Bsale tenga respaldo.",
      critical: "No es alerta crítica por sí sola; indica falta de evidencia de compra.",
    },
    {
      key: "requiere_revision",
      title: "Requieren revisión",
      meaning:
        "Hay una alerta en los antecedentes de compra. Administración debe comprobar documento, cantidad, unidad o costo. No implica automáticamente error.",
      causes: `La última compra comparable presenta una o más anomalías de revisión: ${reviewLabels}.`,
      admin:
        "Priorizar por severidad del motor (alta antes que media): alzas/bajas frente a la compra anterior, recepciones modificadas, cambio de variante, cantidad extrema, detalle incompleto o divergencia vs promedio Bsale. Contrastar factura, unidad y presentación.",
      critical:
        "Es la alerta de calidad más urgente del panel de confiabilidad, pero sigue siendo señal de revisión, no error confirmado.",
      extra:
        "Si también hubiera antigüedad > umbral, prevalece «requieren revisión» (la anomalía tiene precedencia sobre desactualizado).",
    },
    {
      key: "desactualizado",
      title: "Costos desactualizados",
      meaning:
        "El análisis carece de evidencia de compra suficientemente reciente según sus criterios. No significa necesariamente que el dato Bsale esté sin sincronizar o sea incorrecto.",
      causes: `Hay compra comparable válida sin anomalías de revisión, pero la fecha de esa compra supera ${staleDays} días respecto de la fecha de evaluación del motor.`,
      admin:
        "Verificar si hubo compras recientes no clasificadas como comparables, o si el SKU dejó de rotar. El promedio Bsale puede seguir vigente.",
      critical: "No es alerta crítica automática; indica evidencia antigua.",
      extra: `Umbral del motor: (hoy − fecha comparable).days > ${staleDays} (STALE_COST_DAYS). Solo aplica si la fecha existe; sin fecha no entra en este bucket.`,
    },
    {
      key: "sin_costo",
      title: "Sin costo",
      meaning:
        "Producto con costo promedio inexistente, cero o inválido, según la clasificación exacta del servicio.",
      causes:
        "No hay compra comparable utilizable y el costo promedio Bsale es nulo, cero o no positivo.",
      admin: "Completar o corregir el costo en Bsale y revisar recepciones asociadas al SKU.",
      critical: "Alerta operativa alta cuando el SKU tiene stock; el motor la clasifica aparte de «requieren revisión».",
    },
  ]
}

export function CostV2ReliabilityGlossary({ summary }: { summary: CostSummary | null }) {
  const criteria = summary?.reliability_criteria
  const staleDays = criteria?.stale_cost_days ?? DEFAULT_STALE_DAYS
  const reviewCodes = (criteria?.review_anomaly_codes ?? DEFAULT_REVIEW_CODES) as AnomalyCode[]
  const entries = buildEntries(staleDays, reviewCodes)
  const precedence = criteria?.precedence ?? [
    "sin_compra_comparable",
    "requiere_revision",
    "desactualizado",
    "consistente",
  ]

  return (
    <Collapsible defaultOpen={false} className="rounded-xl border border-lq-line bg-lq-surface shadow-[0_1px_2px_rgba(15,23,42,0.03)]">
      <CollapsibleTrigger
        className={cn(
          "group flex w-full items-center justify-between gap-3 px-4 py-3 text-left",
          "hover:bg-lq-canvas/60 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-lq-ink/20",
        )}
      >
        <div>
          <p className="text-sm font-semibold tracking-[-0.01em] text-lq-ink">
            ¿Qué significa cada indicador?
          </p>
          <p className="mt-0.5 text-xs text-lq-muted">
            Criterios exactos del motor de confiabilidad · umbral de antigüedad {staleDays} días
          </p>
        </div>
        <ChevronDown className="h-4 w-4 shrink-0 text-lq-muted transition-transform duration-200 group-data-[state=open]:rotate-180" />
      </CollapsibleTrigger>
      <CollapsibleContent className="border-t border-lq-line px-4 pb-4 pt-3">
        <ol className="mb-4 list-decimal space-y-1 pl-4 text-xs text-lq-muted">
          <li>
            Precedencia: un SKU recibe una sola etiqueta. Orden del motor:{" "}
            <span className="font-medium text-lq-body">{precedence.join(" → ")}</span>.
          </li>
          <li>
            «Sin compra comparable» se parte en <span className="font-medium text-lq-body">solo promedio Bsale</span>{" "}
            (promedio &gt; 0) o <span className="font-medium text-lq-body">sin costo</span> (promedio nulo/cero).
          </li>
          <li>
            Los KPI respetan el filtro CON STOCK OPERATIVO / SIN STOCK OPERATIVO / TODOS (empresa 3:
            oficinas 1, 3, 4 y 5). El stock total Bsale es referencia aparte.
          </li>
        </ol>
        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
          {entries.map((e) => (
            <article
              key={e.key}
              className="rounded-lg border border-lq-line/80 bg-lq-canvas/40 p-3 text-sm leading-5 text-lq-body"
            >
              <h3 className="text-[13px] font-semibold text-lq-ink">{e.title}</h3>
              <dl className="mt-2 space-y-2 text-xs">
                <div>
                  <dt className="font-medium uppercase tracking-wide text-lq-muted">Qué significa</dt>
                  <dd className="mt-0.5">{e.meaning}</dd>
                </div>
                <div>
                  <dt className="font-medium uppercase tracking-wide text-lq-muted">Qué lo provoca</dt>
                  <dd className="mt-0.5">{e.causes}</dd>
                </div>
                <div>
                  <dt className="font-medium uppercase tracking-wide text-lq-muted">Qué revisar</dt>
                  <dd className="mt-0.5">{e.admin}</dd>
                </div>
                <div>
                  <dt className="font-medium uppercase tracking-wide text-lq-muted">¿Alerta crítica?</dt>
                  <dd className="mt-0.5">{e.critical}</dd>
                </div>
                {e.extra ? (
                  <div>
                    <dt className="font-medium uppercase tracking-wide text-lq-muted">Detalle del motor</dt>
                    <dd className="mt-0.5">{e.extra}</dd>
                  </div>
                ) : null}
              </dl>
            </article>
          ))}
        </div>
      </CollapsibleContent>
    </Collapsible>
  )
}
