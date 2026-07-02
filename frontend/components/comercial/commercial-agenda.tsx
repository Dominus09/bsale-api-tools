"use client"

import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import type { CommercialAgendaTask } from "@/lib/api"
import { cn } from "@/lib/utils"

function formatCLP(n: number): string {
  return n.toLocaleString("es-CL", { style: "currency", currency: "CLP", maximumFractionDigits: 0 })
}

const TIPO_STYLES: Record<string, string> = {
  Recuperación: "border-red-500/50 bg-red-500/5",
  "Cross Selling": "border-blue-500/50 bg-blue-500/5",
  "Cliente en Riesgo": "border-amber-500/50 bg-amber-500/5",
  "Cliente VIP": "border-yellow-500/50 bg-yellow-500/5",
  "Cliente Nuevo": "border-sky-500/50 bg-sky-500/5",
  "Frecuencia vencida": "border-violet-500/50 bg-violet-500/5",
}

const SEGMENTO_COLORS: Record<string, string> = {
  VIP: "bg-yellow-500/20 text-yellow-800",
  Premium: "bg-purple-500/20 text-purple-800",
  Crecimiento: "bg-emerald-500/20 text-emerald-800",
  Perdido: "bg-red-500/20 text-red-800",
  "En Riesgo": "bg-amber-500/20 text-amber-800",
}

function ProbBadge({ value, label }: { value?: number; label: string }) {
  if (value == null) return null
  const color = value >= 70 ? "text-emerald-600" : value >= 40 ? "text-amber-600" : "text-muted-foreground"
  return (
    <span className={cn("text-xs font-medium", color)}>
      {label} {value}%
    </span>
  )
}

function AgendaTaskCard({
  task,
  onClientClick,
  onSellerClick,
}: {
  task: CommercialAgendaTask
  onClientClick?: (id: number) => void
  onSellerClick?: (name: string) => void
}) {
  return (
    <div
      className={cn(
        "rounded-xl border-2 p-4 transition-shadow hover:shadow-md",
        TIPO_STYLES[task.tipo] ?? "border-border",
        task.client_id && onClientClick && "cursor-pointer",
      )}
      onClick={() => task.client_id && onClientClick?.(task.client_id)}
    >
      <div className="mb-2 flex flex-wrap items-center gap-2">
        <Badge variant="outline" className="text-[10px] font-bold uppercase">
          {task.tipo}
        </Badge>
        {task.segmento && (
          <span className={cn("rounded-full px-2 py-0.5 text-[10px] font-semibold", SEGMENTO_COLORS[task.segmento] ?? "bg-muted")}>
            {task.segmento}
          </span>
        )}
        <Badge variant={task.prioridad === "alta" ? "destructive" : "secondary"} className="ml-auto text-[10px]">
          {task.prioridad}
        </Badge>
      </div>
      <h4 className="font-semibold leading-tight">{task.cliente}</h4>
      <p className="mt-1 text-sm text-muted-foreground">{task.motivo}</p>
      <div className="mt-3 flex flex-wrap gap-3">
        <ProbBadge value={task.purchase_probability} label="Compra" />
        <ProbBadge value={task.probabilidad_recuperacion} label="Recup." />
        <span className="text-xs text-muted-foreground">Score {task.score}</span>
      </div>
      <div className="mt-3 flex items-end justify-between gap-2">
        <div>
          <p className="text-lg font-bold text-primary">{formatCLP(task.potencial_economico)}</p>
          <p className="text-xs font-medium">{task.accion_sugerida}</p>
        </div>
        <div className="text-right text-xs text-muted-foreground">
          {task.comuna && <p>{task.comuna}</p>}
          {task.vendedor && (
            <button
              type="button"
              className="hover:text-primary hover:underline"
              onClick={(e) => {
                e.stopPropagation()
                onSellerClick?.(task.vendedor!)
              }}
            >
              {task.vendedor}
            </button>
          )}
        </div>
      </div>
      {task.productos_sugeridos.length > 0 && (
        <p className="mt-2 truncate text-xs text-muted-foreground">
          Sugerido: {task.productos_sugeridos.slice(0, 3).join(", ")}
        </p>
      )}
    </div>
  )
}

export function CommercialAgenda({
  agenda,
  selectedSeller,
  onClientClick,
  onSellerClick,
}: {
  agenda: NonNullable<import("@/lib/api").CommercialCrmLayer["agenda"]>
  selectedSeller?: string
  onClientClick?: (id: number) => void
  onSellerClick?: (name: string) => void
}) {
  const vendedorBlock = selectedSeller
    ? agenda.vendedores.find((v) => v.seller_name === selectedSeller)
    : null
  const tasks = vendedorBlock?.tareas ?? agenda.tareas.slice(0, 18)

  return (
    <section className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <h2 className="text-lg font-semibold tracking-tight">Agenda Inteligente</h2>
          <p className="text-sm text-muted-foreground">
            Plan diario por impacto económico — {agenda.total_tareas} tareas generadas
          </p>
        </div>
        {vendedorBlock && (
          <Badge variant="secondary">
            {vendedorBlock.total_tareas} tareas · {formatCLP(vendedorBlock.potencial_total)} potencial
          </Badge>
        )}
      </div>

      {!selectedSeller && agenda.vendedores.length > 0 && (
        <div className="flex flex-wrap gap-2">
          {agenda.vendedores.slice(0, 8).map((v) => (
            <button
              key={v.seller_name}
              type="button"
              className="rounded-lg border bg-card px-3 py-2 text-left text-sm transition-colors hover:bg-muted/60"
              onClick={() => onSellerClick?.(v.seller_name)}
            >
              <p className="font-semibold">{v.seller_name}</p>
              <p className="text-xs text-muted-foreground">
                {v.total_tareas} tareas · {formatCLP(v.potencial_total)}
              </p>
            </button>
          ))}
        </div>
      )}

      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {tasks.map((task, i) => (
          <AgendaTaskCard
            key={`${task.client_id}-${task.tipo}-${i}`}
            task={task}
            onClientClick={onClientClick}
            onSellerClick={onSellerClick}
          />
        ))}
      </div>
    </section>
  )
}

export function CommercialRouteTargets({
  targets,
  onSellerClick,
}: {
  targets: import("@/lib/api").CommercialRouteTarget[]
  onSellerClick?: (name: string) => void
}) {
  if (!targets.length) return null
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">Objetivos por Ruta</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {targets.slice(0, 8).map((r) => (
          <button
            key={r.seller_name}
            type="button"
            className="flex w-full gap-4 rounded-lg border p-3 text-left hover:bg-muted/50"
            onClick={() => onSellerClick?.(r.seller_name)}
          >
            <div className="min-w-0 flex-1">
              <p className="font-semibold">{r.seller_name}</p>
              <p className="text-xs text-muted-foreground">
                {r.clientes_pendientes} pendientes · Potencial {formatCLP(r.potencial)}
              </p>
            </div>
            <div className="shrink-0 text-right text-sm">
              <p className="font-bold">{r.cumplimiento_pct}%</p>
              <p className="text-xs text-muted-foreground">Meta {formatCLP(r.meta)}</p>
            </div>
          </button>
        ))}
      </CardContent>
    </Card>
  )
}
