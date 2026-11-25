"use client"

import type { GeneratePreviewResult } from "@/lib/warehouse/api"
import type { WarehouseElement } from "@/lib/warehouse/types"

type MetricGeometry = { x_m: number; y_m: number; width_m: number; depth_m: number; rotation_deg: number }

type Box = { x: number; y: number; w: number; h: number; cx: number; cy: number; rotation: number }

function box(g: MetricGeometry): Box {
  // Canonical: y_m goes to the right, x_m increases upwards.
  return { x: g.y_m, y: -g.x_m - g.width_m, w: g.depth_m, h: g.width_m,
    cx: g.y_m + g.depth_m / 2, cy: -g.x_m - g.width_m / 2, rotation: g.rotation_deg || 0 }
}

function bounds(b: Box) {
  const rad = b.rotation * Math.PI / 180
  const w = Math.abs(Math.cos(rad)) * b.w + Math.abs(Math.sin(rad)) * b.h
  const h = Math.abs(Math.sin(rad)) * b.w + Math.abs(Math.cos(rad)) * b.h
  return { x0: b.cx - w / 2, x1: b.cx + w / 2, y0: b.cy - h / 2, y1: b.cy + h / 2 }
}

function metricGeometry(g: Record<string, unknown>): MetricGeometry {
  return { x_m: Number(g.x_m), y_m: Number(g.y_m), width_m: Number(g.width_m), depth_m: Number(g.depth_m), rotation_deg: Number(g.rotation_deg || 0) }
}

function Shape({ b, fill, stroke, code, dash = false, fontSize = 0.24 }: {
  b: Box; fill: string; stroke: string; code?: string; dash?: boolean; fontSize?: number
}) {
  const rotation = b.rotation ? `rotate(${b.rotation} ${b.cx} ${b.cy})` : undefined
  return (
    <g>
      <rect x={b.x} y={b.y} width={b.w} height={b.h} fill={fill} stroke={stroke} strokeWidth={0.035} strokeDasharray={dash ? "0.10 0.07" : undefined} transform={rotation} />
      {code ? <><title>{code}</title><text x={b.cx} y={b.cy} textAnchor="middle" dominantBaseline="central" fontSize={fontSize} fontWeight={600} fill="#334155" pointerEvents="none">{code}</text></> : null}
    </g>
  )
}

const STRUCTURES = new Set(["position", "loose_storage", "office", "gate", "door", "wall", "pillar", "beam", "stair", "unusable", "obstacle", "circulation"])

export function GeneratorPlanPreview({ parent, existing, preview, large }: {
  parent: WarehouseElement
  existing: WarehouseElement[]
  preview: GeneratePreviewResult | null
  large: boolean
}) {
  const parentBox = box(parent.geometry)
  const positions = preview?.positions.map(p => ({ code: p.code, b: box(metricGeometry(p.geometry)) })) || []
  const extents = [bounds(parentBox), ...positions.map(p => bounds(p.b))]
  const x0 = Math.min(...extents.map(b => b.x0)) - 0.5
  const x1 = Math.max(...extents.map(b => b.x1)) + 0.5
  const y0 = Math.min(...extents.map(b => b.y0)) - 0.5
  const y1 = Math.max(...extents.map(b => b.y1)) + 0.5
  const w = x1 - x0, h = y1 - y0
  const context = existing.filter(e => e.clientKey !== parent.clientKey && !e.pendingRetire && !e.pendingRemoveGeometry && STRUCTURES.has(e.physical.element_type))
    .map(e => ({ type: e.physical.element_type, code: e.physical.code, b: box(e.geometry) }))
    .filter(e => {
      const b = bounds(e.b)
      return b.x1 > x0 && b.x0 < x1 && b.y1 > y0 && b.y0 < y1
    })
    .sort((a,b) => Number(b.type === "circulation") - Number(a.type === "circulation"))
  const danger = new Set(preview?.blocking_issues?.flatMap(i => i.generated_codes || []) || [])
  const caution = new Set(preview?.position_issues?.filter(i => i.severity === "warning").flatMap(i => i.generated_codes || []) || [])
  const prefix = parent.physical.code + "-"
  const fontSize = 0.25
  const height = large ? Math.max(420, Math.min(1300, h * 53)) : 350
  return (
    <div className="space-y-2">
      <div className="max-h-[430px] overflow-auto rounded-lg border border-slate-200 bg-slate-100 p-1">
        <svg role="img" aria-label="Vista métrica de posiciones propuestas y estructuras existentes" viewBox={`${x0} ${y0} ${w} ${h}`} width="100%" height={height} preserveAspectRatio="xMidYMid meet" className="block">
          <Shape b={parentBox} fill="#f8fafc" stroke="#64748b" dash />
          {context.map(e => <Shape key={e.code} b={e.b} code={e.type === "circulation" ? undefined : e.code}
            fill={e.type === "circulation" ? "#e2e8f0" : e.type === "loose_storage" ? "#fde7b3" : e.type === "position" ? "#cbd5e1" : "#ddd6fe"}
            stroke={e.type === "loose_storage" ? "#b45309" : "#64748b"} fontSize={fontSize} />)}
          {positions.map(p => <g key={p.code}><title>{p.code}</title><Shape b={p.b}
            code={p.code.startsWith(prefix) ? p.code.slice(prefix.length) : p.code}
            fill={danger.has(p.code) ? "#fecaca" : caution.has(p.code) ? "#fde68a" : "#bbf7d0"}
            stroke={danger.has(p.code) ? "#dc2626" : caution.has(p.code) ? "#b45309" : "#15803d"}
            dash fontSize={Math.min(fontSize, p.b.w / 5)} /></g>)}
        </svg>
      </div>
      <div className="flex flex-wrap gap-x-3 gap-y-1 text-[11px] text-slate-600">
        {[["#bbf7d0", "Propuesta"], ["#fecaca", "Conflicto"], ["#fde68a", "Advertencia"], ["#fde7b3", "Sueltos"], ["#cbd5e1", "Existente"]].map(([color, label]) =>
          <span className="inline-flex items-center gap-1" key={label}><span className="h-3 w-3 rounded-sm border border-slate-400" style={{ backgroundColor: color }} />{label}</span>)}
      </div>
    </div>
  )
}
