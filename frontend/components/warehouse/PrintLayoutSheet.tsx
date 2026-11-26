"use client"

import type { SymbologyCatalog } from "@/lib/warehouse/types"
import type { WarehouseElement, WarehouseFacility, WarehouseFloor, WarehouseRevision } from "@/lib/warehouse/types"

type Props = {
  facility: WarehouseFacility | null
  floor: WarehouseFloor | null
  revision: WarehouseRevision | null
  elements: WarehouseElement[]
  catalog: SymbologyCatalog | null
  generatedAt: string
}

/**
 * Hoja imprimible (CSS @media print).
 * Usa la misma orientación longitudinal que el editor (entrada abajo, IZ izquierda):
 * SVG u = y_m, v_svg = length - x_m - width (entrada hacia y SVG mayor).
 */
export function PrintLayoutSheet({
  facility,
  floor,
  revision,
  elements,
  catalog,
  generatedAt,
}: Props) {
  const lengthM = Number(floor?.length_m) || 23.1
  const widthM = Number(floor?.width_m) || 13.7
  const scale = 22
  const provisional = elements.some(
    (e) =>
      e.geometry.validation_status === "pending_validation" ||
      e.geometry.meta_json?.provisional === true,
  )
  const visible = elements.filter((e) => !e.pendingRetire && !e.pendingRemoveGeometry)

  const svgW = widthM * scale
  const svgH = lengthM * scale

  return (
    <div className="print-layout-sheet hidden bg-white text-slate-900 print:block">
      <header className="mb-3 border-b border-black pb-2">
        <div className="text-lg font-bold">Grupo La Quillotana</div>
        <div className="text-sm">
          Inteligencia de Bodega — Layout físico
          {facility ? ` · ${facility.code} — ${facility.name}` : null}
          {floor ? ` · ${floor.code} — ${floor.name}` : null}
        </div>
        <div className="mt-1 text-xs">
          Estado: {revision?.status || "—"} · versión fila {revision?.row_version ?? "—"} ·
          revisión #{revision?.id ?? "—"} · generado {generatedAt}
        </div>
        <div className="mt-1 text-xs text-slate-600">
          Orientación de impresión: longitudinal (entrada abajo · IZ izquierda · DER derecha). No
          altera geometría persistida.
        </div>
        {provisional ? (
          <div className="mt-2 border border-amber-700 bg-amber-50 px-2 py-1 text-xs text-amber-950">
            Advertencia: este plano incluye geometrías provisionales o pendientes de validación.
            La publicación no certifica seguridad ni autoriza apilamiento.
          </div>
        ) : null}
      </header>

      <svg
        width={svgW}
        height={svgH}
        viewBox={`0 0 ${widthM} ${lengthM}`}
        className="border border-slate-400"
      >
        <rect x={0} y={0} width={widthM} height={lengthM} fill="#f8fafc" stroke="#94a3b8" strokeWidth={0.05} />
        <text x={widthM / 2} y={lengthM - 0.25} fontSize={0.4} textAnchor="middle" fill="#334155">
          FRENTE
        </text>
        <text x={1.05} y={0.45} fontSize={0.4} textAnchor="middle" fill="#334155">
          FONDO
        </text>
        {visible.map((e) => {
          // Longitudinal: x_svg = y_m, y_svg = length - x_m - width_m
          const x = e.geometry.y_m
          const y = lengthM - e.geometry.x_m - e.geometry.width_m
          const w = e.geometry.depth_m
          const h = e.geometry.width_m
          const cx = x + w / 2
          const cy = y + h / 2
          // Rotación métrica CCW → en SVG longitudinal (ejes intercambiados + Y invertida)
          // equivalente visual ≈ −rotation_deg en el plano de impresión canónico
          const rot = -(e.geometry.rotation_deg || 0)
          return (
            <g key={e.clientKey}>
              <rect
                x={x}
                y={y}
                width={w}
                height={h}
                fill="none"
                stroke="#0f172a"
                strokeWidth={0.04}
                transform={rot ? `rotate(${rot} ${cx} ${cy})` : undefined}
              />
              {e.physical.element_type !== "facility_boundary" ? (
                <text
                  x={cx}
                  y={cy}
                  fontSize={Math.min(0.45, Math.max(0.22, Math.min(w, h) / 4))}
                  textAnchor="middle"
                  dominantBaseline="middle"
                  fill="#0f172a"
                  transform={rot ? `rotate(${rot} ${cx} ${cy})` : undefined}
                >
                  {e.physical.code}
                </text>
              ) : null}
            </g>
          )
        })}
      </svg>

      <section className="mt-3 text-xs">
        <div className="font-semibold">Dimensiones principales</div>
        <ul className="mt-1 list-disc pl-4">
          <li>
            Piso: largo {lengthM.toFixed(2)} m (vertical en hoja) × ancho {widthM.toFixed(2)} m
            (horizontal)
          </li>
          <li>Elementos visibles: {visible.length}</li>
        </ul>
      </section>

      {catalog ? (
        <section className="mt-3 text-xs">
          <div className="font-semibold">Simbología (resumen)</div>
          <ul className="mt-1 columns-2 gap-4">
            {(catalog.symbols || []).slice(0, 12).map((s) => (
              <li key={s.type}>
                {s.label_es} <span className="text-slate-500">({s.type})</span>
              </li>
            ))}
          </ul>
        </section>
      ) : null}
    </div>
  )
}
