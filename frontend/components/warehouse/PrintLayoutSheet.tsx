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

/** Hoja imprimible (CSS @media print). Reutiliza datos del editor; sin PDF server. */
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
  const scale = 28
  const provisional = elements.some(
    (e) =>
      e.geometry.validation_status === "pending_validation" ||
      e.geometry.meta_json?.provisional === true,
  )
  const visible = elements.filter((e) => !e.pendingRetire && !e.pendingRemoveGeometry)

  return (
    <div className="print-layout-sheet hidden print:block">
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
        {provisional ? (
          <div className="mt-2 border border-amber-700 bg-amber-50 px-2 py-1 text-xs text-amber-950">
            Advertencia: este plano incluye geometrías provisionales o pendientes de validación.
            La publicación no certifica seguridad ni autoriza apilamiento.
          </div>
        ) : null}
      </header>

      <svg
        width={lengthM * scale}
        height={widthM * scale}
        viewBox={`0 0 ${lengthM} ${widthM}`}
        className="border border-slate-400"
      >
        <rect x={0} y={0} width={lengthM} height={widthM} fill="#f8fafc" stroke="#94a3b8" strokeWidth={0.05} />
        {visible.map((e) => (
          <g key={e.clientKey}>
            <rect
              x={e.geometry.x_m}
              y={e.geometry.y_m}
              width={e.geometry.width_m}
              height={e.geometry.depth_m}
              fill="none"
              stroke="#0f172a"
              strokeWidth={0.04}
              transform={
                e.geometry.rotation_deg
                  ? `rotate(${e.geometry.rotation_deg} ${e.geometry.x_m + e.geometry.width_m / 2} ${
                      e.geometry.y_m + e.geometry.depth_m / 2
                    })`
                  : undefined
              }
            />
            <text
              x={e.geometry.x_m + e.geometry.width_m / 2}
              y={e.geometry.y_m + e.geometry.depth_m / 2}
              fontSize={0.35}
              textAnchor="middle"
              dominantBaseline="middle"
              fill="#0f172a"
            >
              {e.physical.code}
            </text>
          </g>
        ))}
      </svg>

      <section className="mt-3 text-xs">
        <div className="font-semibold">Dimensiones principales</div>
        <ul className="mt-1 list-disc pl-4">
          <li>
            Piso: {lengthM.toFixed(2)} m × {widthM.toFixed(2)} m
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
