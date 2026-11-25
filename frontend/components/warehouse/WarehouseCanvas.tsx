"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import { Stage, Layer, Rect, Text, Line, Arrow, Transformer, Group } from "react-konva"
import type Konva from "konva"
import type { WarehouseElement, EditorTool } from "@/lib/warehouse/types"
import { clippedHatchLines, placeCanvasLabels, stairDecoration } from "@/lib/warehouse/canvasPresentation"
import {
  fitOffsets,
  fitScale,
  metricPointToScreen,
  metricToScreen,
  nextViewOrientation,
  screenPointToMetric,
  screenToMetric,
  snapToGrid,
  viewContentSize,
  viewOrientationLabel,
  type ViewOrientationDeg,
  type ViewportTransform,
} from "@/lib/warehouse/coords"
import {
  LAYOUT_COLORS,
  colorsForType,
  labelPriority,
} from "@/lib/warehouse/style"

type Props = {
  elements: WarehouseElement[]
  floorLengthM: number
  floorWidthM: number
  selectedKey: string | null
  tool: EditorTool
  gridM: number
  conflictKeys?: Set<string>
  onSelect: (key: string | null) => void
  onChangeElement: (key: string, patch: Partial<WarehouseElement["geometry"]>) => void
  onCreateAt: (x_m: number, y_m: number) => void
  fitRequest?: number
  viewOrientationDeg?: ViewOrientationDeg
  onViewOrientationChange?: (deg: ViewOrientationDeg) => void
}

export function WarehouseCanvas({
  elements,
  floorLengthM,
  floorWidthM,
  selectedKey,
  tool,
  gridM,
  conflictKeys,
  onSelect,
  onChangeElement,
  onCreateAt,
  fitRequest = 0,
  viewOrientationDeg = 0,
  onViewOrientationChange,
}: Props) {
  const wrapRef = useRef<HTMLDivElement>(null)
  const stageRef = useRef<Konva.Stage>(null)
  const trRef = useRef<Konva.Transformer>(null)
  const [size, setSize] = useState({ w: 800, h: 560 })
  const [scale, setScale] = useState(1)
  const [interactingKey, setInteractingKey] = useState<string | null>(null)
  const [stagePos, setStagePos] = useState({ x: 0, y: 0 })
  const [base, setBase] = useState({ scale: 20, offsetX: 40, offsetY: 40 })
  const measureRef = useRef<{ x: number; y: number } | null>(null)
  const [measureLine, setMeasureLine] = useState<{
    x1: number
    y1: number
    x2: number
    y2: number
    distM: number
  } | null>(null)

  useEffect(() => {
    const el = wrapRef.current
    if (!el) return
    const ro = new ResizeObserver(() => {
      setSize({ w: el.clientWidth, h: el.clientHeight })
    })
    ro.observe(el)
    setSize({ w: el.clientWidth, h: el.clientHeight })
    return () => ro.disconnect()
  }, [])

  const recomputeFit = () => {
    const s = fitScale(floorLengthM, floorWidthM, size.w, size.h, 48, viewOrientationDeg)
    const off = fitOffsets(floorLengthM, floorWidthM, size.w, size.h, s, viewOrientationDeg)
    setBase({ scale: s, offsetX: off.offsetX, offsetY: off.offsetY })
    setScale(1)
    setStagePos({ x: 0, y: 0 })
  }

  useEffect(() => {
    recomputeFit()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [floorLengthM, floorWidthM, size.w, size.h, fitRequest, viewOrientationDeg])

  const vp: ViewportTransform = useMemo(
    () => ({
      scale: base.scale,
      offsetX: base.offsetX,
      offsetY: base.offsetY,
      floorLengthM,
      floorWidthM,
      viewOrientationDeg,
    }),
    [base, floorLengthM, floorWidthM, viewOrientationDeg],
  )

  const content = viewContentSize(floorLengthM, floorWidthM, viewOrientationDeg)

  useEffect(() => {
    const tr = trRef.current
    const stage = stageRef.current
    if (!tr || !stage) return
    const canTransform = tool === "select" || tool === "move" || tool === "resize" || tool === "rotate"
    if (!selectedKey || !canTransform) {
      tr.nodes([])
      tr.getLayer()?.batchDraw()
      return
    }
    const node = stage.findOne(`#el-${selectedKey}`)
    if (node) {
      tr.nodes([node])
      tr.getLayer()?.batchDraw()
    }
  }, [selectedKey, elements, tool, viewOrientationDeg])

  const gridLines = useMemo(() => {
    const lines: Array<{ points: number[] }> = []
    if (!gridM || gridM <= 0) return lines
    // La cuadrícula visual se simplifica a zoom bajo; el snap conserva gridM.
    const displayStep = gridM * Math.max(1, Math.ceil(8 / (gridM * vp.scale * scale)))
    for (let x = 0; x <= floorLengthM + 1e-6; x += displayStep) {
      const a = metricPointToScreen(x, 0, vp)
      const b = metricPointToScreen(x, floorWidthM, vp)
      lines.push({ points: [a.x, a.y, b.x, b.y] })
    }
    for (let y = 0; y <= floorWidthM + 1e-6; y += displayStep) {
      const a = metricPointToScreen(0, y, vp)
      const b = metricPointToScreen(floorLengthM, y, vp)
      lines.push({ points: [a.x, a.y, b.x, b.y] })
    }
    return lines
  }, [gridM, floorLengthM, floorWidthM, vp, scale])

  const sorted = useMemo(() => {
    return [...elements]
      .filter((e) => !e.pendingRetire && !e.pendingRemoveGeometry)
      .sort(
        (a, b) =>
          labelPriority(a.physical.element_type) - labelPriority(b.physical.element_type),
      )
  }, [elements])

  const markers = useMemo(() => {
    if (viewOrientationDeg !== 0) return null
    const unit = 1 / scale
    const front = metricPointToScreen(0, floorWidthM / 2, vp)
    const back = metricPointToScreen(floorLengthM, floorWidthM / 2, vp)
    return [
      { text: "FONDO", x: back.x - 42 * unit, y: back.y - 20 * unit, width: 84 * unit, height: 15 * unit },
      { text: "FRENTE", x: front.x - 42 * unit, y: front.y + 12 * unit, width: 84 * unit, height: 15 * unit },
    ]
  }, [vp, floorLengthM, floorWidthM, viewOrientationDeg, scale])

  const canvasLabels = useMemo(() => {
    const parentById = new Map(sorted.map(el => [el.physical.id, el]))
    const aisleParents = new Set(sorted.filter(el => el.physical.element_type === "aisle")
      .map(el => el.physical.parent_id))
    return placeCanvasLabels(
      sorted.filter(el => el.clientKey !== interactingKey).map(el => {
        const meta = { ...el.physical.meta_json, ...el.geometry.constraints_json, ...el.geometry.meta_json }
        const parent = el.physical.parent_id == null ? undefined : parentById.get(el.physical.parent_id)
        const type = el.physical.element_type
        const rect = metricToScreen({ ...el.geometry, rotation_deg: el.geometry.rotation_deg || 0 }, vp)
        const rad = rect.rotation * Math.PI / 180
        const cx = rect.x + rect.width / 2
        const cy = rect.y + rect.height / 2
        // Local +X = largo métrico; +Y = profundidad métrica.
        const entryPoint = type === "stair"
          ? { x: cx - Math.cos(rad) * rect.width / 2, y: cy - Math.sin(rad) * rect.width / 2 }
          : type === "aisle"
            ? { x: cx + Math.sin(rad) * rect.height / 2, y: cy - Math.cos(rad) * rect.height / 2 }
            : undefined
        return {
          key: el.clientKey,
          code: el.physical.code,
          type,
          rect,
          dimensions: `${el.geometry.width_m.toFixed(2).replace(".", ",")} × ${el.geometry.depth_m.toFixed(2).replace(".", ",")} m`,
          underStair: type === "position" && (meta.under_stair === true || parent?.physical.element_type === "stair"),
          sectorWithAisles: type === "sector" && aisleParents.has(el.physical.id),
          parentKey: parent?.clientKey,
          entryPoint,
          selected: el.clientKey === selectedKey,
        }
      }), scale, markers ?? [],
    )
  }, [sorted, interactingKey, vp, selectedKey, scale, markers])

  const handleWheel = (e: Konva.KonvaEventObject<WheelEvent>) => {
    e.evt.preventDefault()
    const stage = stageRef.current
    if (!stage) return
    const oldScale = scale
    const pointer = stage.getPointerPosition()
    if (!pointer) return
    const scaleBy = 1.08
    const newScale = e.evt.deltaY > 0 ? oldScale / scaleBy : oldScale * scaleBy
    const clamped = Math.min(8, Math.max(0.25, newScale))
    const mousePointTo = {
      x: (pointer.x - stagePos.x) / oldScale,
      y: (pointer.y - stagePos.y) / oldScale,
    }
    setScale(clamped)
    setStagePos({
      x: pointer.x - mousePointTo.x * clamped,
      y: pointer.y - mousePointTo.y * clamped,
    })
  }

  const localPointer = () => {
    const stage = stageRef.current
    const p = stage?.getPointerPosition()
    if (!p || !stage) return null
    return {
      x: (p.x - stage.x()) / stage.scaleX(),
      y: (p.y - stage.y()) / stage.scaleY(),
    }
  }

  return (
    <div ref={wrapRef} className="absolute inset-0 overflow-hidden bg-slate-100">
      <Stage
        ref={stageRef}
        width={size.w}
        height={size.h}
        scaleX={scale}
        scaleY={scale}
        x={stagePos.x}
        y={stagePos.y}
        draggable={tool === "pan" || tool === "select"}
        onDragEnd={(e) => {
          if (e.target === stageRef.current) {
            setStagePos({ x: e.target.x(), y: e.target.y() })
          }
        }}
        onWheel={handleWheel}
        onMouseDown={(e) => {
          if (e.target === stageRef.current) {
            if (
              tool === "create_sector" ||
              tool === "create_position" ||
              tool === "create_structure"
            ) {
              const local = localPointer()
              if (!local) return
              const m = screenPointToMetric(local.x, local.y, vp)
              onCreateAt(snapToGrid(m.x_m, gridM), snapToGrid(m.y_m, gridM))
              return
            }
            if (tool === "measure") {
              const local = localPointer()
              if (!local) return
              if (!measureRef.current) {
                measureRef.current = local
                setMeasureLine(null)
              } else {
                const a = measureRef.current
                const m1 = screenPointToMetric(a.x, a.y, vp)
                const m2 = screenPointToMetric(local.x, local.y, vp)
                const dist = Math.hypot(m2.x_m - m1.x_m, m2.y_m - m1.y_m)
                setMeasureLine({
                  x1: a.x,
                  y1: a.y,
                  x2: local.x,
                  y2: local.y,
                  distM: dist,
                })
                measureRef.current = null
              }
              return
            }
            onSelect(null)
          }
        }}
      >
        <Layer>
          <Rect
            x={vp.offsetX}
            y={vp.offsetY}
            width={content.contentW * vp.scale}
            height={content.contentH * vp.scale}
            fill={LAYOUT_COLORS.background}
            stroke="#94a3b8"
            strokeWidth={1.5}
            listening={false}
          />
          {gridLines.map((ln, i) => (
            <Line key={`g-${i}`} points={ln.points} stroke={LAYOUT_COLORS.grid} strokeWidth={0.8 / scale} opacity={0.65} listening={false} />
          ))}
          {sorted.map((el) => {
            const scr = metricToScreen(
              {
                x_m: el.geometry.x_m,
                y_m: el.geometry.y_m,
                width_m: el.geometry.width_m,
                depth_m: el.geometry.depth_m,
                rotation_deg: el.geometry.rotation_deg || 0,
              },
              vp,
            )
            const mergedMeta = {
              ...((el.physical.meta_json as Record<string, unknown>) || {}),
              ...((el.geometry.constraints_json as Record<string, unknown>) || {}),
              ...((el.geometry.meta_json as Record<string, unknown>) || {}),
            }
            const colors = colorsForType(el.physical.element_type, mergedMeta)
            const selected = el.clientKey === selectedKey
            const pending = el.geometry.validation_status === "pending_validation"
            const conflict = conflictKeys?.has(el.clientKey)
            const cx = scr.x + scr.width / 2
            const cy = scr.y + scr.height / 2
            const type = el.physical.element_type
            const underStair = type === "position" && (mergedMeta.under_stair === true ||
              sorted.some(parent => parent.physical.id === el.physical.parent_id && parent.physical.element_type === "stair"))
            const isStair = type === "stair"
            const isLoose = type === "loose_storage"
            // Los colores semánticos de la simbología se mantienen. Sólo se
            // ajusta el peso visual del trazo y la superposición bajo ESC.
            const strokeW = (selected
              ? LAYOUT_COLORS.selection.strokeWidth
              : conflict ? 2
              : isStair ? 2.2
              : underStair ? 1.3
              : type === "aisle" ? 1.6
              : type === "position" ? 0.95 : 1.1) / scale
            const dash = (underStair
              ? [3, 2]
              : pending
                ? type === "position" ? [3, 2] : LAYOUT_COLORS.pending_validation.dash
                : colors.dash)?.map(value => value / scale)
            const fill = conflict ? LAYOUT_COLORS.geom_conflict.fill
              : isStair ? "#ede9fe"
              : underStair ? "rgba(251,191,36,0.12)"
              : colors.fill
            const stairs = isStair ? stairDecoration(scr.width, scr.height) : null
            const hatch = isLoose ? clippedHatchLines(scr.width, scr.height) : []
            return (
              <Group
                key={el.clientKey}
                id={`el-${el.clientKey}`}
                x={cx}
                y={cy}
                rotation={scr.rotation}
                offsetX={scr.width / 2}
                offsetY={scr.height / 2}
                draggable={tool === "move" || (tool === "select" && selected)}
                onClick={(e) => {
                  e.cancelBubble = true
                  onSelect(el.clientKey)
                }}
                onTap={(e) => {
                  e.cancelBubble = true
                  onSelect(el.clientKey)
                }}
                onDragStart={() => setInteractingKey(el.clientKey)}
                onDragEnd={(e) => {
                  setInteractingKey(null)
                  const node = e.target
                  const topLeft = {
                    x: node.x() - node.offsetX(),
                    y: node.y() - node.offsetY(),
                    width: scr.width,
                    height: scr.height,
                    rotation: node.rotation(),
                  }
                  let m = screenToMetric(topLeft, vp)
                  if (gridM > 0) {
                    m = {
                      ...m,
                      x_m: snapToGrid(m.x_m, gridM),
                      y_m: snapToGrid(m.y_m, gridM),
                    }
                  }
                  onChangeElement(el.clientKey, {
                    x_m: m.x_m,
                    y_m: m.y_m,
                    rotation_deg: m.rotation_deg,
                  })
                }}
                onTransformStart={() => setInteractingKey(el.clientKey)}
                onTransformEnd={(e) => {
                  setInteractingKey(null)
                  const node = e.target
                  const scaleX = node.scaleX()
                  const scaleY = node.scaleY()
                  node.scaleX(1)
                  node.scaleY(1)
                  const w = Math.max(0.05, scr.width * scaleX)
                  const h = Math.max(0.05, scr.height * scaleY)
                  const topLeft = {
                    x: node.x() - w / 2,
                    y: node.y() - h / 2,
                    width: w,
                    height: h,
                    rotation: node.rotation(),
                  }
                  const m = screenToMetric(topLeft, vp)
                  onChangeElement(el.clientKey, {
                    x_m: m.x_m,
                    y_m: m.y_m,
                    width_m: m.width_m,
                    depth_m: m.depth_m,
                    rotation_deg: m.rotation_deg,
                  })
                }}
              >
                <Rect
                  width={scr.width}
                  height={scr.height}
                  fill={fill}
                  stroke={
                    selected
                      ? LAYOUT_COLORS.selection.stroke
                      : conflict
                        ? LAYOUT_COLORS.geom_conflict.stroke
                        : colors.stroke
                  }
                  strokeWidth={strokeW}
                  dash={dash}
                />
                {(stairs || hatch.length > 0) ? (
                  <Group clipX={0} clipY={0} clipWidth={scr.width} clipHeight={scr.height} listening={false}>
                    {stairs?.steps.map((pts, i) => (
                      <Line key={`st-${i}`} points={pts} stroke={colors.stroke}
                        strokeWidth={0.9 / scale} opacity={0.82} listening={false} />
                    ))}
                    {stairs?.arrow ? (
                      <Group listening={false}>
                        <Arrow points={stairs.arrow} stroke="#f8fafc" fill="#f8fafc"
                          strokeWidth={3 / scale} pointerLength={Math.min(7 / scale, scr.width * 0.2)}
                          pointerWidth={Math.min(7 / scale, scr.width * 0.2)} listening={false} />
                        <Arrow points={stairs.arrow} stroke="#4c1d95" fill="#4c1d95"
                          strokeWidth={1.6 / scale} pointerLength={Math.min(5 / scale, scr.width * 0.16)}
                          pointerWidth={Math.min(5 / scale, scr.width * 0.16)} listening={false} />
                      </Group>
                    ) : null}
                    {hatch.map((pts, i) => (
                      <Line key={`lh-${i}`} points={pts} stroke={colors.stroke}
                        strokeWidth={0.9 / scale} opacity={0.35} listening={false} />
                    ))}
                  </Group>
                ) : null}
              </Group>
            )
          })}
          {/* Textos en coordenadas de pantalla, fuera de grupos rotados y del Transformer. */}
          {markers?.map((marker) => (
            <Text key={marker.text} {...marker} align="center" verticalAlign="middle"
              fontSize={11 / scale} fontStyle="bold" fill={LAYOUT_COLORS.marker} listening={false} />
          ))}
          {canvasLabels.map((label) => (
            <Group key={label.key} listening={false}>
              {label.callout && label.anchor ? (
                <>
                  <Line points={[label.anchor.x, label.anchor.y, label.x + label.width / 2, label.y + label.height / 2]}
                    stroke="#64748b" strokeWidth={0.9 / scale} listening={false} />
                  <Rect x={label.x} y={label.y} width={label.width} height={label.height}
                    fill="#f8fafc" stroke="#cbd5e1" strokeWidth={0.7 / scale}
                    cornerRadius={3 / scale} listening={false} />
                </>
              ) : null}
              {label.special && !label.callout ? (
                // El fondo compacto evita que los peldaños reduzcan legibilidad;
                // pertenece a la capa de etiquetas, no al área seleccionable.
                <Rect x={label.x} y={label.y} width={label.width} height={label.height}
                  fill="#fff7ed" opacity={0.92} cornerRadius={2 / scale} listening={false} />
              ) : null}
              <Text x={label.x} y={label.y + label.paddingY} width={label.width}
                height={label.fontSize * 1.3} text={label.text} align="center" verticalAlign="middle"
                fontSize={label.fontSize} fontStyle={label.muted ? "normal" : "bold"}
                fill={label.special ? "#9a3412" : label.muted ? LAYOUT_COLORS.labelMuted : LAYOUT_COLORS.label}
                listening={false} />
              {label.subtitle ? (
                <Text x={label.x} y={label.y + label.paddingY + label.fontSize * 1.3}
                  width={label.width} height={label.subtitleSize * 1.3}
                  text={label.subtitle} align="center" verticalAlign="middle"
                  fontSize={label.subtitleSize} fill={LAYOUT_COLORS.labelMuted} listening={false} />
              ) : null}
            </Group>
          ))}
          {measureLine ? (
            <>
              <Line
                points={[measureLine.x1, measureLine.y1, measureLine.x2, measureLine.y2]}
                stroke="#0ea5e9"
                strokeWidth={2}
                dash={[4, 4]}
              />
              <Text
                x={(measureLine.x1 + measureLine.x2) / 2}
                y={(measureLine.y1 + measureLine.y2) / 2 - 14}
                text={`${measureLine.distM.toFixed(2)} m`}
                fontSize={12}
                fill="#0369a1"
              />
            </>
          ) : null}
          <Transformer
            ref={trRef}
            flipEnabled={false}
            rotateEnabled={tool === "rotate" || tool === "select"}
            enabledAnchors={
              tool === "resize" || tool === "select"
                ? undefined
                : tool === "rotate"
                  ? []
                  : ["top-left", "top-right", "bottom-left", "bottom-right"]
            }
            boundBoxFunc={(oldBox, newBox) => {
              if (newBox.width < 4 || newBox.height < 4) return oldBox
              return newBox
            }}
          />
        </Layer>
      </Stage>
      <div className="pointer-events-none absolute bottom-2 left-2 rounded bg-white/95 px-2 py-1 text-[11px] text-slate-600 shadow">
        Zoom {(scale * 100).toFixed(0)}% · cuadrícula {gridM} m ·{" "}
        {viewOrientationLabel(viewOrientationDeg)}
      </div>
      {onViewOrientationChange ? (
        <button
          type="button"
          className="absolute bottom-2 right-2 rounded border border-slate-300 bg-white/95 px-2 py-1 text-[11px] font-medium text-slate-700 shadow hover:bg-slate-50"
          title="Girar solo la vista (no modifica geometría guardada)"
          onClick={() => onViewOrientationChange(nextViewOrientation(viewOrientationDeg))}
        >
          Girar vista ({viewOrientationDeg}°)
        </button>
      ) : null}
    </div>
  )
}
